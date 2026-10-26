#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# ==============================================================================
# Runs one named arm-4 scenario with pinned JMH settings and records what it ran on.
#
#   sorted-merge-scenarios.sh <scenario> [--bisect] [--dry-run]
#
# Without --bisect it runs BenchmarkSortedMergeMemoryCrossover under JMH (allocation and latency). With --bisect it
# runs sorted-merge-bisect-xmx.sh over the same cells instead (minimum surviving heap). The two answer different
# questions and are never mixed: see the headers of those two files. --dry-run prints every command and writes only
# environment.txt, which is how to check what a scenario will do before committing hours to it.
#
# Scenarios. Every one sweeps OFF and ON, and both ORDER BY shapes (TS_VAL, TS_ONLY), because the two shapes are
# expected to disagree about where the crossover falls. blockSize, numSegments, rowsPerSegment and
# maxExecutionThreads keep the benchmark's defaults (10000, 100, 50000, 10).
#
#   anchors    K=1; overlap FULL and PARTIAL; LIMIT 1000 and 1000000.
#              The reference cells every other scenario is read against. DISJOINT is left out on purpose: it is depth 1,
#              so it is the cell where the two arms are least likely to differ and the least informative to repeat.
#              Both ORDER BY shapes are kept, since doubling an 8-cell grid is cheap.   16 cells.
#   max-limit  K=1; all three overlaps; LIMIT 2147483647 (Integer.MAX_VALUE, offset 0). Nothing prunes and every row
#              comes back, so this is the ceiling of what a LIMIT can ask for.            12 cells.
#   low-k      K in 1, 100, 10000, 50000; all three overlaps; LIMIT 1000 and 1000000. K is how many consecutive rows share
#              a tsCol value, and 50000 is rowsPerSegment, i.e. one distinct key per segment. K > 1 is what lets the
#              tail-to-sort path's per-run heap hold more than one row.                  96 cells (24 per K).
#   consuming  consumingRows in 50000, 1000000; K=1; all three overlaps; LIMIT 1000, 1000000 and 2147483647. One mutable
#              off-heap segment is added after the 100 offline ones, so under ON its leaf is a full scan plus top-K.
#              The limits are one small (the leaf's full scan is the whole cost and the merge's is negligible), one
#              large (the headline cell, where the merge's retention matters) and the maximum (everything returns,
#              consuming rows included). 10000 and 100000 are left out: they sit between regimes the two ends already
#              bracket, and each added limit costs 12 cells per consumingRows value. The no-consuming baselines for
#              these limits come from the anchors and max-limit scenarios (K=1, consumingRows=0) rather than being
#              re-run here, so compare across scenarios only for runs from the same build on the same host.   72 cells.
#
# JMH settings, pinned: -f 1 -wi 1 -i 3 -r 5s -w 5s -prof gc -rf json. The forked JVM gets -Xmx${JAVA_XMX} and
# -XX:+UseG1GC and nothing else (plus -Darm4.cpuTime=true when CPU_TIME=true). Those are the only knobs: change them
# through the environment and the change is written to environment.txt, never silently.
#
# Heap: JAVA_XMX defaults to 24g. The earlier v7 runs passed no -Xmx, which meant "the JVM default", and on this host
# that is 24 GiB (`java -XX:+PrintFlagsFinal -version | grep MaxHeapSize` -> 25769803776, a quarter of the container
# limit), so 24g reproduces them while no longer depending on the host. It is ample for the largest cell: both arms
# hold a few copies of 5,000,000 result rows at roughly 200 bytes each (Object[3], a Long, an Integer and a ~60
# character String), around a gigabyte per copy. A heap that small would make GC the story, which is the opposite of
# what this arm measures; it is not a tuned value and nothing here should be read as sensitive to it. -Xms is left
# alone, as in v7, so the heap grows as G1 sees fit.
#
# Prerequisites, from the repository root:
#   ./mvnw -pl pinot-perf -am install -DskipTests -Ddevelocity.cache.local.enabled=false
#   ./mvnw -pl pinot-perf dependency:build-classpath -Dmdep.outputFile=/tmp/arm4-cp.txt \
#       -Ddevelocity.cache.local.enabled=false
# The build flag is not decoration: the Develocity cache can report BUILD SUCCESS having skipped the work, which for a
# benchmark means running stale classes. The first run of a K builds that K's offline fixtures (three overlaps x 100
# segments x 50,000 rows each, multi-gigabyte, cached under the temp directory); every later run reuses them.
#
# Output, one directory per invocation: ${OUT_ROOT}/<scenario>[-bisect]-<UTC timestamp>/, or OUT_DIR if set.
#   environment.txt   host, cores, memory, java -version, JVM's default MaxHeapSize, git HEAD, number of dirty paths,
#                     JMH and JVM arguments, scenario
#   queue.log         one START and one END line per group of cells, with exit status and seconds. A failing group does
#                     not stop the queue; the script exits non-zero at the end if any group failed.
#   <group>.json/.log JMH result and its console output (or, with --bisect, <group>-<orderBy>.csv/.log/-audit.csv)
# A group is the cells that share one fixture (one K, one consumingRows value), so a failure costs one group.
#
# Environment: JAVA_XMX (24g), CPU_TIME (false), OUT_ROOT (/tmp/arm4-scenarios), OUT_DIR, CP_FILE (/tmp/arm4-cp.txt),
# CLASSES_DIR (<repo>/pinot-perf/target/classes). Further bisection knobs (REPLICATES, HIGH_MB, ...) pass through to
# sorted-merge-bisect-xmx.sh unchanged; see its header.
#
# Bisection. --bisect runs the bisection script once per (group, ORDER BY), over the group's overlaps and limits and
# both modes, with ORDER_BY, KEY_CARDINALITY, CONSUMING_ROWS, LIMITS and OVERLAPS set to match. HIGH_MB defaults to
# 4096 there, which suits 1000000-row results but not 2147483647, where 5,000,000 rows return and the answer is above
# it; max-limit and consuming therefore default HIGH_MB to 16384. Cost is dominated by JVM launches: per cell roughly
# REPLICATES x (2 range checks + ~7 bisection steps) x (REPEATS: 3 for OFF, 2 for ON), i.e. about 45 launches, each
# paying the fixture's mmap load and, for consuming scenarios, a rebuild of the consuming segment (about 1.5 s for
# 1,000,000 rows).
#
# Expected wall clock, JMH: each cell is one forked JVM with 1 warmup + 3 measured 5 s iterations, so 20 s of
# measurement plus fork, fixture load and the setup probe -- the v7 grid took 23 to 46 s per cell. At 1000000 and
# 2147483647 a single invocation takes seconds, so those cells sit at the top of that range or past it.
# See the "Estimated cost" block below for per-scenario totals.
#
# Reading the results. A consuming cell's printed overlap depth is measured at the midpoint of the global key range, so
# once consumingRows is much larger than rowsPerSegment the consuming segment alone decides it (FULL and PARTIAL both
# read 1 at 1000000). Label those cells by overlap and consumingRows, not by depth.
# ==============================================================================

set -u -o pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
BISECT_SCRIPT="${SCRIPT_DIR}/sorted-merge-bisect-xmx.sh"
BENCHMARK_CLASS="BenchmarkSortedMergeMemoryCrossover"

JAVA_XMX="${JAVA_XMX:-24g}"
CPU_TIME="${CPU_TIME:-false}"
OUT_ROOT="${OUT_ROOT:-/tmp/arm4-scenarios}"
CP_FILE="${CP_FILE:-/tmp/arm4-cp.txt}"
CLASSES_DIR="${CLASSES_DIR:-${REPO_ROOT}/pinot-perf/target/classes}"

usage() {
  echo "Usage: $(basename "$0") <anchors|max-limit|low-k|consuming> [--bisect] [--dry-run]" >&2
  exit 2
}

SCENARIO=""
DO_BISECT=false
DRY_RUN=false
for arg in "$@"; do
  case "${arg}" in
    anchors | max-limit | low-k | consuming)
      [[ -z "${SCENARIO}" ]] || usage
      SCENARIO="${arg}"
      ;;
    --bisect) DO_BISECT=true ;;
    --dry-run) DRY_RUN=true ;;
    *) usage ;;
  esac
done
[[ -n "${SCENARIO}" ]] || usage

ALL_OVERLAPS="DISJOINT,PARTIAL,FULL"
ORDER_BYS="TS_VAL,TS_ONLY"
MODES="OFF,ON"

# One group per fixture: "label|overlaps|keyCardinality|consumingRows|limits", comma-separated lists.
GROUPS_DEF=()
case "${SCENARIO}" in
  anchors)
    GROUPS_DEF+=("anchors|FULL,PARTIAL|1|0|1000,1000000")
    ;;
  max-limit)
    GROUPS_DEF+=("max-limit|${ALL_OVERLAPS}|1|0|2147483647")
    ;;
  low-k)
    for k in 1 100 10000 50000; do
      GROUPS_DEF+=("low-k-k${k}|${ALL_OVERLAPS}|${k}|0|1000,1000000")
    done
    ;;
  consuming)
    for c in 50000 1000000; do
      GROUPS_DEF+=("consuming-c${c}|${ALL_OVERLAPS}|1|${c}|1000,1000000,2147483647")
    done
    ;;
esac

# Estimated cost -- cells counted from the definitions above; per-cell time from the v7 grid (23 to 46 s per JMH cell,
# about 4 to 5 s per bisection launch) plus the timings seen running the correctness gate on these same cells: a
# 1,000,000-row consuming segment builds in about 1.5 s, fixtures load in under a second, and a full 5,000,000-row
# result takes a few seconds. These are planning figures, not measurements of this build.
#
#   scenario    cells   JMH wall clock                        bisect wall clock (3 to 5 min per cell)
#   anchors       16    ~6 to 12 min                          ~1 to 1.5 h
#   max-limit     12    ~6 to 12 min                          ~1 to 2 h
#   low-k         96    ~40 to 75 min, plus ~30 s per        ~5 to 8 h
#                       offline fixture not yet on disk
#   consuming     72    ~30 to 60 min                         ~4 to 6 h
#
# The bisection figures are the least certain numbers here: time one cell (LIMITS, OVERLAPS and MODES narrowed to a
# single value) before queueing a whole scenario.

if [[ "${DO_BISECT}" == true ]]; then
  RUN_KIND="${SCENARIO}-bisect"
else
  RUN_KIND="${SCENARIO}"
fi
OUT_DIR="${OUT_DIR:-${OUT_ROOT}/${RUN_KIND}-$(date -u +%Y%m%dT%H%M%SZ)}"
QUEUE_LOG="${OUT_DIR}/queue.log"

JVM_ARGS="-Xmx${JAVA_XMX} -XX:+UseG1GC"
if [[ "${CPU_TIME}" == "true" ]]; then
  JVM_ARGS="${JVM_ARGS} -Darm4.cpuTime=true"
fi
JMH_ARGS=(-f 1 -wi 1 -i 3 -r 5s -w 5s -prof gc -rf json)

if ! [[ "${JAVA_XMX}" =~ ^[1-9][0-9]*[mMgG]$ ]]; then
  echo "JAVA_XMX must look like 24g or 24576m, got '${JAVA_XMX}'" >&2
  exit 2
fi
if [[ ! -d "${CLASSES_DIR}" ]]; then
  echo "Not built: ${CLASSES_DIR} does not exist. Build pinot-perf first (see the header)." >&2
  exit 1
fi
if [[ ! -f "${CP_FILE}" ]]; then
  echo "Classpath file not found: ${CP_FILE}. Generate it as described in the header." >&2
  exit 1
fi
CP="${CLASSES_DIR}:$(cat "${CP_FILE}")"

mkdir -p "${OUT_DIR}" || exit 1

{
  echo "scenario: ${SCENARIO}"
  echo "mode: $([[ "${DO_BISECT}" == true ]] && echo bisect || echo jmh)"
  echo "started_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "host: $(hostname)"
  echo "cores: $(nproc)"
  echo "memory: $(free -g | awk 'NR==2 {print $2 " GiB total, " $7 " GiB available"}')"
  echo "java: $(java -version 2>&1 | tr '\n' ' ')"
  echo "jvm_default_max_heap: $(java -XX:+PrintFlagsFinal -version 2>/dev/null | awk '/ MaxHeapSize / {print $4 " bytes"}')"
  echo "git_head: $(git -C "${REPO_ROOT}" rev-parse HEAD)"
  echo "git_branch: $(git -C "${REPO_ROOT}" rev-parse --abbrev-ref HEAD)"
  echo "git_status_lines: $(git -C "${REPO_ROOT}" status --porcelain | wc -l)"
  echo "classes_dir: ${CLASSES_DIR}"
  echo "forked_jvm_args: ${JVM_ARGS}"
  echo "jmh_args: ${JMH_ARGS[*]} -jvmArgs \"${JVM_ARGS}\""
  echo "groups: ${#GROUPS_DEF[@]}"
  printf 'group: %s\n' "${GROUPS_DEF[@]}"
} >"${OUT_DIR}/environment.txt"

echo "scenario ${SCENARIO} -> ${OUT_DIR}" >&2

FAILED_GROUPS=0

# Runs one command as one queue entry: logged on both sides, never allowed to stop the queue. Output goes to the
# given log. With --dry-run it only prints what it would do.
queue_run() {
  local label="$1" log="$2"
  shift 2
  if [[ "${DRY_RUN}" == true ]]; then
    printf 'DRY-RUN %s:' "${label}"
    printf ' %q' "$@"
    printf '\n'
    return 0
  fi
  local started=${SECONDS} status
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) START ${label}" >>"${QUEUE_LOG}"
  "$@" >"${log}" 2>&1
  status=$?
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) END   ${label} exit=${status} elapsed=$((SECONDS - started))s" \
    >>"${QUEUE_LOG}"
  if [[ ${status} -ne 0 ]]; then
    FAILED_GROUPS=$((FAILED_GROUPS + 1))
  fi
}

for group in "${GROUPS_DEF[@]}"; do
  IFS='|' read -r label overlaps key_cardinality consuming_rows limits <<<"${group}"

  if [[ "${DO_BISECT}" == true ]]; then
    high_mb="${HIGH_MB:-4096}"
    if [[ "${limits}" == *2147483647* ]]; then
      high_mb="${HIGH_MB:-16384}"
    fi
    IFS=',' read -r -a order_by_list <<<"${ORDER_BYS}"
    for order_by in "${order_by_list[@]}"; do
      queue_run "${label}-${order_by}" "${OUT_DIR}/${label}-${order_by}.log" \
        env ORDER_BY="${order_by}" KEY_CARDINALITY="${key_cardinality}" CONSUMING_ROWS="${consuming_rows}" \
        LIMITS="${limits//,/ }" OVERLAPS="${overlaps//,/ }" MODES="${MODES//,/ }" HIGH_MB="${high_mb}" \
        AUDIT_LOG="${OUT_DIR}/${label}-${order_by}-audit.csv" CLASSES_DIR="${CLASSES_DIR}" \
        bash "${BISECT_SCRIPT}" "${CP_FILE}"
    done
  else
    queue_run "${label}" "${OUT_DIR}/${label}.log" \
      java -cp "${CP}" org.openjdk.jmh.Main "${BENCHMARK_CLASS}" "${JMH_ARGS[@]}" -rff "${OUT_DIR}/${label}.json" \
      -jvmArgs "${JVM_ARGS}" \
      -p "_overlap=${overlaps}" -p "_orderBy=${ORDER_BYS}" -p "_mode=${MODES}" -p "_limit=${limits}" \
      -p "_keyCardinality=${key_cardinality}" -p "_consumingRows=${consuming_rows}"
  fi
done

if [[ "${DRY_RUN}" == true ]]; then
  exit 0
fi
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) DONE failed_groups=${FAILED_GROUPS}" >>"${QUEUE_LOG}"
if [[ ${FAILED_GROUPS} -ne 0 ]]; then
  echo "${FAILED_GROUPS} group(s) failed; see ${QUEUE_LOG}" >&2
  exit 1
fi
