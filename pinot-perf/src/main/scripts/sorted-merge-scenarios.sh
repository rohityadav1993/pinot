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
#   consuming  One mutable off-heap consuming segment is added after the 100 offline ones; K=1; LIMIT 1000, 1000000 and
#              2147483647. Under ON its leaf is a full scan plus top-K, and the combine never prunes it.
#                (IN_DOMAIN at consumingRows 50000 is not run: j * rowsPerSegment / consumingRows is j, so it would hold
#                 exactly the data of TAIL c50000 FULL. Only consumingRows 1000000 differs between the shapes.)
#                TAIL (consuming tail beyond offline keys), consumingRows 50000 and 1000000, all three overlaps:
#                  72 cells. The consuming keys run past (or, at 50000, sit inside) the offline keys, so at a small
#                  LIMIT the segment is scanned in full but adds nothing to the result.
#                IN_DOMAIN (consuming keys rescaled into the offline key domain), FULL only, consumingRows 1000000:
#                  12 cells. The consuming rows interleave with the offline rows and compete for the LIMIT,
#                  which TAIL cannot show. FULL only because DISJOINT and PARTIAL have no single key domain.
#              The limits are one small (the leaf's full scan is the whole cost and the merge's is negligible), one
#              large (the headline cell, where the merge's retention matters) and the maximum (everything returns,
#              consuming rows included). 10000 and 100000 are left out: they sit between regimes the two ends already
#              bracket, and each added limit costs 12 cells per TAIL consumingRows value. The no-consuming baselines
#              for these limits come from the anchors and max-limit scenarios (K=1, consumingRows=0) rather than being
#              re-run here, so compare across scenarios only for runs from the same build on the same host.   84 cells.
#
# JMH settings, pinned, and which cells get which. LIMITs below 1000000 run in the benchmark's AverageTime mode,
# `-bm avgt -wi 1 -i 3 -r 5s -w 5s`. LIMITs of 1000000 and above run single-shot, `-bm ss -wi 3 -i 10`: a single
# invocation there takes seconds and allocates about a gigabyte, so three 5 s iterations would hold one or two samples
# each and the "average" would be a handful of operations. Both use `-f 1 -prof gc -rf json`. The two modes are
# separate JMH invocations, hence separate output files: `<group>-avgt.json` and `<group>-ss.json`. Every JMH result
# file records its own mode and iteration counts, and the START line of queue.log repeats them next to the file name.
# The forked JVM gets -Xmx${JAVA_XMX} and -XX:+UseG1GC and nothing else (plus -Darm4.cpuTime=true when CPU_TIME=true).
# Change those through the environment and the change lands in environment.txt, never silently.
#
# Reading single-shot results. Each iteration is exactly one invocation:
#   * the score is that invocation's time in ms, averaged over the 10 measured iterations; the error term is a real
#     spread across 10 independent operations, not over a count of operations inside a 5 s window;
#   * gc.alloc.rate.norm is bytes allocated by that one invocation, which is what it always meant, and reads correctly;
#     gc.alloc.rate (MB/s) is that invocation's allocation over its own duration; gc.count and gc.time are per iteration;
#   * the aux counters reset every iteration, so segmentOps (and cpuOps) is 1 per iteration and 10 in the aggregate,
#     and `segmentsProcessed / segmentOps` (or `cpuNanos / cpuOps`) is still the per-operation figure;
#   * warmup is 3 single invocations plus the one setup probe, so the JIT is warm for the long loops but a first
#     measured iteration can still carry some compilation -- look at the raw per-iteration values, not only the mean;
#   * with a 24g heap the garbage one invocation leaves is often collected during a LATER invocation, so gc.time,
#     gc.count and even a single iteration's wall time can be charged to the wrong operation. Read the median and the
#     raw per-iteration values and gc.time over all 10 iterations, not just the mean of the score;
#   * these cells are not comparable with earlier AverageTime runs: LIMIT 1000000 ran under AverageTime in v7 and now
#     runs single-shot, so a difference across that change may be the mode, not the code.
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
# benchmark means running stale classes. environment.txt warns when any source file is newer than the compiled
# classes. The first run of a K builds that K's offline fixtures (three overlaps x 100 segments x 50,000 rows each,
# multi-gigabyte, cached under the temp directory); every later run reuses them.
#
# Output, one directory per invocation: ${OUT_ROOT}/<scenario>[-bisect]-<UTC timestamp>/, or OUT_DIR if set.
#   environment.txt   host, CPU model, cores, memory, CPU governor, cgroup limits, java -version, the JVM's default
#                     MaxHeapSize, git HEAD, branch, number of dirty paths and (when dirty) a hash of `git diff HEAD`,
#                     classes age against HEAD plus a staleness warning, the path, mtime and sha256 of the pinot-core and
#                     pinot-segment-local jars on the classpath (the operators under test live there, not in pinot-perf)
#                     with a warning when a source under those modules is newer than the jar or a probe class differs
#                     from the module's target/classes, JMH and JVM arguments, scenario, groups
#   queue.log         one START and one END line per group of cells, with exit status and seconds, and the JMH settings
#                     on the START line. A failing group does not stop the queue; the script exits non-zero at the end
#                     if any group failed.
#   JMH:    <group>-avgt.json/.log and <group>-ss.json/.log
#   bisect: <group>-<orderBy>.csv (the result rows, from stdout), .log (stderr) and -audit.csv (every attempt)
# A group is the cells that share one fixture (one K, one consumingRows value and shape), so a failure costs one group.
#
# Environment: JAVA_XMX (24g), CPU_TIME (false), OUT_ROOT (/tmp/arm4-scenarios), OUT_DIR, CP_FILE (/tmp/arm4-cp.txt),
# CLASSES_DIR (<repo>/pinot-perf/target/classes). Further bisection knobs (REPLICATES, HIGH_MB, LOW_MB, ...) pass
# through to sorted-merge-bisect-xmx.sh unchanged; see its header.
#
# Bisection. --bisect runs the bisection script once per (group, ORDER BY), over the group's overlaps and limits and
# both modes, with ORDER_BY, KEY_CARDINALITY, CONSUMING_ROWS, CONSUMING_SHAPE, LIMITS and OVERLAPS set to match.
#   * HIGH_MB defaults to 4096, which suits 1000000-row results but not 2147483647, where 5,000,000 rows return and the
#     answer is above it; groups with that limit default HIGH_MB to 16384.
#   * LOW_MB defaults to 64 and is raised for consuming groups, because the consuming segment's realtime inverted index
#     lives on the heap and the fixture build itself must fit under the floor or the cell reports
#     ERROR_FIXTURE_NOT_READY. Measured on this host, with K=1, there are two different numbers and the floor follows
#     the second one, not the first:
#       - the RETAINED on-heap footprint after a System.gc() (logged as `on-heap retained`): 11.4 MiB for 50,000 rows (any
#         overlap), 212 MiB for 1,000,000 rows with TAIL, 14.1 MiB for 1,000,000 rows with IN_DOMAIN;
#       - the PEAK heap the build needs, which is what decides whether it fits under -Xmx and is larger than the retained
#         figure by whatever garbage the build produces on the way. Probed with single `run` launches at -Xms=-Xmx:
#         1,000,000 TAIL fails to build in 192 MiB and builds in 256 MiB; 1,000,000 IN_DOMAIN built in 64 MiB in 6 of 6
#         launches (3 ON, 3 OFF) and did not get as far as the build line in 32 or 48 MiB (2 launches each), so its build
#         floor is in (48, 64] MiB; 50,000 rows IN_DOMAIN printed its build line in 64 MiB in 3 of 3 launches.
#     The floors are therefore 64 (50,000 rows; and 1,000,000 IN_DOMAIN, which sits right on the build floor with no
#     margin, so an ERROR_FIXTURE_NOT_READY row from that group means raise LOW_MB to 80 and rerun it, it is not a
#     result) and 320 (1,000,000 TAIL: 1.25x the smallest heap it built in, 256 MiB; the margin is traded for
#     resolution at small limits, and REPEATS guards the build noise). A LOW_MB in the environment overrides all.
#   * The operator, not the build, sets ON's minimum at small limits. At LIMIT 1000 with FULL overlap, ON failed in the
#     operator at 64 MiB with a 50,000-row and with a 1,000,000-row IN_DOMAIN segment (3 of 3 each, after the build line
#     had printed) and survived at 72, 80 and 96 MiB (3 of 3 each); OFF survived at 64 MiB. The ON-versus-OFF
#     difference at small limits is therefore a few MiB, near the floor, which is why the floor's resolution matters.
#   * Resolution caveat for consuming-c1000000 (TAIL), floor 320. At small limits both arms may still survive at 320 MiB
#     and report AT_OR_BELOW_LOW, which hides ON-versus-OFF heap differences of the size seen above. In that group only
#     the cells whose answer is above 320 MiB carry an ON-versus-OFF comparison; for the others the group says only
#     "fits in 320 MiB". The floor cannot go much lower without losing the build itself (it fails at 192 MiB).
#   * Absolute minimum-heap numbers for consuming cells include that on-heap footprint. ON versus OFF at the same
#     consumingRows and shape stays valid; comparing a consuming cell against a consumingRows=0 anchor needs the footprint
#     above (it is logged on the `built consuming segment` line as `on-heap retained`) subtracted first.
# Cost is dominated by JVM launches: per cell roughly REPLICATES x (2 range checks + ~7 bisection steps) x (REPEATS: 3 for
# OFF, 2 for ON), about 36 to 54 launches, each paying the fixture's mmap load, the operation itself and, for consuming
# groups, a rebuild of the consuming segment (about 1.5 s for 1,000,000 rows).
#
# Estimated cost. Cells are counted from the definitions above. Per-operation times are from short calibration probes
# on this host, FULL overlap, cold JVM, so upper bounds for a warm operation: LIMIT 1000 about 0.5 s on either arm;
# 1000000 about 8 s OFF and 1.2 to 2 s ON; 2147483647 about 22 to 24 s OFF and 3 to 4 s ON; a consuming segment adds
# little. JMH: an avgt cell is 23 to 46 s (the v7 grid); a single-shot cell is fork and setup (about 5 s) plus 14
# operations (the setup probe, 3 warmup, 10 measured). Bisection: about 3 s per launch at LIMIT 1000, 10 s at 1000000
# OFF (3 s ON), 26 s at 2147483647 OFF (6 s ON).
#
#   scenario              cells  JMH wall clock         bisect wall clock
#   anchors                 16   ~11 to 16 min          ~1 to 1.5 h
#   max-limit               12   ~30 to 45 min          ~2.5 to 3.5 h
#   low-k                   96   ~70 to 100 min         ~6 to 8 h
#   consuming (TAIL)        72   ~105 to 120 min        ~8 to 10 h
#   consuming (IN_DOMAIN)   12   ~25 to 30 min          ~1.5 to 2 h
#   consuming (both)        84   ~2.2 h                 ~10 to 12 h
#   all four scenarios     208   ~4.5 to 5 h            ~20 to 24 h
#
# These are planning figures good to perhaps +/-40%, and the bisection ones are the least certain: time one cell
# (LIMITS, OVERLAPS and MODES narrowed to a single value) before queueing a whole scenario. The single-shot cells
# dominate: the OFF arm at LIMIT 2147483647 alone is about five minutes of JMH per cell.
#
# Reading consuming results. A TAIL cell's printed overlap depth is measured at the midpoint of the global key range, so
# once consumingRows is much larger than rowsPerSegment the consuming segment alone decides it (FULL and PARTIAL both
# read 1 at 1000000). Label those cells by overlap, shape and consumingRows, not by depth. IN_DOMAIN reads 101.
#
# ORDER BY shapes and fixture version. TS_ONLY (ORDER BY tsCol) has a fully sorted prefix: the per-cursor block is
# min(limit + offset, 10000) and ON takes its "no tail to sort" path. TS_VAL (ORDER BY tsCol, valCol) is meant to have a
# prefix of 1 of 2: block 10000 and ON takes its "tail to sort" path (a bounded per-run buffer), OFF the partially
# sorted path. That requires valCol NOT to be sorted in the segment. Fixtures keyed without `_v2` (everything built
# before the valCol permutation) had valCol ascending, so Pinot flagged it sorted, the prefix was 2, and TS_VAL ran the
# TS_ONLY plan: ANY earlier TS_VAL result, JMH or bisection, is a TS_ONLY result and must not be reported as TS_VAL.
# Fixture version 2 permutes valCol within each segment (valCol = segment*rowsPerSegment + (j*7919) % rows), checks
# that no offline segment reports valCol sorted, and the driver checks the sorted prefix per segment before any run.
# The old fixture directories are left on disk, unused. The first run of each K rebuilds its offline fixtures.
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

# LIMITs at or above this run single-shot; see the header.
SS_MIN_LIMIT=1000000

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

# One group per fixture: "label|overlaps|keyCardinality|consumingRows|limits|consumingShape|lowMb", comma-separated lists.
# lowMb is empty where the bisection script's default floor applies; see the header for the consuming values.
GROUPS_DEF=()
case "${SCENARIO}" in
  anchors)
    GROUPS_DEF+=("anchors|FULL,PARTIAL|1|0|1000,1000000|TAIL|")
    ;;
  max-limit)
    GROUPS_DEF+=("max-limit|${ALL_OVERLAPS}|1|0|2147483647|TAIL|")
    ;;
  low-k)
    for k in 1 100 10000 50000; do
      GROUPS_DEF+=("low-k-k${k}|${ALL_OVERLAPS}|${k}|0|1000,1000000|TAIL|")
    done
    ;;
  consuming)
    GROUPS_DEF+=("consuming-c50000|${ALL_OVERLAPS}|1|50000|1000,1000000,2147483647|TAIL|64")
    GROUPS_DEF+=("consuming-c1000000|${ALL_OVERLAPS}|1|1000000|1000,1000000,2147483647|TAIL|320")
    GROUPS_DEF+=("consuming-indomain-c1000000|FULL|1|1000000|1000,1000000,2147483647|IN_DOMAIN|64")
    ;;
esac

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
JMH_COMMON=(-f 1 -prof gc -rf json)
JMH_AVGT=(-bm avgt -wi 1 -i 3 -r 5s -w 5s)
JMH_SS=(-bm ss -wi 3 -i 10)

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

# First readable value among the given files, or "unavailable".
first_readable() {
  local f
  for f in "$@"; do
    if [[ -r "${f}" ]]; then
      tr '\n' ' ' <"${f}"
      return
    fi
  done
  echo "unavailable"
}

# The compiled classes must not be older than the sources they claim to be: a benchmark run from stale classes
# measures code nobody is reading. Newest class file against any Java source in pinot-perf modified after it.
newest_class="$(find "${CLASSES_DIR}" -name '*.class' -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1)"
newest_class_path="${newest_class#* }"
newest_class_time="$(date -u -d "@${newest_class%% *}" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || echo unknown)"
stale_sources=""
if [[ -n "${newest_class_path}" ]]; then
  stale_sources="$(find "${REPO_ROOT}/pinot-perf/src/main/java" -name '*.java' -newer "${newest_class_path}" | head -5)"
fi
git_dirty_lines="$(git -C "${REPO_ROOT}" status --porcelain | wc -l)"

# The operators under test are not in pinot-perf: they live in pinot-core and pinot-segment-local and are reached
# through the jars on CP_FILE (normally ~/.m2), so a fresh pinot-perf can still be measuring an old operator. For each
# of those modules this records the jar's path, file mtime and sha256, and runs two staleness checks:
#   1. a source file under <module>/src/main/java newer than the jar FILE's mtime. The file mtime, never the entry
#      times inside the jar: Pinot builds reproducibly, so every entry is stamped from the HEAD commit time and says
#      nothing about when the jar was built;
#   2. one representative class from the jar compared byte for byte with <module>/target/classes, which is where
#      `install` took the jar's contents from. This is the sturdier check (it does not depend on filesystem times) but
#      only works when the module has been built in this checkout; otherwise it reports "not compared".
# Neither can prove the jar matches HEAD; the sha256 is recorded so that two runs can at least be told apart.
jar_report=()
jar_warnings=()
check_operator_jar() {
  local module="$1" probe_class="$2" jar newer jar_time cmp_result
  jar="$(tr ':' '\n' <"${CP_FILE}" | grep -E "/${module}-[^/]*\.jar$" | grep -v -- '-tests\.jar$' | head -1)"
  if [[ -z "${jar}" || ! -f "${jar}" ]]; then
    jar_warnings+=("${module}: no jar on the classpath in ${CP_FILE}, so this run may not be using the operator code in the checkout")
    return
  fi
  jar_time="$(date -u -r "${jar}" +%Y-%m-%dT%H:%M:%SZ)"
  jar_report+=("${module}_jar: ${jar} mtime=${jar_time} sha256=$(sha256sum "${jar}" | cut -d' ' -f1)")
  newer="$(find "${REPO_ROOT}/${module}/src/main/java" -name '*.java' -newer "${jar}" 2>/dev/null | head -3 | tr '\n' ' ')"
  if [[ -n "${newer}" ]]; then
    jar_warnings+=("${module}: sources newer than ${jar} (${jar_time}): ${newer}")
  fi
  local built="${REPO_ROOT}/${module}/target/classes/${probe_class}"
  if [[ -f "${built}" ]]; then
    if unzip -p "${jar}" "${probe_class}" 2>/dev/null | cmp -s - "${built}"; then
      cmp_result="${probe_class##*/} in the jar is identical to ${module}/target/classes"
    else
      cmp_result="DIFFERS"
      jar_warnings+=("${module}: ${probe_class##*/} in ${jar} differs from ${module}/target/classes; reinstall the module")
    fi
  else
    cmp_result="not compared (${module}/target/classes/${probe_class} does not exist)"
  fi
  jar_report+=("${module}_jar_class_check: ${cmp_result}")
}
check_operator_jar pinot-core org/apache/pinot/core/operator/combine/StreamingSelectionOrderByCombineOperator.class
check_operator_jar pinot-segment-local org/apache/pinot/segment/local/indexsegment/mutable/MutableSegmentImpl.class

{
  echo "scenario: ${SCENARIO}"
  echo "mode: $([[ "${DO_BISECT}" == true ]] && echo bisect || echo jmh)"
  echo "started_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "host: $(hostname)"
  echo "cpu_model: $(lscpu 2>/dev/null | awk -F: '/Model name/ {gsub(/^ +/, "", $2); print $2; exit}')"
  echo "cores: $(nproc)"
  echo "cpu_scaling_governor: $(first_readable /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)"
  echo "memory: $(free -g | awk 'NR==2 {print $2 " GiB total, " $7 " GiB available"}')"
  echo "cgroup_memory_limit: $(first_readable /sys/fs/cgroup/memory.max /sys/fs/cgroup/memory/memory.limit_in_bytes)"
  echo "cgroup_cpu_limit: $(first_readable /sys/fs/cgroup/cpu.max /sys/fs/cgroup/cpu/cpu.cfs_quota_us)"
  echo "java: $(java -version 2>&1 | tr '\n' ' ')"
  echo "jvm_default_max_heap: $(java -XX:+PrintFlagsFinal -version 2>/dev/null | awk '/ MaxHeapSize / {print $4 " bytes"}')"
  echo "git_head: $(git -C "${REPO_ROOT}" rev-parse HEAD)"
  echo "git_head_commit_time: $(git -C "${REPO_ROOT}" log -1 --format=%cI)"
  echo "git_branch: $(git -C "${REPO_ROOT}" rev-parse --abbrev-ref HEAD)"
  echo "git_status_lines: ${git_dirty_lines}"
  if [[ "${git_dirty_lines}" -ne 0 ]]; then
    echo "git_diff_head_sha256: $(git -C "${REPO_ROOT}" diff HEAD | sha256sum | cut -c1-16) (tracked changes only;" \
      "untracked files are counted in git_status_lines but not hashed)"
  fi
  echo "classes_dir: ${CLASSES_DIR}"
  echo "classes_newest_file_time: ${newest_class_time}"
  if [[ -n "${stale_sources}" ]]; then
    echo "WARNING_stale_classes: these sources are newer than the newest compiled class, so the results may not be of" \
      "this code: $(tr '\n' ' ' <<<"${stale_sources}")"
  else
    echo "stale_classes_check: no pinot-perf source is newer than the newest compiled class"
  fi
  printf '%s\n' "${jar_report[@]}"
  if [[ ${#jar_warnings[@]} -gt 0 ]]; then
    printf 'WARNING_stale_operator_jar: %s\n' "${jar_warnings[@]}"
  else
    echo "operator_jar_check: no pinot-core or pinot-segment-local source is newer than its jar, and the probe classes match"
  fi
  echo "forked_jvm_args: ${JVM_ARGS}"
  echo "jmh_common: ${JMH_COMMON[*]}"
  echo "jmh_avgt (limits < ${SS_MIN_LIMIT}): ${JMH_AVGT[*]}"
  echo "jmh_ss (limits >= ${SS_MIN_LIMIT}): ${JMH_SS[*]}"
  echo "groups: ${#GROUPS_DEF[@]}"
  printf 'group: %s\n' "${GROUPS_DEF[@]}"
} >"${OUT_DIR}/environment.txt"

if [[ -n "${stale_sources}" ]]; then
  echo "WARNING: pinot-perf sources are newer than the compiled classes in ${CLASSES_DIR}; rebuild first." >&2
fi
if [[ ${#jar_warnings[@]} -gt 0 ]]; then
  printf 'WARNING: operator jar check: %s\n' "${jar_warnings[@]}" >&2
fi
echo "scenario ${SCENARIO} -> ${OUT_DIR}" >&2

FAILED_GROUPS=0

# Runs one command as one queue entry: logged on both sides, never allowed to stop the queue. `desc` is free text for
# the START line (the settings the entry used). Stdout goes to `out` and stderr to `err`; the same file for both merges
# them. With --dry-run it only prints what it would do.
queue_run() {
  local label="$1" out="$2" err="$3" desc="$4"
  shift 4
  if [[ "${DRY_RUN}" == true ]]; then
    printf 'DRY-RUN %s [%s] stdout=%s stderr=%s:' "${label}" "${desc}" "${out##*/}" "${err##*/}"
    printf ' %q' "$@"
    printf '\n'
    return 0
  fi
  local started=${SECONDS} status
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) START ${label} ${desc} -> ${out##*/}" >>"${QUEUE_LOG}"
  if [[ "${out}" == "${err}" ]]; then
    "$@" >"${out}" 2>&1
  else
    "$@" >"${out}" 2>"${err}"
  fi
  status=$?
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) END   ${label} exit=${status} elapsed=$((SECONDS - started))s" >>"${QUEUE_LOG}"
  if [[ ${status} -ne 0 ]]; then
    FAILED_GROUPS=$((FAILED_GROUPS + 1))
  fi
}

# Splits a comma-separated limit list into the ones that run AverageTime and the ones that run single-shot.
split_limits() {
  local limits="$1" limit
  AVGT_LIMITS=""
  SS_LIMITS=""
  IFS=',' read -r -a limit_list <<<"${limits}"
  for limit in "${limit_list[@]}"; do
    if (( limit >= SS_MIN_LIMIT )); then
      SS_LIMITS+="${SS_LIMITS:+,}${limit}"
    else
      AVGT_LIMITS+="${AVGT_LIMITS:+,}${limit}"
    fi
  done
}

# One JMH invocation over one group's cells at one set of mode settings. `suffix` is avgt or ss.
jmh_run() {
  local label="$1" suffix="$2" limits="$3" overlaps="$4" key_cardinality="$5" consuming_rows="$6" shape="$7"
  shift 7
  queue_run "${label}-${suffix}" "${OUT_DIR}/${label}-${suffix}.log" "${OUT_DIR}/${label}-${suffix}.log" \
    "jmh $* ${JMH_COMMON[*]} limits=${limits}" \
    java -cp "${CP}" org.openjdk.jmh.Main "${BENCHMARK_CLASS}" "${JMH_COMMON[@]}" "$@" \
    -rff "${OUT_DIR}/${label}-${suffix}.json" -jvmArgs "${JVM_ARGS}" \
    -p "_overlap=${overlaps}" -p "_orderBy=${ORDER_BYS}" -p "_mode=${MODES}" -p "_limit=${limits}" \
    -p "_keyCardinality=${key_cardinality}" -p "_consumingRows=${consuming_rows}" -p "_consumingShape=${shape}"
}

for group in "${GROUPS_DEF[@]}"; do
  IFS='|' read -r label overlaps key_cardinality consuming_rows limits shape low_mb <<<"${group}"

  if [[ "${DO_BISECT}" == true ]]; then
    high_mb="${HIGH_MB:-4096}"
    if [[ "${limits}" == *2147483647* ]]; then
      high_mb="${HIGH_MB:-16384}"
    fi
    # The group's floor unless the caller set one; empty means the bisection script's own default.
    group_low_mb="${LOW_MB:-${low_mb}}"
    IFS=',' read -r -a order_by_list <<<"${ORDER_BYS}"
    for order_by in "${order_by_list[@]}"; do
      env_args=(ORDER_BY="${order_by}" KEY_CARDINALITY="${key_cardinality}" CONSUMING_ROWS="${consuming_rows}"
        CONSUMING_SHAPE="${shape}" LIMITS="${limits//,/ }" OVERLAPS="${overlaps//,/ }" MODES="${MODES//,/ }"
        HIGH_MB="${high_mb}" AUDIT_LOG="${OUT_DIR}/${label}-${order_by}-audit.csv" CLASSES_DIR="${CLASSES_DIR}")
      if [[ -n "${group_low_mb}" ]]; then
        env_args+=(LOW_MB="${group_low_mb}")
      fi
      # The CSV is the bisection script's stdout and its progress and errors are stderr, so they must not share a file.
      queue_run "${label}-${order_by}" "${OUT_DIR}/${label}-${order_by}.csv" "${OUT_DIR}/${label}-${order_by}.log" \
        "bisect ${env_args[*]}" env "${env_args[@]}" bash "${BISECT_SCRIPT}" "${CP_FILE}"
    done
  else
    split_limits "${limits}"
    if [[ -n "${AVGT_LIMITS}" ]]; then
      jmh_run "${label}" avgt "${AVGT_LIMITS}" "${overlaps}" "${key_cardinality}" "${consuming_rows}" "${shape}" \
        "${JMH_AVGT[@]}"
    fi
    if [[ -n "${SS_LIMITS}" ]]; then
      jmh_run "${label}" ss "${SS_LIMITS}" "${overlaps}" "${key_cardinality}" "${consuming_rows}" "${shape}" \
        "${JMH_SS[@]}"
    fi
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
