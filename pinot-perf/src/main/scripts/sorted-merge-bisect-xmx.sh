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

set -u -o pipefail

CP_FILE="${1:-/tmp/arm4-cp.txt}"
MAIN_CLASS="org.apache.pinot.perf.sortedmerge.SortedMergeMemoryProbe"
CLASSES_DIR="${CLASSES_DIR:-pinot-perf/target/classes}"

# Decision points only. The full 36-cell grid is instrument 1's job; bisection is far too slow to sweep. FULL is
# included because it is the only overlap where depth(L) reaches NUM_SEGMENTS, i.e. the only one that exercises the
# retention this instrument exists to measure; with W=10 the model puts its crossover at LIMIT 100000, inside the sweep.
#
# Budget: 24 cells at roughly 8 attempts each (a HIGH_MB ceiling check plus log2((4096-64)/32) bisection steps).
# REPEATS_OFF (below) triples every attempt on the 12 OFF cells, so that is about 380 JVM launches rather than the
# ~190 an unrepeated search would need.
#
# FULL/OFF at the larger limits may not fit under HIGH_MB. That is reported as ">HIGH_MB" rather than guessed at;
# raise HIGH_MB and rerun just that cell if a bound is needed.
LIMITS=(1000 10000 100000 1000000)
OVERLAPS=(DISJOINT PARTIAL FULL)
MODES=(OFF ON)

NUM_SEGMENTS="${NUM_SEGMENTS:-100}"
ROWS_PER_SEGMENT="${ROWS_PER_SEGMENT:-50000}"
BLOCK_SIZE="${BLOCK_SIZE:-10000}"
# W. Pinned rather than left to resolve from the host's core count, because the OFF arm's retention scales with
# it and an unpinned value makes results incomparable between machines.
MAX_EXECUTION_THREADS="${MAX_EXECUTION_THREADS:-10}"
# K, SortedMergeFixture's keyCardinality knob: how many consecutive rows within a segment share one tsCol value.
# Defaults to 1, which is byte-identical to the fixture this script has always built, so leaving this unset keeps
# today's behaviour exactly. Override to make the tail-to-sort path's per-run heap hold more than one row.
KEY_CARDINALITY="${KEY_CARDINALITY:-1}"

# How many consecutive runs must survive a heap size before it counts as surviving.
#
# Measured 2026-09-18: the two arms are not equally repeatable. Two identical full-scale gate runs produced identical
# segments-processed counts for ON in all 18 cells, and different counts for OFF in five of them (DISJOINT@100k 16 vs
# 12, PARTIAL@10 3 vs 7, PARTIAL@100 3 vs 2, PARTIAL@1000 4 vs 6, PARTIAL@100k 26 vs 27). OFF's min/max pruning races
# against tasks that have already been dispatched, so how many segments are in flight -- and therefore how much it
# retains -- depends on thread scheduling.
#
# That makes OFF's minimum surviving heap a distribution rather than a value. A single run per heap size converges on
# one draw from it and looks perfectly well-behaved while doing so, reporting a ceiling that a rerun would breach. So
# OFF must clear a heap size several times in a row before the bisection believes it. ON is deterministic and keeps a
# single run. The resulting figure is "minimum heap surviving N consecutive runs", and N is recorded in the output so
# the number cannot be read as something stronger than it is.
REPEATS_OFF="${REPEATS_OFF:-3}"
REPEATS_ON="${REPEATS_ON:-1}"

# A zero or negative override would make attempt()'s loop body never execute, so every heap size would report as
# surviving and the script would confidently emit LOW_MB for all 24 cells. Refuse rather than produce that.
for repeats_var in REPEATS_OFF REPEATS_ON; do
  if ! [[ "${!repeats_var}" =~ ^[1-9][0-9]*$ ]]; then
    echo "${repeats_var} must be a positive integer, got '${!repeats_var}'" >&2
    exit 1
  fi
done

LOW_MB="${LOW_MB:-64}"
HIGH_MB="${HIGH_MB:-4096}"
# Stop bisecting once the bracket is this tight. Finer than this is measuring JVM noise, not the operator.
TOLERANCE_MB="${TOLERANCE_MB:-32}"

if [[ ! -f "${CP_FILE}" ]]; then
  echo "Classpath file not found: ${CP_FILE}" >&2
  echo "Generate it with: ./mvnw -pl pinot-perf dependency:build-classpath -Dmdep.outputFile=${CP_FILE} -Ddevelocity.cache.local.enabled=false" >&2
  exit 1
fi
if [[ ! -d "${CLASSES_DIR}" ]]; then
  echo "Not built: ${CLASSES_DIR} does not exist. Compile pinot-perf first." >&2
  exit 1
fi

CP="${CLASSES_DIR}:$(cat "${CP_FILE}")"

# Runs one configuration at one heap size.
#
# The fixture must be ready before the merge starts, so at very small -Xmx a run can die during fixture setup
# rather than during the merge, which would record the fixture's own footprint as the operator's. The probe
# prints "[arm4] built" when it generates the fixture and "[arm4] reused" when it mmap-loads an existing one;
# either line means setup completed, and a failure with neither is reported as exit 2 and excluded from the
# search rather than folded into it.
#
# In practice every attempt after the first should log "reused" — the fixture persists on disk between JVMs by
# default. A run of this script where attempts keep logging "built" means on-disk reuse is not working, and the
# script will take many hours instead of about one. That is worth stopping for.
# The probe distinguishes its failure modes and so must this: `SortedMergeMemoryProbe` exits 3 on
# OutOfMemoryError and 1 on any other Throwable, including a failed G1 engagement check. Treating exit 1 as an
# OOM would raise `low` exactly as a real OOM does, so a plain bug in the harness would be silently reported as
# "this arm needs more memory" while the stack trace explaining it was discarded. Any exit that is neither 0 nor
# an OOM therefore abandons the cell and prints what the probe said.
#
# Exit codes: 0 = completed, 1 = OOM during the merge (a real operator measurement),
#             2 = failed before the fixture was ready (NOT an operator measurement — must not steer the search),
#             3 = the probe failed for some other reason (NOT a measurement — the cell is abandoned and reported).
attempt_once() {
  local overlap="$1" mode="$2" limit="$3" heap_mb="$4"
  local out
  # Trailing "TS_VAL" pins the pre-existing query shape explicitly so KEY_CARDINALITY can occupy the argument slot
  # after it; SortedMergeMemoryProbe's CLI is positional, so KEY_CARDINALITY cannot be passed without naming the
  # orderBy argument that comes before it. TS_VAL is the probe's own default, so this changes nothing when
  # KEY_CARDINALITY is left at 1.
  out=$(java -Xmx"${heap_mb}"m -XX:+ExitOnOutOfMemoryError -cp "${CP}" "${MAIN_CLASS}" \
        run "${overlap}" "${mode}" "${limit}" "${NUM_SEGMENTS}" "${ROWS_PER_SEGMENT}" "${BLOCK_SIZE}" \
        "${MAX_EXECUTION_THREADS}" TS_VAL "${KEY_CARDINALITY}" 2>&1)
  local status=$?
  if [[ ${status} -eq 0 ]]; then
    return 0
  fi
  if ! grep -qE "\[arm4\] (built|reused)" <<<"${out}"; then
    return 2
  fi
  # 3 is the probe's own OutOfMemoryError exit. 137 and 143 mean the kernel or a signal stopped the JVM, which is
  # still resource exhaustion rather than a harness bug, so they steer the search the same way.
  if [[ ${status} -eq 3 || ${status} -eq 137 || ${status} -eq 143 ]]; then
    return 1
  fi
  echo "probe failed with exit ${status} (not an OOM) for ${overlap}/${mode}/limit=${limit} at ${heap_mb}MB:" >&2
  echo "${out}" >&2
  return 3
}

# Returns the repeat count this arm requires before a heap size counts as surviving. See REPEATS_OFF above.
repeats_for() {
  if [[ "$1" == "OFF" ]]; then echo "${REPEATS_OFF}"; else echo "${REPEATS_ON}"; fi
}

# One heap size, judged over repeats_for(mode) consecutive runs. Survival must be unanimous: a single OOM in any of
# them means this heap is too small, because the question is what heap the arm needs reliably, not what it happened to
# get away with once. Exits early on the first failure -- there is nothing further to learn from this heap size.
# Exit codes match attempt_once: 0 = survived every run, 1 = OOMed during the merge, 2 = died before the fixture was
# ready, 3 = the probe failed for a non-OOM reason. The last two both abandon the cell.
attempt() {
  local overlap="$1" mode="$2" limit="$3" heap_mb="$4"
  local repeats
  repeats=$(repeats_for "${mode}")
  local i
  for (( i = 0; i < repeats; i++ )); do
    attempt_once "${overlap}" "${mode}" "${limit}" "${heap_mb}"
    local status=$?
    if [[ ${status} -ne 0 ]]; then
      return ${status}
    fi
  done
  return 0
}

# Build the fixtures once, outside the bisection, at a heap generous enough that setup cannot be what fails.
# Without this the first attempt of each overlap pays the build cost at whatever heap the bisection happens to
# be probing, and a build that OOMs there looks like an operator result.
prebuild() {
  local overlap
  for overlap in "${OVERLAPS[@]}"; do
    echo "prebuilding ${overlap} fixture..." >&2
    java -Xmx8g -cp "${CP}" "${MAIN_CLASS}" run "${overlap}" OFF 10 \
         "${NUM_SEGMENTS}" "${ROWS_PER_SEGMENT}" "${BLOCK_SIZE}" "${MAX_EXECUTION_THREADS}" TS_VAL \
         "${KEY_CARDINALITY}" >&2 || {
      echo "prebuild failed for ${overlap}; aborting" >&2
      exit 1
    }
  done
}

prebuild

# min_surviving_heap_mb is the smallest heap that survived consecutive_runs_required runs in a row, so it is only as
# strong as that column: at 1 it is a single observation of a value that may vary between runs.
echo "overlap,mode,limit,min_surviving_heap_mb,consecutive_runs_required"

for overlap in "${OVERLAPS[@]}"; do
  for limit in "${LIMITS[@]}"; do
    for mode in "${MODES[@]}"; do
      repeats=$(repeats_for "${mode}")
      # Confirm the ceiling is inside the bracket before bisecting: if even HIGH_MB fails, there is no answer
      # to converge on and reporting the bracket edge as the result would be a silent lie.
      attempt "${overlap}" "${mode}" "${limit}" "${HIGH_MB}"
      case $? in
        1) echo "${overlap},${mode},${limit},>${HIGH_MB},${repeats}" ; continue ;;
        2) echo "${overlap},${mode},${limit},ERROR_FIXTURE_NOT_READY,${repeats}" ; continue ;;
        3) echo "${overlap},${mode},${limit},ERROR_PROBE_FAILED,${repeats}" ; continue ;;
        *) ;;
      esac

      low=${LOW_MB}
      high=${HIGH_MB}
      aborted=""
      while (( high - low > TOLERANCE_MB )); do
        mid=$(( (low + high) / 2 ))
        attempt "${overlap}" "${mode}" "${limit}" "${mid}"
        case $? in
          0) high=${mid} ;;
          1) low=${mid} ;;
          # The JVM died before the merge started, so this tells us nothing about the operator. Folding it in would
          # raise `low` exactly as a genuine operator OOM does, and the fixture's own footprint would be reported as
          # the operator's minimum heap -- the precise miscategorisation the churn/ceiling split exists to avoid.
          # There is no safe way to continue the search past an unclassifiable point, so abandon this cell.
          2) aborted="ERROR_FIXTURE_NOT_READY" ; break ;;
          # The probe threw something that is not an OOM, so this heap size says nothing about the operator, and the
          # bug it just reported is worth more than the remaining bisection steps. Abandon the cell rather than guess.
          3) aborted="ERROR_PROBE_FAILED" ; break ;;
        esac
      done
      if [[ -n "${aborted}" ]]; then
        echo "${overlap},${mode},${limit},${aborted},${repeats}"
      else
        echo "${overlap},${mode},${limit},${high},${repeats}"
      fi
    done
  done
done
