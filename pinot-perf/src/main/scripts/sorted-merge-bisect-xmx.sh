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
# WHAT THIS MEASURES, AND WHAT IT DOES NOT
#
# This script searches for the smallest -Xmx under which a configuration
# completes: its *minimum surviving heap under pinned G1*. That is an
# operational quantity -- what an operator has to provision for this workload to
# run at all -- and it is the only thing the numbers below support.
#
# It is NOT a measurement of peak live heap, and must not be quoted as one. A
# JVM fails at `live set + GC headroom + fragmentation`, and headroom scales
# with allocation rate, which is the one thing the two arms differ in by about
# 5x (OFF ~1.4 GB/op vs ON ~300 MB/op). At equal live set the OFF arm therefore
# dies at a higher heap from churn alone. So a ratio read off this grid is a
# ratio of provisioning requirements, not of retention. Measuring retention
# needs a different instrument (max post-full-GC occupancy under -Xlog:gc);
# that instrument is deliberately not built here.
# ==============================================================================

set -u -o pipefail

CP_FILE="${1:-/tmp/arm4-cp.txt}"
MAIN_CLASS="org.apache.pinot.perf.sortedmerge.SortedMergeMemoryProbe"
CLASSES_DIR="${CLASSES_DIR:-pinot-perf/target/classes}"

# Decision points only. The full 36-cell grid is instrument 1's job; bisection is far too slow to sweep. FULL is
# included because it is the only overlap where depth(L) reaches NUM_SEGMENTS, i.e. the only one that exercises the
# behaviour this instrument exists to bound; with W=10 the model puts its crossover at LIMIT 100000, inside the sweep.
#
# Budget: 24 cells, each measured REPLICATES times, at roughly 10 attempts per replicate (a HIGH_MB ceiling check, a
# LOW_MB floor check, and the bisection steps a relative tolerance leaves). REPEATS_OFF and REPEATS_ON multiply every
# attempt again. At the defaults that is on the order of 900 JVM launches, so expect hours rather than the ~19 minutes
# the single-replicate v3 grid took. Time one cell before launching the whole thing.
#
# The three axes are overridable as space-separated strings, which is what makes the self-test
# (sorted-merge-bisect-xmx-selftest.sh) able to exercise the classification logic on a two-cell grid in seconds
# instead of running the full sweep. Any override changes what the numbers mean, so the banner below prints the
# grid actually used -- a run's own output has to say what it measured.
read -r -a LIMITS <<<"${LIMITS:-1000 10000 100000 1000000}"
read -r -a OVERLAPS <<<"${OVERLAPS:-DISJOINT PARTIAL FULL}"
read -r -a MODES <<<"${MODES:-OFF ON}"

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
# Measured 2026-09-18: the two arms are not equally repeatable in *workload*. Two identical full-scale gate runs
# produced identical segments-processed counts for ON in all 18 cells, and different counts for OFF in five of them
# (DISJOINT@100k 16 vs 12, PARTIAL@10 3 vs 7, PARTIAL@100 3 vs 2, PARTIAL@1000 4 vs 6, PARTIAL@100k 26 vs 27). OFF's
# min/max pruning races against tasks that have already been dispatched, so how many segments are in flight -- and
# therefore how much it retains -- depends on thread scheduling. That is why OFF repeats more than ON.
#
# ON does not repeat only once, though. Identical segments-processed counts show the *workload* is deterministic;
# they say nothing about GC and allocation timing, which is what actually varies at the OOM boundary and is
# nondeterministic for both arms however fixed the workload is. So ON gets at least two runs as well, and the
# resulting figure is "minimum heap surviving N consecutive runs", with N recorded in the output so the number
# cannot be read as something stronger than it is.
REPEATS_OFF="${REPEATS_OFF:-3}"
REPEATS_ON="${REPEATS_ON:-2}"

# How many independent times each cell is measured end to end.
#
# Survival near the boundary is a sigmoid, not a step: requiring R consecutive survivals converges on the p^R ~ 0.5
# contour, an arbitrary and unstated quantile, and which contour it lands on depends on the draws it happened to get.
# Two runs of a single-replicate search can therefore disagree with nothing in the output saying so. Measuring each
# cell more than once and emitting every replicate as its own row makes that disagreement visible instead of hiding
# it behind an average. Bisection also assumes the survival predicate is monotone in heap size, which nothing checks;
# the per-attempt audit log is what makes a non-monotone response recoverable after the fact.
REPLICATES="${REPLICATES:-2}"

LOW_MB="${LOW_MB:-64}"
HIGH_MB="${HIGH_MB:-4096}"

# Stop bisecting once the bracket is within TOLERANCE_PCT of the upper bound, but never finer than
# TOLERANCE_MIN_MB. A single absolute tolerance cannot serve both ends of a 64-4096 range: the old flat 32 MB was
# half the floor (far too coarse to distinguish a cell that needs 70 MB from one that needs 100) and 0.8% of the
# ceiling (several bisection steps spent resolving noise on a 4 GB cell).
TOLERANCE_PCT="${TOLERANCE_PCT:-5}"
TOLERANCE_MIN_MB="${TOLERANCE_MIN_MB:-16}"

# Wall-clock ceiling per JVM launch. A JVM thrashing just above its limit can make progress too slowly to finish
# while never tripping the GC overhead limit, which hangs the queue indefinitely. TERM first so the JVM runs its
# shutdown hooks (which do not delete the on-disk fixture unless -Darm4.fixture.keep=false), then KILL as a backstop
# for a JVM too wedged to respond.
ATTEMPT_TIMEOUT_S="${ATTEMPT_TIMEOUT_S:-900}"
ATTEMPT_KILL_GRACE_S="${ATTEMPT_KILL_GRACE_S:-60}"

# One line per JVM launch. Roughly 900 attempts otherwise collapse into 48 CSV rows, which makes a suspicious
# convergence or a non-monotone response impossible to reconstruct afterwards.
AUDIT_LOG="${AUDIT_LOG:-/tmp/arm4-bisect-audit-$(date +%Y%m%dT%H%M%S).csv}"

# TOLERANCE_MB was this script's tolerance knob until the relative tolerance above replaced it. Silently ignoring a
# value someone deliberately set is how a run gets done at settings nobody chose, so refuse instead.
if [[ -n "${TOLERANCE_MB:-}" ]]; then
  echo "TOLERANCE_MB is no longer used. Set TOLERANCE_PCT (percent of the upper bound) and TOLERANCE_MIN_MB" >&2
  echo "(absolute floor) instead; the tolerance is now relative to where in the range the search is." >&2
  exit 1
fi

# A zero or negative repeat count would make attempt()'s loop body never execute, so every heap size would report as
# surviving and the script would confidently emit the floor for all 24 cells. The same class of mistake in any of the
# other knobs produces a quieter version of the same lie, so all of them are checked.
for int_var in REPEATS_OFF REPEATS_ON REPLICATES LOW_MB HIGH_MB TOLERANCE_PCT TOLERANCE_MIN_MB ATTEMPT_TIMEOUT_S \
    ATTEMPT_KILL_GRACE_S NUM_SEGMENTS ROWS_PER_SEGMENT BLOCK_SIZE MAX_EXECUTION_THREADS KEY_CARDINALITY; do
  if ! [[ "${!int_var}" =~ ^[1-9][0-9]*$ ]]; then
    echo "${int_var} must be a positive integer, got '${!int_var}'" >&2
    exit 1
  fi
done

if (( TOLERANCE_PCT > 100 )); then
  echo "TOLERANCE_PCT must be between 1 and 100, got ${TOLERANCE_PCT}" >&2
  exit 1
fi
if (( LOW_MB >= HIGH_MB )); then
  echo "LOW_MB (${LOW_MB}) must be below HIGH_MB (${HIGH_MB})" >&2
  exit 1
fi

# Presence is not enough. attempt_once depends on two GNU-specific behaviours: the --kill-after flag, and the exit
# codes 124 (the TERM fired) and 137 (the KILL backstop was needed), which are how a timed-out attempt is told apart
# from an OOM. A busybox or toybox timeout would fail before java ever ran, with an exit code matching none of the
# classifications, and every cell in the grid would be reported as ERROR_FIXTURE_NOT_READY -- a wrong answer that
# looks like a fixture problem. So probe the actual behaviour rather than the name.
if ! command -v timeout >/dev/null 2>&1; then
  echo "GNU coreutils 'timeout' is required but not on PATH." >&2
  exit 1
fi
if ! timeout --kill-after=1 1 true >/dev/null 2>&1; then
  echo "'timeout' on this host does not accept --kill-after; GNU coreutils timeout is required." >&2
  exit 1
fi
timeout --kill-after=1 1 sleep 5 >/dev/null 2>&1
if [[ $? -ne 124 ]]; then
  echo "'timeout' on this host does not exit 124 on timeout, so timed-out attempts cannot be told apart from" >&2
  echo "OutOfMemoryError. GNU coreutils timeout is required." >&2
  exit 1
fi

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

# Pinned so the figures are reproducible and can be read alongside the 4 GB JMH runs. Minimum surviving heap depends
# strongly on the collector (region overhead, humongous allocation threshold, IHOP) and on whether the heap is allowed
# to grow adaptively, so leaving either to the JDK's defaults makes the number specific to the JDK that produced it.
# -Xms is set equal to -Xmx per attempt, in attempt_once.
JVM_FLAGS=(-XX:+UseG1GC -XX:+ExitOnOutOfMemoryError)

# Identifies the attempt currently in flight, for the audit log. Set by the cell loop and by attempt() rather than
# threaded through every signature, which would make the call sites unreadable for no gain.
REPLICATE=0
REPEAT_INDEX=0

# Establish the audit log before anything else runs, and treat failure as fatal. Every later write is a bare
# append whose failure would only print to stderr, so a bad AUDIT_LOG override would quietly remove the audit
# trail this whole change is built around and nobody would find out until hours later.
mkdir -p "$(dirname "${AUDIT_LOG}")" 2>/dev/null
if ! echo "replicate,attempt_utc,overlap,mode,limit,heap_mb,repeat_index,outcome,elapsed_s,detail" \
    >"${AUDIT_LOG}"; then
  echo "Cannot write the per-attempt audit log at ${AUDIT_LOG}" >&2
  exit 1
fi

# Two levels of vocabulary, deliberately: `outcome` here is what one JVM launch did, so OK and OOM are ordinary
# results rather than failures. The ERROR_-prefixed tokens in the main CSV are cell-level verdicts, and mean the
# cell was abandoned without an answer. An OOM is never an ERROR_ at cell level -- it is the signal the search runs on.
audit() {
  local overlap="$1" mode="$2" limit="$3" heap_mb="$4" outcome="$5" elapsed="$6" detail="$7"
  printf '%d,%s,%s,%s,%d,%d,%d,%s,%d,%s\n' "${REPLICATE}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${overlap}" "${mode}" \
      "${limit}" "${heap_mb}" "${REPEAT_INDEX}" "${outcome}" "${elapsed}" "${detail}" >>"${AUDIT_LOG}"
}

# Runs one configuration at one heap size.
#
# The fixture must be ready before the merge starts, so at very small -Xmx a run can die during fixture setup
# rather than during the merge, which would record the fixture's own footprint as the operator's. The probe
# prints "[arm4] built" when it generates the fixture and "[arm4] reused" when it mmap-loads an existing one;
# either line means setup completed, and a failure with neither is reported as exit 2 and excluded from the
# search rather than folded into it.
#
# In practice every attempt after the first should log "reused" -- the fixture persists on disk between JVMs by
# default. A run of this script where attempts keep logging "built" means on-disk reuse is not working, and the
# script will take many hours longer than it should. That is worth stopping for.
#
# The probe distinguishes its failure modes and so must this: `SortedMergeMemoryProbe` exits 3 on
# OutOfMemoryError and 1 on any other Throwable, including a failed G1 engagement check. Treating exit 1 as an
# OOM would raise `low` exactly as a real OOM does, so a plain bug in the harness would be silently reported as
# "this arm needs more memory" while the stack trace explaining it was discarded. Any exit that is neither 0 nor
# an OOM therefore abandons the cell and prints what the probe said.
#
# Exit codes: 0 = completed, 1 = OOM during the merge (a real operator measurement),
#             2 = failed before the fixture was ready (NOT an operator measurement -- must not steer the search),
#             3 = the probe failed for some other reason (NOT a measurement -- the cell is abandoned and reported),
#             4 = the attempt exceeded ATTEMPT_TIMEOUT_S (NOT a measurement -- the cell is abandoned and reported).
attempt_once() {
  local overlap="$1" mode="$2" limit="$3" heap_mb="$4"
  local out status started elapsed
  started=${SECONDS}
  # Trailing "TS_VAL" pins the pre-existing query shape explicitly so KEY_CARDINALITY can occupy the argument slot
  # after it; SortedMergeMemoryProbe's CLI is positional, so KEY_CARDINALITY cannot be passed without naming the
  # orderBy argument that comes before it. TS_VAL is the probe's own default, so this changes nothing when
  # KEY_CARDINALITY is left at 1.
  out=$(timeout --kill-after="${ATTEMPT_KILL_GRACE_S}" "${ATTEMPT_TIMEOUT_S}" \
        java -Xms"${heap_mb}"m -Xmx"${heap_mb}"m "${JVM_FLAGS[@]}" -cp "${CP}" "${MAIN_CLASS}" \
        run "${overlap}" "${mode}" "${limit}" "${NUM_SEGMENTS}" "${ROWS_PER_SEGMENT}" "${BLOCK_SIZE}" \
        "${MAX_EXECUTION_THREADS}" TS_VAL "${KEY_CARDINALITY}" 2>&1)
  status=$?
  elapsed=$(( SECONDS - started ))

  if [[ ${status} -eq 0 ]]; then
    # segmentsProcessed varies run to run for OFF, and a bisection that walked a non-monotone response leaves no
    # other trace of why. Recording it per attempt is what makes that reconstructable.
    audit "${overlap}" "${mode}" "${limit}" "${heap_mb}" OK "${elapsed}" \
        "$(grep -oE 'segmentsProcessed=[0-9]+' <<<"${out}" | tail -1)"
    return 0
  fi

  # timeout reports 124 when its TERM fired, and 137 when the KILL backstop was needed. 137 is also what the kernel's
  # OOM killer produces, so the exit code alone cannot separate them; an attempt that ran for at least the timeout
  # did so because of the timeout. Checked before the OOM classification, because a wedged JVM must not be recorded
  # as evidence that this arm needs more heap.
  if [[ ${status} -eq 124 ]] || { [[ ${status} -eq 137 ]] && (( elapsed >= ATTEMPT_TIMEOUT_S )); }; then
    audit "${overlap}" "${mode}" "${limit}" "${heap_mb}" TIMEOUT "${elapsed}" ""
    echo "attempt exceeded ${ATTEMPT_TIMEOUT_S}s for ${overlap}/${mode}/limit=${limit} at ${heap_mb}MB" >&2
    return 4
  fi

  if ! grep -qE "\[arm4\] (built|reused)" <<<"${out}"; then
    audit "${overlap}" "${mode}" "${limit}" "${heap_mb}" FIXTURE_NOT_READY "${elapsed}" "exit=${status}"
    return 2
  fi

  # 3 is the probe's own OutOfMemoryError exit. 137 and 143 mean the kernel or a signal stopped the JVM, which is
  # still resource exhaustion rather than a harness bug, so they steer the search the same way.
  if [[ ${status} -eq 3 || ${status} -eq 137 || ${status} -eq 143 ]]; then
    audit "${overlap}" "${mode}" "${limit}" "${heap_mb}" OOM "${elapsed}" "exit=${status}"
    return 1
  fi

  audit "${overlap}" "${mode}" "${limit}" "${heap_mb}" PROBE_FAILED "${elapsed}" "exit=${status}"
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
# Exit codes match attempt_once.
attempt() {
  local overlap="$1" mode="$2" limit="$3" heap_mb="$4"
  local repeats
  repeats=$(repeats_for "${mode}")
  local i status
  for (( i = 0; i < repeats; i++ )); do
    REPEAT_INDEX=${i}
    attempt_once "${overlap}" "${mode}" "${limit}" "${heap_mb}"
    status=$?
    if [[ ${status} -ne 0 ]]; then
      return ${status}
    fi
  done
  return 0
}

# Measures one cell once, end to end. Echoes "outcome,low_mb,high_mb", where the pair is the half-open bracket
# (low_mb, high_mb] that the search narrowed to: the arm OOMed at low_mb and survived at high_mb, and both of those
# heap sizes were actually tried. Fields are left empty where no bound was established.
measure_cell() {
  local overlap="$1" mode="$2" limit="$3"

  # Confirm the ceiling is inside the bracket before bisecting: if even HIGH_MB fails, there is no answer to
  # converge on and reporting the bracket edge as the result would be a silent lie.
  attempt "${overlap}" "${mode}" "${limit}" "${HIGH_MB}"
  case $? in
    1) echo "ABOVE_HIGH,${HIGH_MB}," ; return ;;
    2) echo "ERROR_FIXTURE_NOT_READY,," ; return ;;
    3) echo "ERROR_PROBE_FAILED,," ; return ;;
    4) echo "ERROR_TIMEOUT,," ; return ;;
    *) ;;
  esac

  # The floor counterpart, and the reason it exists: without it `low` never rises for a cell that already fits in
  # LOW_MB, and the search converges 2080 -> 1072 -> 568 -> 316 -> 190 -> 127 -> 95 and prints 95 -- a threshold
  # that was never observed, for a cell whose honest answer is "at or below 64". Every number this script emits
  # has to be a heap size it actually ran at.
  attempt "${overlap}" "${mode}" "${limit}" "${LOW_MB}"
  case $? in
    0) echo "AT_OR_BELOW_LOW,,${LOW_MB}" ; return ;;
    2) echo "ERROR_FIXTURE_NOT_READY,," ; return ;;
    3) echo "ERROR_PROBE_FAILED,," ; return ;;
    4) echo "ERROR_TIMEOUT,," ; return ;;
    *) ;;
  esac

  local low=${LOW_MB} high=${HIGH_MB} tol mid
  while :; do
    tol=$(( high * TOLERANCE_PCT / 100 ))
    if (( tol < TOLERANCE_MIN_MB )); then
      tol=${TOLERANCE_MIN_MB}
    fi
    (( high - low > tol )) || break
    mid=$(( (low + high) / 2 ))
    attempt "${overlap}" "${mode}" "${limit}" "${mid}"
    case $? in
      0) high=${mid} ;;
      1) low=${mid} ;;
      # The JVM died before the merge started, so this tells us nothing about the operator. Folding it in would
      # raise `low` exactly as a genuine operator OOM does, and the fixture's own footprint would be reported as
      # the operator's minimum heap -- the precise miscategorisation the churn/ceiling split exists to avoid.
      # There is no safe way to continue the search past an unclassifiable point, so abandon this cell.
      2) echo "ERROR_FIXTURE_NOT_READY,," ; return ;;
      # The probe threw something that is not an OOM, so this heap size says nothing about the operator, and the
      # bug it just reported is worth more than the remaining bisection steps. Abandon the cell rather than guess.
      3) echo "ERROR_PROBE_FAILED,," ; return ;;
      4) echo "ERROR_TIMEOUT,," ; return ;;
    esac
  done
  echo "BRACKET,${low},${high}"
}

# Build the fixtures once, outside the bisection, at a heap generous enough that setup cannot be what fails.
# Without this the first attempt of each overlap pays the build cost at whatever heap the bisection happens to
# be probing, and a build that OOMs there looks like an operator result.
prebuild() {
  local overlap
  for overlap in "${OVERLAPS[@]}"; do
    echo "prebuilding ${overlap} fixture..." >&2
    java -Xmx8g -XX:+UseG1GC -cp "${CP}" "${MAIN_CLASS}" run "${overlap}" OFF 10 \
         "${NUM_SEGMENTS}" "${ROWS_PER_SEGMENT}" "${BLOCK_SIZE}" "${MAX_EXECUTION_THREADS}" TS_VAL \
         "${KEY_CARDINALITY}" >&2 || {
      echo "prebuild failed for ${overlap}; aborting" >&2
      exit 1
    }
  done
}

echo "measuring minimum surviving heap under pinned G1 (-Xms=-Xmx, -XX:+UseG1GC)" >&2
echo "  overlaps: ${OVERLAPS[*]}" >&2
echo "  limits:   ${LIMITS[*]}" >&2
echo "  modes:    ${MODES[*]} (x ${REPLICATES} replicates)" >&2
echo "  keyCardinality=${KEY_CARDINALITY}, range ${LOW_MB}-${HIGH_MB}MB, per-attempt timeout ${ATTEMPT_TIMEOUT_S}s" >&2
echo "  per-attempt audit log: ${AUDIT_LOG}" >&2

prebuild

# Each row is one independent measurement of one cell, not an aggregate: replicates are emitted separately so that
# two replicates disagreeing is visible in the output rather than averaged away. The bracket is half-open --
# (low_mb, high_mb] -- and both bounds were tried, so neither is an interpolation. consecutive_runs_required is the
# unanimity threshold behind the survival at high_mb; at 1 that would be a single observation of a value that varies
# between runs, which is why no arm is set to 1.
echo "overlap,mode,limit,replicate,outcome,low_mb,high_mb,consecutive_runs_required"

for overlap in "${OVERLAPS[@]}"; do
  for limit in "${LIMITS[@]}"; do
    for mode in "${MODES[@]}"; do
      repeats=$(repeats_for "${mode}")
      for (( replicate = 1; replicate <= REPLICATES; replicate++ )); do
        REPLICATE=${replicate}
        result=$(measure_cell "${overlap}" "${mode}" "${limit}")
        echo "${overlap},${mode},${limit},${replicate},${result},${repeats}"
      done
    done
  done
done
