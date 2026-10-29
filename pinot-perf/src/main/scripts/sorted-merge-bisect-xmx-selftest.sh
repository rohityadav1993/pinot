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

# Self-test for sorted-merge-bisect-xmx.sh.
#
# The bisection emits numbers that get quoted as results, and everything about whether those numbers are right lives
# in one place: how an exit code is classified, and how the surviving bracket is assembled from the classifications.
# A real run takes hours and cannot exercise the failure branches at all -- there is no way to make a genuine JVM
# produce an OOM-killer 137, a wedged process and a harness bug on demand. So the failure modes are injected here
# instead, with a stub standing in for java, against the two-cell grid the axis overrides make possible.
#
# Runs in a few seconds. No Pinot build required.

set -u -o pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
UNDER_TEST="${SCRIPT_DIR}/sorted-merge-bisect-xmx.sh"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

mkdir -p "${WORK_DIR}/bin" "${WORK_DIR}/classes"
echo "${WORK_DIR}/classes" >"${WORK_DIR}/cp.txt"

FAILURES=0
CASES=0

# Stub java. SCENARIO selects the failure mode to inject; the prebuild launch (-Xmx8g, which does not match the
# -Xmx<n>m pattern) always succeeds so that setup is never what a case is testing.
cat >"${WORK_DIR}/bin/java" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${ARGV_LOG}"
heap=0
for a in "$@"; do
  case "$a" in
    -Xmx*m) h=${a#-Xmx}; heap=${h%m} ;;
  esac
done
if [[ ${heap} -eq 0 ]]; then
  echo "[arm4] built"
  exit 0
fi

case "${SCENARIO}" in
  threshold)
    echo "[arm4] reused"
    if [[ ${heap} -ge 300 ]]; then
      echo "OK segmentsProcessed=12"
      exit 0
    fi
    exit 3
    ;;
  always_survives)
    echo "[arm4] reused"
    echo "OK segmentsProcessed=12"
    exit 0
    ;;
  consuming_threshold)
    # As threshold, for a run that asked for a consuming segment: setup is only complete once both lines are out.
    echo "[arm4] reused"
    echo "[arm4] built consuming segment x in 0.1s: 500 rows"
    if [[ ${heap} -ge 300 ]]; then
      echo "OK segmentsProcessed=12"
      exit 0
    fi
    exit 3
    ;;
  consuming_build_oom)
    # Died while building the consuming segment: the offline fixture was ready, the consuming one never was. That is
    # the fixture's footprint, not the operator's, and must not be read as an OOM that steers the search.
    echo "[arm4] reused"
    exit 3
    ;;
  never_survives)
    echo "[arm4] reused"
    exit 3
    ;;
  hangs)
    echo "[arm4] reused"
    sleep 30
    exit 0
    ;;
  dies_before_fixture)
    echo "some unrelated startup failure"
    exit 3
    ;;
  harness_bug)
    echo "[arm4] reused"
    echo "java.lang.IllegalStateException: G1 engagement check failed"
    exit 1
    ;;
  oom_killer)
    # The kernel's OOM killer: 137, but promptly. Must classify as an OOM, not as a timeout.
    echo "[arm4] reused"
    exit 137
    ;;
  ignores_term)
    # Wedged past the deadline and deaf to TERM, so timeout's KILL backstop produces 137 late. Must classify as a
    # timeout despite sharing an exit code with the OOM killer above.
    echo "[arm4] reused"
    trap '' TERM
    sleep 30
    exit 0
    ;;
esac
STUB
chmod +x "${WORK_DIR}/bin/java"

# Runs the script over a one-cell grid under the given scenario and echoes the single data row it produced.
run_scenario() {
  local scenario="$1"
  shift
  # Through `env`, not bare assignments: the extra "$@" words are env overrides such as REPEATS_ON=0, and a shell only
  # treats NAME=value as an assignment when it is written literally, not when it comes out of an expansion. Without
  # `env` those words were run as a command, which failed silently into /dev/null -- so every refusal case below
  # reported REFUSED without the script under test ever having seen the override.
  env SCENARIO="${scenario}" \
    ARGV_LOG="${WORK_DIR}/argv.log" \
    PATH="${WORK_DIR}/bin:${PATH}" \
    CLASSES_DIR="${WORK_DIR}/classes" \
    AUDIT_LOG="${WORK_DIR}/audit.csv" \
    OVERLAPS="DISJOINT" \
    LIMITS="1000" \
    MODES="OFF" \
    REPLICATES=1 \
    REPEATS_OFF=1 \
    ATTEMPT_TIMEOUT_S=1 \
    ATTEMPT_KILL_GRACE_S=1 \
    "$@" \
    bash "${UNDER_TEST}" "${WORK_DIR}/cp.txt" 2>/dev/null | tail -1
}

check() {
  local name="$1" expected="$2" actual="$3"
  CASES=$((CASES + 1))
  if [[ "${actual}" == "${expected}" ]]; then
    echo "  ok   ${name}"
  else
    echo "  FAIL ${name}"
    echo "         expected: ${expected}"
    echo "         actual:   ${actual}"
    FAILURES=$((FAILURES + 1))
  fi
}

echo "classification and bracket assembly:"

# The bracket is half-open and both bounds were run: the stub survives at 300 and dies at 284, and those are the
# two numbers reported. Nothing between them is interpolated.
check "a real threshold converges to a bracket straddling it" \
    "DISJOINT,OFF,1000,1,BRACKET,284,300,1" \
    "$(run_scenario threshold)"

# B2: without the floor probe this printed 95, a heap size that was never tried.
check "a cell that fits in the floor reports the floor, not an unprobed midpoint" \
    "DISJOINT,OFF,1000,1,AT_OR_BELOW_LOW,,64,1" \
    "$(run_scenario always_survives)"

check "a cell that does not fit under the ceiling reports no upper bound" \
    "DISJOINT,OFF,1000,1,ABOVE_HIGH,4096,,1" \
    "$(run_scenario never_survives)"

echo "failure modes that must not steer the search:"

check "a wedged attempt is abandoned rather than read as needing more heap" \
    "DISJOINT,OFF,1000,1,ERROR_TIMEOUT,,,1" \
    "$(run_scenario hangs)"

check "dying before the fixture is ready is not an operator measurement" \
    "DISJOINT,OFF,1000,1,ERROR_FIXTURE_NOT_READY,,,1" \
    "$(run_scenario dies_before_fixture)"

check "a non-OOM harness failure abandons the cell instead of raising the floor" \
    "DISJOINT,OFF,1000,1,ERROR_PROBE_FAILED,,,1" \
    "$(run_scenario harness_bug)"

# The two 137s. Exit code alone cannot separate them, so the split is on elapsed time; getting it backwards would
# either discard real OOM evidence or record a hung JVM as proof that this arm needs more memory.
check "a prompt 137 is the OOM killer and steers the search" \
    "DISJOINT,OFF,1000,1,ABOVE_HIGH,4096,,1" \
    "$(run_scenario oom_killer)"

check "a 137 after the deadline is the kill backstop and abandons the cell" \
    "DISJOINT,OFF,1000,1,ERROR_TIMEOUT,,,1" \
    "$(run_scenario ignores_term)"

echo "pinned JVM flags:"

rm -f "${WORK_DIR}/argv.log"
run_scenario always_survives >/dev/null
measurement_launch="$(grep -m1 -- '-Xmx64m' "${WORK_DIR}/argv.log" | cut -d' ' -f1-4)"
check "heap is pinned at both ends and the collector is explicit" \
    "-Xms64m -Xmx64m -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError" \
    "${measurement_launch}"

echo "axes:"

# Defaults must be the command line the script has always run. The probe's CLI is positional, so this is the tail
# that matters: orderBy then keyCardinality, and no consumingRows at all.
rm -f "${WORK_DIR}/argv.log"
default_row="$(run_scenario always_survives)"
default_tail="$(grep -m1 -- '-Xmx64m' "${WORK_DIR}/argv.log" | awk '{print $(NF-1), $NF}')"
check "the default launch ends with the original 'TS_VAL 1' and passes nothing else" "TS_VAL 1" "${default_tail}"
check "the default CSV row has exactly the original eight columns" \
    "DISJOINT,OFF,1000,1,AT_OR_BELOW_LOW,,64,1" "${default_row}"

rm -f "${WORK_DIR}/argv.log"
axes_row="$(run_scenario consuming_threshold ORDER_BY=TS_ONLY KEY_CARDINALITY=100 CONSUMING_ROWS=500)"
axes_tail="$(grep -m1 -- '-Xmx64m' "${WORK_DIR}/argv.log" | awk '{print $(NF-2), $(NF-1), $NF}')"
check "non-default axes reach the probe in positional order" "TS_ONLY 100 500" "${axes_tail}"
check "non-default axes are recorded on every CSV row" \
    "DISJOINT,OFF,1000,1,BRACKET,284,300,1,TS_ONLY,100,500,TAIL" "${axes_row}"

rm -f "${WORK_DIR}/argv.log"
shape_row="$(run_scenario consuming_threshold OVERLAPS=FULL CONSUMING_ROWS=500 CONSUMING_SHAPE=IN_DOMAIN)"
shape_tail="$(grep -m1 -- '-Xmx64m' "${WORK_DIR}/argv.log" | awk '{print $(NF-1), $NF}')"
check "an in-domain consuming shape follows consumingRows on the probe command line" "500 IN_DOMAIN" "${shape_tail}"
check "the consuming shape is recorded on every CSV row" \
    "FULL,OFF,1000,1,BRACKET,284,300,1,TS_VAL,1,500,IN_DOMAIN" "${shape_row}"

rm -f "${WORK_DIR}/argv.log"
run_scenario consuming_threshold CONSUMING_ROWS=500 >/dev/null
tail_tail="$(grep -m1 -- '-Xmx64m' "${WORK_DIR}/argv.log" | awk '{print $(NF-1), $NF}')"
check "the default TAIL shape is not passed to the probe at all" "1 500" "${tail_tail}"

# Not an OOM: the consuming segment never finished building, so the operator never ran.
check "dying while building the consuming segment is not an operator measurement" \
    "DISJOINT,OFF,1000,1,ERROR_FIXTURE_NOT_READY,,,1,TS_VAL,1,500,TAIL" \
    "$(run_scenario consuming_build_oom CONSUMING_ROWS=500)"

echo "audit trail:"

check "the per-attempt log header is the documented schema" \
    "replicate,attempt_utc,overlap,mode,limit,heap_mb,repeat_index,outcome,elapsed_s,detail" \
    "$(head -1 "${WORK_DIR}/audit.csv")"

run_scenario threshold >/dev/null
check "every attempt of the bisection is recorded, OOMs included" \
    "OOM" \
    "$(awk -F, '$6 == 64 {print $8}' "${WORK_DIR}/audit.csv" | head -1)"

echo "refusals:"

# A knob someone deliberately set must never be silently dropped: a zero repeat count would make every heap size
# report as surviving, and a stale TOLERANCE_MB would run the sweep at a tolerance nobody chose.
refusal() {
  local output
  output="$(run_scenario always_survives "$@" 2>&1)"
  if [[ -n "${output}" ]]; then echo "ACCEPTED"; else echo "REFUSED"; fi
}

check "a zero repeat count is refused" "REFUSED" "$(refusal REPEATS_ON=0)"
check "the removed TOLERANCE_MB knob is refused, not ignored" "REFUSED" "$(refusal TOLERANCE_MB=32)"
check "a floor at or above the ceiling is refused" "REFUSED" "$(refusal LOW_MB=4096)"
check "an unknown ORDER_BY is refused" "REFUSED" "$(refusal ORDER_BY=BOGUS)"
check "a negative CONSUMING_ROWS is refused" "REFUSED" "$(refusal CONSUMING_ROWS=-1)"
check "a LIMIT past INT_MAX is refused" "REFUSED" "$(refusal LIMITS=2147483648)"
check "IN_DOMAIN without a consuming segment is refused" "REFUSED" \
    "$(refusal OVERLAPS=FULL CONSUMING_SHAPE=IN_DOMAIN)"
check "IN_DOMAIN over an overlap other than FULL is refused" "REFUSED" \
    "$(refusal CONSUMING_ROWS=500 CONSUMING_SHAPE=IN_DOMAIN)"
check "an unknown CONSUMING_SHAPE is refused" "REFUSED" "$(refusal CONSUMING_ROWS=500 CONSUMING_SHAPE=BOGUS)"
# Without this the refusals above could pass for the wrong reason (the script never starting at all).
check "the widest valid LIMIT is accepted" "ACCEPTED" "$(refusal LIMITS=2147483647)"

echo
if [[ ${FAILURES} -eq 0 ]]; then
  echo "${CASES} cases, all passed"
  exit 0
fi
echo "${CASES} cases, ${FAILURES} failed"
exit 1
