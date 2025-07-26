#!/usr/bin/env bash
# https://github.com/kubernetes/kube-state-metrics/blob/main/tests/compare_benchmarks.sh (originally written by mxinden)

# exit immediately when a command fails
set -e
# only exit with zero if all commands of the pipeline exit successfully
set -o pipefail
# error on unset variables
set -u

[[ "$#" -eq 1 ]] || echo "One argument required, $# provided."

REF_CURRENT="$(git rev-parse --abbrev-ref HEAD)"
BASE_TO_COMPARE=$1

RESULT_CURRENT="$(mktemp)-${REF_CURRENT}"
RESULT_TO_COMPARE="$(mktemp)-${BASE_TO_COMPARE}"

BENCH_COUNT=${BENCH_COUNT:-10}
BENCHSTAT_CONFIDENCE_LEVEL=${BENCHSTAT_CONFIDENCE_LEVEL:-0.9}
BENCHSTAT_FORMAT=${BENCHSTAT_FORMAT:-"text"}
BENCH_PARAMETERS=${BENCH_PARAMETERS:-"--count 2000000 --batch-size 10000"}

if [[ "${BENCHSTAT_FORMAT}" == "csv" ]] && [[ -z "${BENCHSTAT_OUTPUT_FILE}" ]]; then
  echo "BENCHSTAT_FORMAT is set to csv, but BENCHSTAT_OUTPUT_FILE is not set."
  exit 1
fi

function bench() {
  local output_file
  output_file="$1"
  make build

  for _ in $(seq "$BENCH_COUNT"); do
    echo ./bin/bbolt bench --gobench-output --profile-mode n ${BENCH_PARAMETERS}
    # shellcheck disable=SC2086
    ./bin/bbolt bench --gobench-output --profile-mode n ${BENCH_PARAMETERS} >> "${output_file}"
  done
}

function main() {
  echo "### Benchmarking PR ${REF_CURRENT}"
  bench "${RESULT_CURRENT}"
  echo ""
  echo "### Done benchmarking ${REF_CURRENT}"

  echo "### Benchmarking base ${BASE_TO_COMPARE}"
  git checkout "${BASE_TO_COMPARE}"
  bench "${RESULT_TO_COMPARE}"
  echo ""
  echo "### Done benchmarking ${BASE_TO_COMPARE}"

  git checkout -

  echo ""
  echo "### Result"
  echo "BASE=${BASE_TO_COMPARE} HEAD=${REF_CURRENT}"

  if [[ "${BENCHSTAT_FORMAT}" == "csv" ]]; then
    benchstat -format=csv -confidence="${BENCHSTAT_CONFIDENCE_LEVEL}" BASE="${RESULT_TO_COMPARE}" HEAD="${RESULT_CURRENT}" 2>/dev/null 1>"${BENCHSTAT_OUTPUT_FILE}"
  else
    if [[ -z "${BENCHSTAT_OUTPUT_FILE}" ]]; then
      benchstat -confidence="${BENCHSTAT_CONFIDENCE_LEVEL}" BASE="${RESULT_TO_COMPARE}" HEAD="${RESULT_CURRENT}"
    else
      benchstat -confidence="${BENCHSTAT_CONFIDENCE_LEVEL}" BASE="${RESULT_TO_COMPARE}" HEAD="${RESULT_CURRENT}" 1>"${BENCHSTAT_OUTPUT_FILE}"
    fi
  fi
}

main
