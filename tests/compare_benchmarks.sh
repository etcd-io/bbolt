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
REF_TO_COMPARE=$1

RESULT_CURRENT="$(mktemp)-${REF_CURRENT}"
RESULT_TO_COMPARE="$(mktemp)-${REF_TO_COMPARE}"

echo ""
echo "### Testing ${REF_CURRENT}"

go test -benchmem -run=NONE -bench=. ./... | tee "${RESULT_CURRENT}"

echo ""
echo "### Done testing ${REF_CURRENT}"

echo ""
echo "### Testing ${REF_TO_COMPARE}"

git checkout "${REF_TO_COMPARE}"

go test -benchmem -run=NONE -bench=. ./... | tee "${RESULT_TO_COMPARE}"

echo ""
echo "### Done testing ${REF_TO_COMPARE}"

git checkout -

echo ""
echo "### Result"
echo "old=${REF_TO_COMPARE} new=${REF_CURRENT}"

benchstat "${RESULT_TO_COMPARE}" "${RESULT_CURRENT}"
