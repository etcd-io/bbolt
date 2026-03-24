#!/usr/bin/env bash

# The version present in the .go-version is the default version that test and build scripts will use.
# However, it is possible to control the version that should be used with the help of env vars:
# - FORCE_HOST_GO: if set to a non-empty value, use the version of go installed in system's $PATH.
# - GO_VERSION: desired version of go to be used, might differ from what is present in .go-version.
#               If empty, the value defaults to the version in .go-version.
function determine_go_version {
  # Borrowing from how Kubernetes does this:
  #  https://github.com/kubernetes/kubernetes/blob/17854f0e0a153b06f9d0db096e2cd8ab2fa89c11/hack/lib/golang.sh#L510-L520

  # Get the directory where this script is located
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  ETCD_ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

  # default GO_VERSION to content of .go-version
  GO_VERSION="${GO_VERSION:-"$(cat "${ETCD_ROOT_DIR}/.go-version")"}"
  if [ "${GOTOOLCHAIN:-auto}" != 'auto' ]; then
    # no-op, just respect GOTOOLCHAIN
    :
  elif [ -n "${FORCE_HOST_GO:-}" ]; then
    export GOTOOLCHAIN='local'
  else
    GOTOOLCHAIN="go${GO_VERSION}"
    export GOTOOLCHAIN
  fi
}

determine_go_version
