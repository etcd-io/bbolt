#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

# === Function Definitions ===
function get_gpg_key {
  local git_email
  local key_id

  git_email=$(git config --get user.email)
  key_id=$(gpg --list-keys --with-colons "${git_email}" | awk -F: '/^pub:/ { print $5 }')
  if [[ -z "${key_id}" ]]; then
    echo "Failed to load gpg key. Is gpg set up correctly for etcd releases?"
    return 2
  fi
  echo "${key_id}"
}

# === Main Script Logic ===
function main {
  VERSION="$1"

  if [ -z "${VERSION}" ]; then
    read -p "Release version (e.g., v1.2.3) " -r VERSION
    if [[ ! "${VERSION}" =~ ^v[0-9]+.[0-9]+.[0-9]+ ]]; then
      echo "Expected 'version' param of the form 'v<major-version>.<minor-version>.<patch-version>' but got '${VERSION}'"
      exit 1
    fi
  fi

  VERSION=v${VERSION#v}
  RELEASE_VERSION="${VERSION#v}"
  MINOR_VERSION=$(echo "${RELEASE_VERSION}" | cut -d. -f 1-2)
  RELEASE_BRANCH="release-${MINOR_VERSION}"

  REPOSITORY=${REPOSITORY:-"git@github.com:etcd-io/bbolt.git"}

  local remote_tag_exists
  remote_tag_exists=$(git ls-remote "${REPOSITORY}" "refs/tags/${VERSION}" | grep -c "${VERSION}" || true)
  if [ "${remote_tag_exists}" -gt 0 ]; then
    echo "Release version tag exists on remote."
    exit 1
  fi

  # Set up release directory.
  local reldir="/tmp/bbolt-release-${VERSION}"
  echo "Preparing temporary directory: ${reldir}"
  if [ ! -d "${reldir}/bbolt" ]; then
    mkdir -p "${reldir}"
    cd "${reldir}"
    git clone "${REPOSITORY}" --branch "${RELEASE_BRANCH}" --depth 1
  fi
  cd "${reldir}/bbolt" || exit 2
  git checkout "${RELEASE_BRANCH}" || exit 2
  git fetch origin
  git reset --hard "origin/${RELEASE_BRANCH}"

  # ensuring the minor-version is identical.
  source_version=$(grep -E "\s+Version\s*=" ./version/version.go | sed -e "s/.*\"\(.*\)\".*/\1/g")
  if [[ "${source_version}" != "${RELEASE_VERSION}" ]]; then
     source_minor_version=$(echo "${source_version}" | cut -d. -f 1-2)
     if [[ "${source_minor_version}" != "${MINOR_VERSION}" ]]; then
       echo "Wrong bbolt minor version in version.go. Expected ${MINOR_VERSION} but got ${source_minor_version}. Aborting."
       exit 1
     fi
  fi

  # bump 'version.go'.
  echo "Updating version from '${source_version}' to '${RELEASE_VERSION}' in 'version.go'"
  if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i '' "s/${source_version}/${RELEASE_VERSION}/g" ./version/version.go
  else
    sed -i "s/${source_version}/${RELEASE_VERSION}/g" ./version/version.go
  fi

  # push 'version.go' to remote.
  echo "committing 'version.go'"
  git add ./version/version.go
  git commit -s -m "Update version to ${VERSION}"
  git push origin "${RELEASE_BRANCH}"
  echo "'version.go' has been committed to remote repo."

  # create tag and push to remote.
  echo "Creating new tag for '${VERSION}'"
  key_id=$(get_gpg_key) || return 2
  git tag --local-user "${key_id}" --sign "${VERSION}" --message "${VERSION}"
  git push origin "${VERSION}"
  echo "Tag '${VERSION}' has been created and pushed to remote repo."
  echo "SUCCESS"
}

main "$1"
