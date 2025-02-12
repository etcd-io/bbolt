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
echo "enter release string according to semantic versioning (e.g. v1.2.3)."
read -r INPUT
if [[ ! "${INPUT}" =~ ^v[0-9]+.[0-9]+.[0-9]+ ]]; then
  echo "Expected 'version' param of the form 'v<major-version>.<minor-version>.<patch-version>' but got '${INPUT}'"
  exit 1
fi

VERSION=${INPUT#v}
RELEASE_VERSION="${VERSION}"
MINOR_VERSION=$(echo "${VERSION}" | cut -d. -f 1-2)

REPOSITORY=${REPOSITORY:-"git@github.com:etcd-io/bbolt.git"}
REMOTE="${REMOTE:-"origin"}"

remote_tag_exists=$(git ls-remote --tags "${REPOSITORY}" | grep -c "${INPUT}" || true)
if [ "${remote_tag_exists}" -gt 0 ]; then
   echo "Release version tag exists on remote."
   exit 1
fi

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
sed -i "s/${source_version}/${RELEASE_VERSION}/g" ./version/version.go

# push 'version.go' to remote.
echo "committing 'version.go'"
git add ./version/version.go
git commit -s -m "Update version to ${VERSION}"
git push "${REMOTE}" "${INPUT}"
echo "'version.go' has been committed to remote repo."

# create tag and push to remote.
echo "Creating new tag for '${INPUT}'"
key_id=$(get_gpg_key) || return 2
git tag --local-user "${key_id}" --sign "${INPUT}" --message "${INPUT}"
git push "${REMOTE}" "${INPUT}"
echo "Tag '${INPUT}' has been created and pushed to remote repo."
echo "SUCCESS"
