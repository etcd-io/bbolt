#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

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
   echo "Release version tag exists on remote. Checking out refs/tags/${INPUT}"
   git checkout -q "tags/${INPUT}"
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

# creating a branch to bump 'version.go'.
date_string=$(date +%Y%m%d)
local_branch_name="version_${date_string}"
local_branch_err=$(git checkout -b "${local_branch_name}" | grep -E "error|fatal" || true )
if [[ -n "${local_branch_err}" ]]; then
  echo "${local_branch_err}"
fi

# bump 'version.go'.
echo "Updating version from '${source_version}' to '${RELEASE_VERSION}' in 'version.go'"
sed -i "s/${source_version}/${RELEASE_VERSION}/g" ./version/version.go

# push 'version.go' to remote.
echo "committing 'version.go'"
git add ./version/version.go
git commit -s -m "Update version to ${VERSION}"
echo "'version.go' has been committed to remote repo."

# create tag and push to remote.
echo "Creating new tag for '${INPUT}'"
git tag -f "${INPUT}"
git push -f "${REMOTE}" "${INPUT}"

echo "Tag '${INPUT}' has been created and pushed to remote repo."
echo "SUCCESS"
