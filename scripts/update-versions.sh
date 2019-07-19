#!/usr/bin/env bash
# This script is pretty low tech, but it helps keep the doc version numbers
# up to date. It should be run as a `pre-release-hook` from cargo-release.

set -eo pipefail

if [ -z "$PREV_VERSION" ] || [ -z "$NEW_VERSION" ]; then
    echo "Missing PREV_VERSION or NEW_VERSION."
    echo "This script needs to run as a 'pre-release-hook' from cargo-release."
    exit 1
fi

for file in README.md; do
    sed -i.bak -E \
        -e "s|version=[0-9.]+|version=${NEW_VERSION}|g" \
        -e "s|redis/[0-9.]+|redis/${NEW_VERSION}|g" \
        -e "s|redis = \"[0-9.]+\"|redis = \"${NEW_VERSION}\"|g" \
        "${CRATE_ROOT}/$file"
    rm "${CRATE_ROOT}/$file.bak"
done
