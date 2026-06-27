#!/usr/bin/env bash
set -euo pipefail

INCLUDE='crates/**'
CHANGELOG='crates/ldrs/CHANGELOG.md'

NEXT=$(git-cliff --include-path "$INCLUDE" --unreleased --bumped-version)
echo "Next version: $NEXT"

git-cliff --include-path "$INCLUDE" \
          --bump \
          --unreleased \
          --prepend "$CHANGELOG"

cargo set-version -p ldrs "${NEXT#v}"

echo "Bumped ldrs to ${NEXT#v} and prepended $CHANGELOG"
echo "Review the diff, then commit and tag $NEXT"
