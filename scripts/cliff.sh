#!/usr/bin/env bash
set -euo pipefail

INCLUDE='crates/**'
CHANGELOG='crates/ldrs/CHANGELOG.md'
TAG_PATTERN='ldrs-v[0-9].*'

NEXT=$(git-cliff --tag-pattern "$TAG_PATTERN" --include-path "$INCLUDE" --unreleased --bumped-version)
# git-cliff returns the full tag name (ldrs-vX.Y.Z); cargo needs the bare semver
VERSION="${NEXT#ldrs-}"
VERSION="${VERSION#v}"
echo "Next version: $NEXT ($VERSION)"

git-cliff --tag-pattern "$TAG_PATTERN" \
          --include-path "$INCLUDE" \
          --bump \
          --unreleased \
          --prepend "$CHANGELOG"

cargo set-version -p ldrs "$VERSION"

echo "Bumped ldrs to $VERSION and prepended $CHANGELOG"
echo "Review the diff, then commit and tag $NEXT"
