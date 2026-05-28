#!/usr/bin/env bash
set -euo pipefail

CHANGELOG='crates/ldrs/CHANGELOG.md'

VERSION=$(cargo pkgid -p ldrs | sed 's/.*[#@]//')
TAG="v$VERSION"

if git rev-parse "$TAG" >/dev/null 2>&1; then
  echo "Tag $TAG already exists at $(git rev-parse --short "$TAG")"
  exit 0
fi

NOTES=$(awk "/^## \\[$VERSION\\]/{flag=1; next} /^## \\[/{flag=0} flag" "$CHANGELOG")

if [ -z "$NOTES" ]; then
  echo "Warning: no CHANGELOG section found for $VERSION"
  git tag -a "$TAG" -m "Release $TAG"
else
  git tag -a "$TAG" -m "Release $TAG" -m "$NOTES"
fi

echo "Created tag $TAG at $(git rev-parse --short HEAD)"
echo "Push with: git push origin $TAG"
