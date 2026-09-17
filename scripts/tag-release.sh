#!/usr/bin/env bash
set -euo pipefail

# Tags every artifact whose current version has no tag yet. Keep in step with
# the ARTIFACTS list in cliff.sh.
ARTIFACTS=(ldrs ldrs-delta)

for ARTIFACT in "${ARTIFACTS[@]}"; do
  CHANGELOG="crates/$ARTIFACT/CHANGELOG.md"
  VERSION=$(cargo pkgid -p "$ARTIFACT" | sed 's/.*[#@]//')
  TAG="$ARTIFACT-v$VERSION"

  if git rev-parse "$TAG" >/dev/null 2>&1; then
    echo "$ARTIFACT: tag $TAG already exists at $(git rev-parse --short "$TAG")"
    continue
  fi

  NOTES=''
  if [ -f "$CHANGELOG" ]; then
    NOTES=$(awk "/^## \\[$VERSION\\]/{flag=1; next} /^## \\[/{flag=0} flag" "$CHANGELOG")
  fi

  if [ -z "$NOTES" ]; then
    echo "Warning: no $CHANGELOG section found for $VERSION"
    git tag -a "$TAG" -m "Release $TAG"
  else
    git tag -a "$TAG" -m "Release $TAG" -m "$NOTES"
  fi

  echo "Created tag $TAG at $(git rev-parse --short HEAD)"
  echo "Push with: git push origin $TAG"
done
