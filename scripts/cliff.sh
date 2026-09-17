#!/usr/bin/env bash
set -euo pipefail

# Each entry is "<crate>:<comma-separated include paths>". A crate bumps when a commit
# touched one of its paths. Adding an artifact is one line.
ARTIFACTS=(
  "ldrs:crates/**"
  "ldrs-delta:crates/ldrs-delta/**,crates/ldrs-arrow/**,crates/ldrs-parquet/**,crates/ldrs-storage/**"
)

for entry in "${ARTIFACTS[@]}"; do
  ARTIFACT="${entry%%:*}"
  PATHS="${entry#*:}"
  CHANGELOG="crates/$ARTIFACT/CHANGELOG.md"
  TAG_PATTERN="$ARTIFACT-v[0-9].*"

  INCLUDE_ARGS=()
  IFS=',' read -ra paths <<< "$PATHS"
  for p in "${paths[@]}"; do
    INCLUDE_ARGS+=(--include-path "$p")
  done

  CURRENT=$(cargo pkgid -p "$ARTIFACT" | sed 's/.*[#@]//')

  # git-cliff refuses to compute a bump with no tag matching the pattern
  if ! NEXT=$(git-cliff --tag-pattern "$TAG_PATTERN" "${INCLUDE_ARGS[@]}" \
                        --unreleased --bumped-version 2>/dev/null); then
    echo "$ARTIFACT: no tag matching $TAG_PATTERN; seed one at $ARTIFACT-v$CURRENT first"
    continue
  fi

  # git-cliff returns the full tag name (<crate>-vX.Y.Z); cargo needs the bare semver
  VERSION="${NEXT#"$ARTIFACT-"}"
  VERSION="${VERSION#v}"

  if [ "$VERSION" = "$CURRENT" ]; then
    echo "$ARTIFACT: nothing unreleased, staying at $CURRENT"
    continue
  fi

  echo "$ARTIFACT: $CURRENT -> $VERSION ($NEXT)"

  # --prepend needs the file to exist; a new artifact has no changelog yet
  [ -f "$CHANGELOG" ] || : > "$CHANGELOG"

  git-cliff --tag-pattern "$TAG_PATTERN" \
            "${INCLUDE_ARGS[@]}" \
            --bump \
            --unreleased \
            --prepend "$CHANGELOG"

  cargo set-version -p "$ARTIFACT" "$VERSION"
  echo "Bumped $ARTIFACT to $VERSION and prepended $CHANGELOG"
done

echo "Review the diff, then commit and run scripts/tag-release.sh"
