#!/bin/sh
# Release Forge to production: tag what beta is running, and let CI deploy that tag.
#
#   work → push to beta → CI tests, builds, deploys anvil-forge-beta
#        → watch it on beta
#        → scripts/release.sh → tags beta's commit vX.Y.Z, fast-forwards main to it
#        → CI builds :X.Y.Z / :stable and deploys anvil-forge (production), health-gated
#
# Only a tag deploys production; a push to main deploys nothing. The version is typed at the
# terminal, so a release can't happen from a script or by accident.
#
#   scripts/release.sh            # release whatever beta is at
#   scripts/release.sh --dry-run  # say what it would do
set -eu

DRY=""
[ "${1:-}" = "--dry-run" ] && DRY=1

REMOTE="${FORGE_REMOTE:-origin}"
git fetch -q --tags "$REMOTE" main beta

BETA="$(git rev-parse "$REMOTE/beta")"
MAIN="$(git rev-parse "$REMOTE/main")"
LAST="$(git tag --list 'v*' --sort=-v:refname | head -1)"

if [ -n "$LAST" ] && [ "$(git rev-parse "$LAST^{commit}")" = "$BETA" ]; then
  echo "Nothing to release: $LAST is already beta's commit $(git rev-parse --short "$BETA")."
  exit 0
fi

# main only ever follows releases, so it must be behind beta, never beside it.
if ! git merge-base --is-ancestor "$MAIN" "$BETA"; then
  echo "Refusing: main has $(git rev-list --count "$BETA..$MAIN") commit(s) beta lacks, so this would not be a fast-forward." >&2
  echo "  Merge main into beta, push beta, let it deploy, then release." >&2
  exit 1
fi

SINCE="${LAST:-$MAIN}"
echo "Releasing $(git rev-parse --short "$BETA") (last release: ${LAST:-none})"
echo
git --no-pager log --no-merges --format='  - %s' "$SINCE..$BETA"
echo

# Beta's CI run for this commit must have passed: production gets the bytes beta was deployed with.
if command -v gh >/dev/null 2>&1; then
  RUN="$(gh run list --branch beta --commit "$BETA" -L 1 --json conclusion -q '.[0].conclusion' 2>/dev/null || true)"
  if [ "$RUN" != "success" ]; then
    echo "Refusing: beta's CI run for this commit is '${RUN:-missing}', not success." >&2
    exit 1
  fi
  echo "beta's CI run for this commit passed."
else
  echo "! gh not installed: can't confirm beta's CI run passed."
fi
echo

[ -n "$DRY" ] && { echo "(dry run: nothing tagged)"; exit 0; }

if [ ! -r /dev/tty ]; then
  echo "Refusing: a release is confirmed at a terminal." >&2
  exit 1
fi
printf 'Version to release (e.g. 1.2.0, no v): ' > /dev/tty
read -r VERSION < /dev/tty
case "$VERSION" in
  ''|*[!0-9.]*|.*|*.|*..*) echo "Not a version: '$VERSION'." >&2; exit 1 ;;
esac
TAG="v$VERSION"
if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
  echo "Refusing: $TAG already exists." >&2
  exit 1
fi
printf 'Type %s again to release it to production: ' "$VERSION" > /dev/tty
read -r AGAIN < /dev/tty
[ "$AGAIN" = "$VERSION" ] || { echo "Not released."; exit 1; }

git tag -a "$TAG" -m "Forge $VERSION" "$BETA"
git push "$REMOTE" "$TAG"
git push "$REMOTE" "$BETA:refs/heads/main"
echo
echo "Tagged $TAG. CI is deploying it to production: gh run list --branch $TAG"
