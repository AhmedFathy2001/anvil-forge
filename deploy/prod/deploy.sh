#!/bin/sh
# /opt/anvil/forge/deploy.sh — production Forge's deploy, run by CI on a release tag.
#
#   /opt/anvil/forge/deploy.sh ghcr.io/ahmedfathy2001/anvil-forge:sha-<full>
#
# Pins the image in this directory's .env (FORGE_IMAGE) and recreates anvil-forge through
# docker-compose.yml beside it. The run settings (database URL, tokens, network) live in forge.env
# here on the box and never in the repo. A rollback is editing FORGE_IMAGE and running `up -d` again.
set -eu
cd "$(dirname "$0")"
IMAGE="${1:?usage: deploy.sh <image>}"
case "$IMAGE" in ghcr.io/ahmedfathy2001/anvil-forge:*) ;; *) echo "refusing image $IMAGE" >&2; exit 1 ;; esac
touch .env
sed -i "s|^FORGE_IMAGE=.*|FORGE_IMAGE=$IMAGE|" .env
grep -q "^FORGE_IMAGE=" .env || echo "FORGE_IMAGE=$IMAGE" >> .env
docker compose pull anvil-forge
docker compose up -d anvil-forge
docker image prune -f --filter "until=168h" >/dev/null 2>&1 || true
