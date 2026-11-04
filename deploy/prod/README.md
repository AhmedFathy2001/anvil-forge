# Production Forge on the box

Production Forge was started by hand with `docker run`. Release deploys (a `vX.Y.Z` tag, see
`scripts/release.sh`) recreate it through `/opt/anvil/forge/deploy.sh`, so it moves to compose once:

1. `mkdir -p /opt/anvil/forge` and copy `deploy.sh` and `docker-compose.yml` from here into it.
2. Write `/opt/anvil/forge/forge.env` with the running container's settings
   (`docker inspect anvil-forge --format '{{range .Config.Env}}{{println .}}{{end}}'`, minus PATH),
   and check its networks and any volumes or ports (`docker inspect anvil-forge`) match the compose file.
3. Cut over: `docker rm -f anvil-forge && /opt/anvil/forge/deploy.sh <the image it was running>`.

From then on every release tag deploys it, health-gated on the tagged commit.
