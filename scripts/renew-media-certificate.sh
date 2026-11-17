#!/usr/bin/env sh
set -eu
project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
# Compose remounts renewed certificate inodes; restart alone can retain old binds.
docker compose --env-file "$project_dir/.env" -f "$project_dir/docker-compose.media.yml" up -d --force-recreate --no-deps --wait --wait-timeout 60 turn
