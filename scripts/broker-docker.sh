#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"
compose() { docker compose --env-file "$PROJECT_DIR/.env" -f "$PROJECT_DIR/docker-compose.broker.yml" "$@"; }
case "${1:-up}" in
  up) compose config --quiet; compose up -d --build --wait --wait-timeout 120 ;;
  config) compose config --quiet ;;
  status) compose ps ;;
  logs) compose logs --tail=100 --follow ;;
  stop) compose down ;;
  *) echo 'Usage: broker-docker.sh up|config|status|logs|stop' >&2; exit 2 ;;
esac
