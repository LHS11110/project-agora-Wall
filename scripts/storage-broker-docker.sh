#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose() { docker compose --project-directory "$PROJECT_DIR" --env-file "$PROJECT_DIR/.env" -f "$PROJECT_DIR/docker-compose.storage-broker.yml" "$@"; }
case "${1:-up}" in
  up) python3 "$PROJECT_DIR/scripts/setup.py" --sync-storage-only --backend-dir "${AGORA_BE_DIR:-$PROJECT_DIR/../project-agora-BE}"; compose config --quiet; compose up -d --wait --wait-timeout 60 ;;
  config) compose config --quiet ;;
  test) compose exec -T storage-broker haproxy -c -f /usr/local/etc/haproxy/haproxy.cfg ;;
  status) compose ps ;;
  logs) compose logs --tail=100 --follow ;;
  stop) compose down ;;
  *) echo 'Usage: storage-broker-docker.sh up|config|test|status|logs|stop' >&2; exit 2 ;;
esac
