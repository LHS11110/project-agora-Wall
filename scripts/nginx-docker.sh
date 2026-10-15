#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"
command_name="${1:-development}"
compose() { docker compose --project-directory "$PROJECT_DIR" --env-file "$PROJECT_DIR/.env" -f "$PROJECT_DIR/docker-compose.nginx.yml" "$@"; }
case "$command_name" in
  development|production)
    export FRONTEND_MODE="$command_name"
    python3 "$PROJECT_DIR/scripts/preflight.py"
    docker info >/dev/null 2>&1 || { echo 'Docker engine is unavailable or access is denied' >&2; exit 1; }
    compose config --quiet
    if [ "$command_name" = production ]; then
      docker run --rm --network none --mount type=volume,src=agora-frontend-dist,dst=/frontend,readonly nginx:stable-alpine sh -c 'test -f /frontend/current/index.html' \
        || { echo 'Frontend build is missing. Export the FE build first.' >&2; exit 1; }
    fi
    compose up -d --wait --wait-timeout 60
    compose exec -T nginx nginx -t
    if [ "$command_name" = production ]; then
      compose exec -T nginx test -f /usr/share/nginx/html/current/index.html \
        || { echo 'Frontend production build is missing. Run the FE frontend-build exporter on this Docker engine.' >&2; exit 1; }
    fi
    ;;
  config) compose config --quiet ;;
  test) compose exec -T nginx nginx -t ;;
  status) compose ps ;;
  stop) compose down ;;
  logs) compose logs --tail=100 --follow ;;
  *) echo 'Usage: nginx-docker.sh development|production|config|test|status|stop|logs' >&2; exit 2 ;;
esac
