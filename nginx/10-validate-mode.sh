#!/bin/sh
set -eu
case "${FRONTEND_MODE:-production}" in
  development|production) ;;
  *) echo 'FRONTEND_MODE must be development or production' >&2; exit 1 ;;
esac
