#!/bin/sh
set -e

case "${PROCESS}" in
  api)
    uv run --no-dev alembic upgrade head
    exec uv run --no-dev uvicorn app.main:app --host 0.0.0.0 --port 8000
    ;;
  consumer)
    exec uv run --no-dev python -m app.worker
    ;;
  *)
    echo "Unknown PROCESS=${PROCESS}" >&2
    exit 1
    ;;
esac
