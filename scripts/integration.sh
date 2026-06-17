#!/usr/bin/env sh
set -eu

docker compose up -d kurrentdb

attempt=0
until curl --fail --silent --show-error http://localhost:2113/health/live >/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    docker compose logs kurrentdb
    exit 1
  fi
  sleep 1
done

KURRENTDB_INTEGRATION=1 \
  KURRENTDB_CONNECTION_STRING="${KURRENTDB_CONNECTION_STRING:-kurrentdb://localhost:2113?tls=false}" \
  gleam test

if [ "${KEEP_KURRENTDB:-0}" != "1" ]; then
  docker compose down
fi
