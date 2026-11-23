#!/usr/bin/env bash
# End-to-end tests for xxljob: download the xxl-job schema SQL (SHA-256 verified) -> start MySQL + xxl-job-admin ->
# run the e2e tests -> remove the containers.
#
#   make e2e                         # default version 2.4.1 (or ./e2e/run.sh)
#   XXL_JOB_VERSION=3.5.0 make e2e   # pick a version: 2.4.1 / 3.3.1 / 3.4.2 / 3.5.0
#   make e2e-all                     # run every version in turn
#   KEEP=1 make e2e                  # keep the containers afterwards to inspect them in the admin UI (admin / 123456)
#   XXLJOB_E2E_VERBOSE=1 make e2e    # print every executor log line (warnings and errors are always printed)
#
# Requires docker (compose v2) and curl. Ports: admin 18080 (XXLJOB_E2E_ADMIN_PORT), executor 9999 (XXLJOB_E2E_PORT).
set -euo pipefail

cd "$(dirname "$0")"

export XXL_JOB_VERSION="${XXL_JOB_VERSION:-2.4.1}"

# Per-version differences: schema SQL checksum, the admin's context path (root since 3.4.1), the admin's arguments and
# TOKEN, the executor's AccessToken. Before 3.5 the token is the admin's global xxl.job.accessToken (the image defaults
# it to default_token, so it is always set explicitly); since 3.5 it is stored on the executor record. 3.3.1 runs with
# no token at all: it is the version that rejects requests lacking the token header.
case "$XXL_JOB_VERSION" in
  2.4.1) SQL_SHA256=f495e8a258111af4f79befad3c70256d656952cf56e64bd5db69494bf1fdcf36 CONTEXT_PATH=/xxl-job-admin TOKEN=e2e-token ADMIN_PARAMS=--xxl.job.accessToken=e2e-token ;;
  3.3.1) SQL_SHA256=aa08c3fd66e851543378e96f1ca925d71a728af6d5dc5deca44375cca594ba7d CONTEXT_PATH=/xxl-job-admin TOKEN= ADMIN_PARAMS=--xxl.job.accessToken= ;;
  3.4.2) SQL_SHA256=6fb3b764bd63a66e63f23c4cb6254a5cb277fa0db23bec14586935699fb60483 CONTEXT_PATH= TOKEN=e2e-token ADMIN_PARAMS=--xxl.job.accessToken=e2e-token ;;
  3.5.0) SQL_SHA256=0ab12dbda74de586dd71a044f52136ae17ebfcde82ee8e5693a689a34f56b600 CONTEXT_PATH= TOKEN=e2e-token ADMIN_PARAMS= ;;
  *) echo "unsupported XXL_JOB_VERSION=$XXL_JOB_VERSION (supported: 2.4.1, 3.3.1, 3.4.2, 3.5.0)" >&2; exit 1 ;;
esac
export XXLJOB_E2E_TOKEN="$TOKEN"

SQL_URL="https://raw.githubusercontent.com/xuxueli/xxl-job/${XXL_JOB_VERSION}/doc/db/tables_xxl_job.sql"
SQL_FILE=".cache/tables_xxl_job-${XXL_JOB_VERSION}.sql"

ADMIN_PORT="${XXLJOB_E2E_ADMIN_PORT:-18080}"
export XXLJOB_E2E_ADMIN_PORT="$ADMIN_PORT"
export XXLJOB_E2E_ADMIN="http://127.0.0.1:${ADMIN_PORT}${CONTEXT_PATH}"
export XXLJOB_E2E_VERSION="$XXL_JOB_VERSION"
export XXLJOB_E2E_ADMIN_PARAMS="$ADMIN_PARAMS"

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1"; else shasum -a 256 "$1"; fi
}

mkdir -p .cache
if [[ ! -f "$SQL_FILE" ]]; then
  echo ">> downloading xxl-job ${XXL_JOB_VERSION} schema"
  curl -fsSL -o "$SQL_FILE" "$SQL_URL"
fi
if [[ "$(sha256 "$SQL_FILE" | cut -d' ' -f1)" != "$SQL_SHA256" ]]; then
  echo "checksum mismatch for $SQL_FILE" >&2
  rm -f "$SQL_FILE"
  exit 1
fi

cleanup() {
  if [[ "${KEEP:-}" == "1" ]]; then
    echo ">> containers kept; stop with: XXL_JOB_VERSION=${XXL_JOB_VERSION} docker compose -f $(pwd)/docker-compose.yml down -v"
  else
    docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo ">> starting mysql + xxl-job-admin ${XXL_JOB_VERSION}"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true # remove containers and data left by a previous run (possibly another version)
docker compose up -d
echo ">> waiting for xxl-job-admin"
# any HTTP response means it is up (the home page redirects to the login page when not signed in)
if ! curl -s -o /dev/null --retry 60 --retry-delay 2 --retry-all-errors --max-time 5 "${XXLJOB_E2E_ADMIN}/" 2>/dev/null; then
  echo "xxl-job-admin did not become ready; logs:" >&2
  docker compose logs --tail 50 admin >&2
  exit 1
fi

echo ">> running e2e tests against xxl-job-admin ${XXL_JOB_VERSION}"
go test -tags e2e -count=1 -v .
