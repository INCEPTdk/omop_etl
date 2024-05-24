#!/bin/sh
set -eu
SELF="$(basename "$0" ".sh")"

makeJSON() {
  cat <<EOM > "connection.json"
{
  "dbms": "${DB_DBMS}",
  "server": "${DB_SERVER}",
  "port": "${DB_PORT}",
  "dbname": "${DB_DBNAME}",
  "user": "${DB_USER}",
  "password": "${DB_PASSWORD}"
}
EOM
}

warn() {
  printf '%s %s %s\n' "$(date '+%FT%T%z')" "$SELF" "$*" >&2
}

main() {
  makeJSON
  export PYTHONPATH="$PWD"
  export SOURCE_SCHEMA="$SOURCE_SCHEMA"
  export TARGET_SCHEMA="$TARGET_SCHEMA"

  if [ -n "${RUN_TESTS:-}" ]; then
    if [ -z "${DEV_BUILD:-}" ]; then
      warn "tests requested on non-dev container build, attempting to install dependencies"
      python3 -m pip install -r requirements.dev.txt
    fi
    warn "starting in RUN_TESTS mode as $(id) in ${PWD}"
    export ETL_RUN_INTEGRATION_TESTS="OFF"
    export ETL_TEST_TARGET_HOST="$DB_SERVER"
    export ETL_TEST_TARGET_DBNAME="$DB_DBNAME"
    export ETL_TEST_TARGET_USER="$DB_USER"
    export ETL_TEST_TARGET_PASSWORD="$DB_PASSWORD"
    export ETL_TEST_TARGET_PORT="$DB_PORT"
    exec pytest tests/testsuite.py
  fi

  warn "starting as $(id) in ${PWD}"
  exec python3 etl/tools/merge.py -v "${VERBOSITY_LEVEL}"
}

main "$@"; exit
