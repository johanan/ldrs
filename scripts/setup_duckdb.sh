#!/usr/bin/env bash
set -euo pipefail

#   scripts/setup_duckdb.sh
#   scripts/setup_duckdb.sh 1.2.1 1.5.2

CLI_ROOT="${DUCKDB_CLI_ROOT:-$HOME/.duckdb/cli}"

binary_for() {
    local binary="$CLI_ROOT/$1/duckdb"
    local base="https://github.com/duckdb/duckdb/releases/download/v$1"
    if [ ! -x "$binary" ]; then
        mkdir -p "$CLI_ROOT/$1"
        # 2.0 ships a tarball; earlier releases ship a zip under an underscored name.
        if curl -sLf -o "/tmp/duckdb-$1.tar.gz" "$base/duckdb-cli-linux-amd64.tar.gz"; then
            tar -xzf "/tmp/duckdb-$1.tar.gz" -C "$CLI_ROOT/$1"
        else
            curl -sSLf -o "/tmp/duckdb-$1.zip" "$base/duckdb_cli-linux-amd64.zip"
            unzip -oq "/tmp/duckdb-$1.zip" -d "$CLI_ROOT/$1"
        fi
    fi
    echo "$binary"
}

setup() {
    "$1" -c "INSTALL nanoarrow FROM community;"
    "$1" -bail -c "LOAD nanoarrow;"
    echo "duckdb ready: $("$1" --version), nanoarrow installed"
}

if [ "$#" -eq 0 ]; then
    setup "${DUCKDB:-duckdb}"
else
    for version in "$@"; do
        setup "$(binary_for "$version")"
    done
fi
