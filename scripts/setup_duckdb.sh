set -e

DUCKDB="${DUCKDB:-duckdb}"

"$DUCKDB" -c "INSTALL nanoarrow FROM community;"
"$DUCKDB" -bail -c "LOAD nanoarrow;"

echo "duckdb ready: $("$DUCKDB" --version), nanoarrow installed"
