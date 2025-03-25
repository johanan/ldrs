set -e
docker compose -f test_data/docker-compose.yml up -d

until docker exec postgres_test pg_isready -U postgres -d postgres; do
  echo "Waiting for postgres..."
  sleep 2
done

echo "Postgres ready"

docker exec postgres_test psql -U postgres -d postgres -c "
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'test_role') THEN
    CREATE ROLE test_role;
  END IF;
END
\$\$;
GRANT test_role TO postgres;

GRANT ALL PRIVILEGES ON DATABASE postgres TO test_role;
"