set -e
docker compose -f test_data/docker-compose.yml up -d

until docker exec postgres_test pg_isready -U postgres -d postgres; do
  echo "Waiting for postgres..."
  sleep 2
done

echo "Postgres ready"
