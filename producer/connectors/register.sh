#!/usr/bin/env bash
# Registers (or updates) the Oracle/Postgres JDBC source connectors against
# the Kafka Connect REST API. Run this after `docker compose up kafka-connect`
# and after filling in real values in producer/connectors/*.json.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"

for config_file in producer/connectors/*.json; do
  name=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['name'])" "$config_file")

  echo "Registering ${name} from ${config_file}..."
  curl -s -X PUT \
    -H "Content-Type: application/json" \
    --data "$(python3 -c "import json,sys; print(json.dumps(json.load(open(sys.argv[1]))['config']))" "$config_file")" \
    "${CONNECT_URL}/connectors/${name}/config" | python3 -m json.tool
  echo
done
