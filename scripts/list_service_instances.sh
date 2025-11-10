#!/usr/bin/env bash
set -euo pipefail

# Lists instances registered for a service via the Nacos REST API.
# Usage:
#   ./scripts/list_service_instances.sh               # lists DemoGrpcService
#   ./scripts/list_service_instances.sh MyService     # lists MyService
# NACOS_SERVER_ADDR env var controls the base URL (default http://127.0.0.1:8848).

SERVER_BASE="${NACOS_SERVER_ADDR:-http://127.0.0.1:8848}"
SERVICE_NAME="${1:-DemoGrpcService}"

QUERY_URL="${SERVER_BASE%/}/nacos/v1/ns/instance/list?serviceName=${SERVICE_NAME}"

echo "Querying instances for ${SERVICE_NAME} via:"
echo "  curl '${QUERY_URL}'"
echo
response="$(curl -sS "${QUERY_URL}")"
if command -v jq >/dev/null 2>&1; then
  echo "${response}" | jq '.'
else
  echo "${response}" | python3 -m json.tool
fi
