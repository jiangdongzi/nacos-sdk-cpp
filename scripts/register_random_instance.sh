#!/usr/bin/env bash
set -euo pipefail

# Simple helper to register a random instance of DemoGrpcService (or a service you pass in)
# Usage:
#   ./scripts/register_random_instance.sh            # registers DemoGrpcService
#   ./scripts/register_random_instance.sh MyService  # registers MyService
#
# Customize the target server via NACOS_SERVER_ADDR env var (default http://127.0.0.1:8848).

SERVER_BASE="${NACOS_SERVER_ADDR:-http://127.0.0.1:8848}"
SERVICE_NAME="${1:-DemoGrpcService}"
REGISTER_ENDPOINT="${SERVER_BASE%/}/nacos/v1/ns/instance"

# Generate random loopback IP (127.0.0.2-127.0.0.254) and high port.
last_octet=$(( (RANDOM % 253) + 2 ))
instance_ip="127.0.0.${last_octet}"
instance_port="$(shuf -i 10000-60000 -n 1)"
metadata="source=grpc-demo,ts=$(date +%s)"

echo "Registering ${SERVICE_NAME} at ${instance_ip}:${instance_port} -> ${REGISTER_ENDPOINT}"

response="$(curl -sS -X POST "${REGISTER_ENDPOINT}" \
  -d "serviceName=${SERVICE_NAME}" \
  -d "ip=${instance_ip}" \
  -d "port=${instance_port}" \
  -d "weight=1.0" \
  -d "healthy=true" \
  -d "enable=true" \
  -d "clusterName=DEFAULT" \
  -d "ephemeral=true" \
  --data-urlencode "metadata=${metadata}")"

if [[ "${response}" == "ok" ]]; then
  echo "Registered instance successfully."
  echo "Query current instances with:"
  echo "  curl '${SERVER_BASE%/}/nacos/v1/ns/instance/list?serviceName=${SERVICE_NAME}'"
else
  echo "Registration failed, server response:"
  echo "${response}"
  exit 1
fi
