#!/usr/bin/env bash
set -euo pipefail

# 先手动启动监听器：
#   ./build/grpc-config-listen.out grpc-config-listen-test 127.0.0.1:8848
#
# 再运行本脚本发布随机内容：
#   ./scripts/publish_random_config.sh
#
# 可选参数和环境变量：
#   ./scripts/publish_random_config.sh my-data-id
#   NACOS_SERVER_ADDR=http://127.0.0.1:8848 GROUP=my-group ./scripts/publish_random_config.sh
#   NACOS_USERNAME=nacos NACOS_PASSWORD=你的密码 ./scripts/publish_random_config.sh

SERVER_BASE="${NACOS_SERVER_ADDR:-http://127.0.0.1:8848}"
SERVER_BASE="${SERVER_BASE%/}"
DATA_ID="${1:-grpc-config-listen-test}"
GROUP="${GROUP:-DEFAULT_GROUP}"
CONTENT="random-config-$(date +%s%N)-${RANDOM}"

echo "准备发布随机配置："
echo "  server : ${SERVER_BASE}"
echo "  dataId : ${DATA_ID}"
echo "  group  : ${GROUP}"
echo "  content: ${CONTENT}"
echo

if curl -fsS --max-time 3 \
    "${SERVER_BASE}/nacos/v1/console/health/readiness" >/dev/null 2>&1; then
    response="$(curl -fsS -X POST "${SERVER_BASE}/nacos/v1/cs/configs" \
        --data-urlencode "dataId=${DATA_ID}" \
        --data-urlencode "group=${GROUP}" \
        --data-urlencode "content=${CONTENT}")"
else
    username="${NACOS_USERNAME:-nacos}"
    password="${NACOS_PASSWORD:-nacos123}"
    login_response="$(curl -fsS -X POST "${SERVER_BASE}/nacos/v3/auth/user/login" \
        --data-urlencode "username=${username}" \
        --data-urlencode "password=${password}")"
    access_token="$(printf '%s' "${login_response}" | python3 -c \
        'import json,sys; d=json.load(sys.stdin); print((d.get("data") or {}).get("accessToken") or "")')"

    if [[ -z "${access_token}" ]]; then
        echo "登录 Nacos 3.x 失败，请设置正确的 NACOS_USERNAME/NACOS_PASSWORD。" >&2
        exit 1
    fi

    response="$(curl -fsS -X POST -G "${SERVER_BASE}/nacos/v3/admin/cs/config" \
        -H "accessToken: ${access_token}" \
        --data-urlencode "dataId=${DATA_ID}" \
        --data-urlencode "groupName=${GROUP}" \
        --data-urlencode "namespaceId=public" \
        --data-urlencode "content=${CONTENT}")"
fi

echo "服务端响应：${response}"
echo
echo "发布完成，请在 grpc-config-listen.out 终端查看："
echo "  CONFIG_UPDATE_RECEIVED=${CONTENT}"
