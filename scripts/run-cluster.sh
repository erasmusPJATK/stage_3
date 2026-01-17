#!/usr/bin/env bash
set -euo pipefail

ROLE="${1:-}"
NODES="${2:-192.168.1.144,192.168.1.139,192.168.1.201}"
MQ_HOST="${3:-192.168.1.144}"

if [[ -z "$ROLE" ]]; then
  echo "Usage: ./scripts/run-cluster.sh A|B|C [nodes] [mq_host]"
  exit 1
fi

IFS=',' read -r -a ARR <<< "$NODES"
if [[ "${#ARR[@]}" -lt 3 ]]; then
  echo "Nodes must contain 3 comma-separated IPs"
  exit 1
fi

A_IP="${ARR[0]}"
B_IP="${ARR[1]}"
C_IP="${ARR[2]}"

case "$ROLE" in
  A) HOST_IP="$A_IP" ;;
  B) HOST_IP="$B_IP" ;;
  C) HOST_IP="$C_IP" ;;
  *) echo "Role must be A, B or C"; exit 1 ;;
esac

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUNTIME_DIR="$PROJECT_ROOT/.runtime"
NGINX_DIR="$RUNTIME_DIR/nginx"
mkdir -p "$NGINX_DIR"

MQ_URL="tcp://${MQ_HOST}:61616"
ORIGIN_URL="http://${HOST_IP}:7001"
REPL_FACTOR="3"

HZ_MEMBERS="${A_IP}:5701,${A_IP}:5702,${B_IP}:5701,${B_IP}:5702,${C_IP}:5701,${C_IP}:5702"

# nginx config only on A
if [[ "$ROLE" == "A" ]]; then
cat > "$NGINX_DIR/default.conf" <<EOF
upstream search_cluster {
  least_conn;
  server ${A_IP}:7003 max_fails=2 fail_timeout=2s;
  server ${B_IP}:7003 max_fails=2 fail_timeout=2s;
  server ${C_IP}:7003 max_fails=2 fail_timeout=2s;
}

server {
  listen 80;

  location / {
    proxy_pass http://search_cluster;

    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_set_header Host \$host;
    proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto \$scheme;

    proxy_connect_timeout 3s;
    proxy_send_timeout 30s;
    proxy_read_timeout 30s;

    proxy_next_upstream error timeout http_502 http_503 http_504;
  }
}
EOF
fi

# compose override
OVERRIDE_PATH="$RUNTIME_DIR/cluster.override.yml"
cat > "$OVERRIDE_PATH" <<EOF
services:
  ingestion:
    ports:
      - "7001:7001"
    command:
      - java
      - -jar
      - /app/app.jar
      - --port=7001
      - --origin=${ORIGIN_URL}
      - --mq=${MQ_URL}
      - --indexingQueue=ingestion.ingested
      - --indexingMqEnabled=true
      - --replFactor=${REPL_FACTOR}

  indexing:
    ports:
      - "7002:7002"
      - "5701:5701"
    environment:
      - JAVA_TOOL_OPTIONS=-Dhazelcast.local.publicAddress=${HOST_IP}:5701
    command:
      - java
      - -jar
      - /app/app.jar
      - --port=7002
      - --mq=${MQ_URL}
      - --mqEnabled=true
      - --ingestion=http://${HOST_IP}:7001
      - --hzCluster=bd-hz
      - --hzMembers=${HZ_MEMBERS}

  search:
    ports:
      - "7003:7003"
      - "5702:5701"
    environment:
      - JAVA_TOOL_OPTIONS=-Dhazelcast.local.publicAddress=${HOST_IP}:5702
    command:
      - java
      - -jar
      - /app/app.jar
      - --port=7003
      - --hzCluster=bd-hz
      - --hzMembers=${HZ_MEMBERS}
EOF

if [[ "$ROLE" == "A" ]]; then
cat >> "$OVERRIDE_PATH" <<EOF

  lb:
    volumes:
      - ./.runtime/nginx/default.conf:/etc/nginx/conf.d/default.conf:ro
EOF
fi

cd "$PROJECT_ROOT"

if [[ "$ROLE" == "A" ]]; then
  docker compose -f docker-compose.infra.yml -f docker-compose.yml -f ./.runtime/cluster.override.yml up -d --build
else
  docker compose -f docker-compose.yml -f ./.runtime/cluster.override.yml up -d --build --no-deps ingestion indexing search
fi

echo "OK: node $ROLE started (HOST_IP=$HOST_IP, MQ=$MQ_URL)"
if [[ "$ROLE" == "A" ]]; then
  echo "LB: http://${HOST_IP}:18080/status"
fi
