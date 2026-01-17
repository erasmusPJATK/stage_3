param(
  [Parameter(Mandatory=$true)]
  [ValidateSet("A","B","C")]
  [string]$Role,

  [string]$Nodes = "192.168.1.144,192.168.1.139,192.168.1.201",

  [string]$MqHost = "192.168.1.144"
)

$ErrorActionPreference = "Stop"

$nodeList = $Nodes.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }
if ($nodeList.Count -lt 3) {
  throw "Nodes must contain 3 comma-separated IPs, e.g. 192.168.1.144,192.168.1.139,192.168.1.201"
}

$ipMap = @{
  "A" = $nodeList[0]
  "B" = $nodeList[1]
  "C" = $nodeList[2]
}

$hostIp = $ipMap[$Role]
$mqUrl = "tcp://$MqHost:61616"
$originUrl = "http://$hostIp:7001"
$replFactor = 3

$members = @(
  "$($nodeList[0]):5701","$($nodeList[0]):5702",
  "$($nodeList[1]):5701","$($nodeList[1]):5702",
  "$($nodeList[2]):5701","$($nodeList[2]):5702"
) -join ","

$projectRoot = Join-Path $PSScriptRoot ".."
$runtimeDir = Join-Path $projectRoot ".runtime"
$nginxDir = Join-Path $runtimeDir "nginx"

New-Item -ItemType Directory -Force -Path $nginxDir | Out-Null

if ($Role -eq "A") {
  $nginxConfPath = Join-Path $nginxDir "default.conf"
  $upstreams = $nodeList | ForEach-Object { "  server ${_}:7003 max_fails=2 fail_timeout=2s;" }

  $nginxText = @"
upstream search_cluster {
  least_conn;
$($upstreams -join "`n")
}

server {
  listen 80;

  location / {
    proxy_pass http://search_cluster;

    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_set_header Host `$host;
    proxy_set_header X-Forwarded-For `$proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto `$scheme;

    proxy_connect_timeout 3s;
    proxy_send_timeout 30s;
    proxy_read_timeout 30s;

    proxy_next_upstream error timeout http_502 http_503 http_504;
  }
}
"@
  $nginxText | Set-Content -NoNewline -Encoding UTF8 $nginxConfPath
}

$overridePath = Join-Path $runtimeDir "cluster.override.yml"

$override = @"
services:
  ingestion:
    ports:
      - "7001:7001"
    command:
      - java
      - -jar
      - /app/app.jar
      - --port=7001
      - --origin=$originUrl
      - --mq=$mqUrl
      - --indexingQueue=ingestion.ingested
      - --indexingMqEnabled=true
      - --replFactor=$replFactor

  indexing:
    ports:
      - "7002:7002"
      - "5701:5701"
    environment:
      - JAVA_TOOL_OPTIONS=-Dhazelcast.local.publicAddress=$hostIp:5701
    command:
      - java
      - -jar
      - /app/app.jar
      - --port=7002
      - --mq=$mqUrl
      - --mqEnabled=true
      - --ingestion=http://$hostIp:7001
      - --hzCluster=bd-hz
      - --hzMembers=$members

  search:
    ports:
      - "7003:7003"
      - "5702:5701"
    environment:
      - JAVA_TOOL_OPTIONS=-Dhazelcast.local.publicAddress=$hostIp:5702
    command:
      - java
      - -jar
      - /app/app.jar
      - --port=7003
      - --hzCluster=bd-hz
      - --hzMembers=$members
"@

if ($Role -eq "A") {
  $override += @"

  lb:
    volumes:
      - ./.runtime/nginx/default.conf:/etc/nginx/conf.d/default.conf:ro
"@
}

$override | Set-Content -NoNewline -Encoding UTF8 $overridePath

Push-Location $projectRoot
try {
  if ($Role -eq "A") {
    docker compose -f docker-compose.infra.yml -f docker-compose.yml -f ./.runtime/cluster.override.yml up -d --build
  } else {
    docker compose -f docker-compose.yml -f ./.runtime/cluster.override.yml up -d --build --no-deps ingestion indexing search
  }
} finally {
  Pop-Location
}

Write-Host "OK: node $Role started (HOST_IP=$hostIp, MQ=$mqUrl)"
if ($Role -eq "A") {
  Write-Host "LB: http://$hostIp:18080/status"
}
