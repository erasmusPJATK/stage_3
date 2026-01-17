param(
  [ValidateSet("local","cluster")]
  [string]$Mode = "local",

  [string]$Nodes = "",

  [string]$Me = "",

  [string]$MqHost = "",

  [switch]$Infra,

  [int]$ScaleSearch = 1
)

$ErrorActionPreference = "Stop"

function Get-MyIp {
  try {
    $ip = (Get-NetIPAddress -AddressFamily IPv4 |
            Where-Object { $_.IPAddress -match "^\d+\.\d+\.\d+\.\d+$" -and $_.IPAddress -ne "127.0.0.1" } |
            Select-Object -First 1 -ExpandProperty IPAddress)
    return $ip
  } catch { return "" }
}

$projectRoot = Join-Path $PSScriptRoot ".."
$runtimeDir  = Join-Path $projectRoot ".runtime"
$nginxDir    = Join-Path $runtimeDir "nginx"
New-Item -ItemType Directory -Force -Path $nginxDir | Out-Null

$nginxConfPath = Join-Path $nginxDir "default.conf"
$overridePath  = Join-Path $runtimeDir "cluster.override.yml"

if ($Mode -eq "local") {

  $nginxText = @"
resolver 127.0.0.11 ipv6=off valid=2s;

upstream search_cluster {
  least_conn;
  server search:7003 resolve;
}

server {
  listen 80;

  location / {
    proxy_pass http://search_cluster;
    add_header X-Upstream `$upstream_addr always;
    proxy_next_upstream error timeout http_502 http_503 http_504;
  }
}
"@
  [System.IO.File]::WriteAllText($nginxConfPath, $nginxText, [System.Text.Encoding]::ASCII)

  Push-Location $projectRoot
  try {
    docker compose -f docker-compose.infra.yml -f docker-compose.yml up -d --build --scale search=$ScaleSearch
  } finally {
    Pop-Location
  }

  Write-Host "OK (LOCAL). LB: http://localhost:18080/status"
  exit 0
}

$nodeList = $Nodes.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }
if ($nodeList.Count -lt 1) { throw "Cluster mode requires -Nodes, e.g. -Nodes `"192.168.1.144,192.168.1.139`"" }

if ([string]::IsNullOrWhiteSpace(${Me})) { ${Me} = Get-MyIp }
if ([string]::IsNullOrWhiteSpace(${Me})) { throw "Cannot detect local IP. Pass -Me <your_ip>." }

if ([string]::IsNullOrWhiteSpace($MqHost)) { $MqHost = $nodeList[0] }
$mqUrl = "tcp://${MqHost}:61616"

$replFactor = [Math]::Min($nodeList.Count, 3)

$members = @()
foreach ($n in $nodeList) {
  $members += "${n}:5701"
  $members += "${n}:5702"
}
$membersCsv = ($members -join ",")

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
    add_header X-Upstream `$upstream_addr always;

    proxy_connect_timeout 3s;
    proxy_send_timeout 30s;
    proxy_read_timeout 30s;

    proxy_next_upstream error timeout http_502 http_503 http_504;
  }
}
"@
[System.IO.File]::WriteAllText($nginxConfPath, $nginxText, [System.Text.Encoding]::ASCII)

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
      - --mq=$mqUrl
      - --indexingQueue=ingestion.ingested
      - --origin=http://${Me}:7001
      - --replFactor=$replFactor

  indexing:
    ports:
      - "7002:7002"
      - "5701:5701"
    command:
      - java
      - -jar
      - /app/app.jar
      - --port=7002
      - --mq=$mqUrl
      - --ingestion=http://ingestion:7001
      - --hzCluster=bd-hz
      - --hzMembers=$membersCsv
      - --hzPort=5701
      - --hzInterface=${Me}

  search:
    ports:
      - "7003:7003"
      - "5702:5702"
    command:
      - java
      - -jar
      - /app/app.jar
      - --port=7003
      - --hzCluster=bd-hz
      - --hzMembers=$membersCsv
      - --hzPort=5702
      - --hzInterface=${Me}
"@
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($overridePath, $override, $utf8NoBom)

Push-Location $projectRoot
try {
  if ($Infra) {
    docker compose -f docker-compose.infra.yml up -d --build
  }
  docker compose -f docker-compose.yml -f ./.runtime/cluster.override.yml up -d --build
} finally {
  Pop-Location
}

Write-Host "OK (CLUSTER). ME=${Me} MQ=$mqUrl R=$replFactor"
if ($Infra) {
  Write-Host "LB: http://${Me}:18080/status"
}
