param(
# can be: "1" or "1,3" or "1 3" or even: -ScaleList 1,3 (array)
    [object]$ScaleList = "1",

    [int]$SmallN  = 10,
    [int]$MediumN = 50,
    [int]$LargeN  = 200,

    [int]$SearchRequests = 200,

    [int]$IndexTimeoutSmall  = 1800,
    [int]$IndexTimeoutMedium = 3600,
    [int]$IndexTimeoutLarge  = 7200
)

$ErrorActionPreference = "Stop"
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -Scope Global -ErrorAction SilentlyContinue) {
    $global:PSNativeCommandUseErrorActionPreference = $false
}

# ---------- SCALE LIST PARSE (FIX) ----------
function Parse-ScaleList([object]$ScaleListObj) {
    $scaleStr = ""
    if ($ScaleListObj -is [System.Array]) {
        $scaleStr = ($ScaleListObj -join ",")
    } else {
        $scaleStr = [string]$ScaleListObj
    }

    $scales = @(
    $scaleStr -split '[,; ]+' |
            ForEach-Object { $_.Trim() } |
            Where-Object { $_ -match '^\d+$' } |
            ForEach-Object { [int]$_ }
    )

    if ($scales.Count -eq 0) { return @(1) }
    return $scales
}

$SCALES = Parse-ScaleList $ScaleList

# ---------- PATHS ----------
$BenchRoot   = $PSScriptRoot
$ProjectRoot = (Resolve-Path (Join-Path $BenchRoot "..")).Path
$RunsRoot    = Join-Path $BenchRoot "runs"
New-Item -ItemType Directory -Force $RunsRoot | Out-Null

$RunDir = Join-Path $RunsRoot ("RUN_" + (Get-Date -Format "yyyyMMdd_HHmmss"))
New-Item -ItemType Directory -Force $RunDir | Out-Null

Write-Host ""
Write-Host "========================================"
Write-Host "BENCH RUN DIR: $RunDir"
Write-Host "PROJECT ROOT : $ProjectRoot"
Write-Host "SCALES       : $($SCALES -join ',')"
Write-Host "SIZES        : small=$SmallN medium=$MediumN large=$LargeN"
Write-Host "========================================"
Write-Host ""

Push-Location $ProjectRoot

# ---------- CURL ----------
$CURL = "curl.exe"
if (-not (Get-Command $CURL -ErrorAction SilentlyContinue)) {
    throw "curl.exe not found. On Windows it should exist. If not: install curl or use Git Bash."
}

$INGEST = "http://127.0.0.1:7001"
$INDEX  = "http://127.0.0.1:7002"
$LB     = "http://127.0.0.1:18080"

# ---------- HELPERS ----------
function Curl-HttpCode([string]$Url) {
    try { return (& $CURL -s -o NUL -w '%{http_code}' $Url) } catch { return "" }
}

function Wait-Http200([string]$Url, [int]$TimeoutSec = 180) {
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    while ($sw.Elapsed.TotalSeconds -lt $TimeoutSec) {
        $code = Curl-HttpCode $Url
        if ($code -eq "200") { return $true }
        Start-Sleep -Seconds 2
    }
    return $false
}

# Runs native commands through cmd /c to avoid stderr => NativeCommandError in PowerShell
function Run-CmdLogged([string]$Cmd, [string]$LogPath) {
    Write-Host "CMD> $Cmd"
    cmd /c "$Cmd 2>&1" | Tee-Object -FilePath $LogPath | Out-Null
    return $LASTEXITCODE
}

function Compose-Down() {
    Write-Host "`n>>> DOCKER COMPOSE DOWN (-v)"
    $log = Join-Path $RunDir "compose_down.log"
    $exit = Run-CmdLogged "docker compose -f docker-compose.infra.yml -f docker-compose.yml down -v --remove-orphans" $log
    # exit code can be non-zero if nothing existed; do not hard-fail
}

function Start-Local([int]$ScaleSearch) {
    Compose-Down

    Write-Host "`n>>> START LOCAL (search replicas = $ScaleSearch)"
    $log = Join-Path $RunDir ("start_local_scale" + $ScaleSearch + ".log")

    $runCluster = Join-Path $ProjectRoot "scripts\run-cluster.ps1"
    $cmd = "powershell -NoProfile -ExecutionPolicy Bypass -File `"$runCluster`" -Mode local -ScaleSearch $ScaleSearch"
    $exit = Run-CmdLogged $cmd $log

    if ($exit -ne 0) {
        throw "run-cluster.ps1 failed with exit code $exit (see $log)"
    }

    if (-not (Wait-Http200 "$LB/status" 240)) {
        throw "LB not ready at $LB/status. Check: docker logs task3-lb-1"
    }
    if (-not (Wait-Http200 "$INDEX/index/status" 240)) {
        throw "Indexing not ready at $INDEX/index/status"
    }

    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" |
            Out-File (Join-Path $RunDir ("docker_ps_scale" + $ScaleSearch + ".txt")) -Encoding utf8

    Write-Host "OK (LOCAL). LB: $LB/status"
}

# ---------- DATASET IDS ----------
function Get-GutenbergTopIds([int]$Max = 600) {
    try {
        $resp = Invoke-WebRequest -Uri "https://www.gutenberg.org/browse/scores/top" -UseBasicParsing -TimeoutSec 25
        $html = $resp.Content
        $matches = [regex]::Matches($html, '/ebooks/(\d+)')
        $ids = @()
        foreach ($m in $matches) { $ids += [int]$m.Groups[1].Value }
        $ids = $ids | Select-Object -Unique
        if ($ids.Count -gt $Max) { $ids = $ids[0..($Max-1)] }
        return $ids
    } catch {
        return @()
    }
}

# Get a lot, so Large=200 always has enough
$TOP_IDS = Get-GutenbergTopIds 600

# fallback: expanded list (still: if you have internet, TOP_IDS will be used anyway)
if ($TOP_IDS.Count -lt 220) {
    $TOP_IDS = @(
        1342,84,11,1661,2701,98,1952,1080,2600,174,46,76,120,1400,2542,4363,3207,4300,23,74,35,6130,408,141,43,160,55,219,996,
        25344,260,61,62,63,64,65,66,67,68,69,70,71,72,73,85,86,87,88,89,90,
        244,254,255,256,257,258,259,261,262,263,264,265,266,267,268,269,271,272,273,274,275,
        345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,
        1184,1232,1251,135,145,150,151,152,153,154,155,156,157,158,159,
        203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,
        514,515,516,517,518,519,520,521,522,523,524,525,526,527,528,529,
        100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115
    ) | Select-Object -Unique
}

function Get-DatasetIds([string]$Name, [int]$N) {
    if ($TOP_IDS.Count -ge $N) { return $TOP_IDS[0..($N-1)] }
    return $TOP_IDS
}

# ---------- INGEST ----------
function Ingest-Pack([string]$PackName, [int[]]$Ids, [int]$ScaleSearch) {
    $jsonl   = Join-Path $RunDir ("K1_ingest_" + $PackName + "_S" + $ScaleSearch + ".jsonl")
    $summary = Join-Path $RunDir ("K1_ingest_" + $PackName + "_S" + $ScaleSearch + "_summary.txt")
    $idsFile = Join-Path $RunDir ("DATASET_" + $PackName + "_ids.txt")

    $Ids | Out-File $idsFile -Encoding utf8

    Write-Host "`n>>> INGEST: $PackName (n=$($Ids.Count)) [scale=$ScaleSearch]"
    "# $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Out-File $jsonl -Encoding utf8

    $ok = 0
    $sw = [System.Diagnostics.Stopwatch]::StartNew()

    foreach ($id in $Ids) {
        $url  = "$INGEST/ingest/$id"
        $resp = & $CURL -s --connect-timeout 3 --max-time 240 -X POST $url
        $resp | Add-Content -Path $jsonl -Encoding utf8

        try {
            $obj = $resp | ConvertFrom-Json
            if ($obj.http_status -eq 200) { $ok++ }
        } catch {}
    }

    $sw.Stop()

    $rate = if ($sw.Elapsed.TotalSeconds -gt 0) { [Math]::Round(($ok / $sw.Elapsed.TotalSeconds), 4) } else { 0 }
    "pack=$PackName docs_ok=$ok docs_total=$($Ids.Count) time_s=$([Math]::Round($sw.Elapsed.TotalSeconds,3)) docs_per_s=$rate" |
            Out-File $summary -Encoding utf8

    Write-Host "OK docs: $ok / $($Ids.Count)"
    return $ok
}

# ---------- INDEXING WAIT ----------
function Get-IndexStatus() {
    try { return Invoke-RestMethod -Uri "$INDEX/index/status" -TimeoutSec 5 } catch { return $null }
}

function Wait-IndexingComplete([string]$PackName, [int]$ExpectedDocs, [int]$TimeoutSec, [int]$ScaleSearch) {
    $poll  = Join-Path $RunDir ("K2_index_" + $PackName + "_S" + $ScaleSearch + "_poll.csv")
    $final = Join-Path $RunDir ("K2_index_" + $PackName + "_S" + $ScaleSearch + "_final.json")

    Write-Host "`n>>> WAIT INDEXING: $PackName expectedDocs=$ExpectedDocs timeout=${TimeoutSec}s [scale=$ScaleSearch]"
    "ts,elapsed_s,docs,terms" | Out-File $poll -Encoding utf8

    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $stable = 0
    $prevDocs = -1
    $prevTerms = -1

    while ($sw.Elapsed.TotalSeconds -lt $TimeoutSec) {
        $st = Get-IndexStatus
        if ($null -eq $st) {
            Add-Content -Path $poll -Value "$(Get-Date -Format s),$([int]$sw.Elapsed.TotalSeconds),NA,NA" -Encoding utf8
            Start-Sleep -Seconds 3
            continue
        }

        $docs  = [int]$st.docs
        $terms = [int]$st.terms

        Add-Content -Path $poll -Value "$(Get-Date -Format s),$([int]$sw.Elapsed.TotalSeconds),$docs,$terms" -Encoding utf8
        Write-Host ("  t={0,6}s  docs={1,5}  terms={2,8}  stable={3}" -f ([int]$sw.Elapsed.TotalSeconds), $docs, $terms, $stable)

        if ($docs -ge $ExpectedDocs) {
            if ($docs -eq $prevDocs -and $terms -eq $prevTerms) { $stable++ } else { $stable = 0 }
            if ($stable -ge 3) {
                $st | ConvertTo-Json -Depth 8 | Out-File $final -Encoding utf8
                Write-Host "INDEXING DONE: docs=$docs terms=$terms"
                return $true
            }
        }

        $prevDocs  = $docs
        $prevTerms = $terms
        Start-Sleep -Seconds 3
    }

    $last = Get-IndexStatus
    if ($last) { $last | ConvertTo-Json -Depth 8 | Out-File $final -Encoding utf8 }
    Write-Host "INDEXING TIMEOUT (saved last status)"
    return $false
}

# ---------- SEARCH BENCH ----------
function Percentile([double[]]$Arr, [double]$P) {
    if ($Arr.Count -eq 0) { return 0 }
    $sorted = $Arr | Sort-Object
    $idx = [int][Math]::Floor(($P / 100.0) * ($sorted.Count - 1))
    return $sorted[$idx]
}

function Parse-DoubleInvariant([string]$s) {
    $v = 0.0
    [double]::TryParse($s,
            [System.Globalization.NumberStyles]::Any,
            [System.Globalization.CultureInfo]::InvariantCulture,
            [ref]$v) | Out-Null
    return $v
}

function Run-SearchBenchmark([string]$PackName, [int]$N, [int]$ScaleSearch) {
    $csv = Join-Path $RunDir ("K3_search_" + $PackName + "_S" + $ScaleSearch + "_latency.csv")
    $sum = Join-Path $RunDir ("K3_search_" + $PackName + "_S" + $ScaleSearch + "_summary.txt")

    $terms = @('love','war','peace','world','time','man','woman','night','king','day','good','great','death','life','heart','friend','fire','sea','home','money')

    Write-Host "`n>>> SEARCH BENCH: $PackName requests=$N [scale=$ScaleSearch]"
    "iter,term,latency_s,http_code" | Out-File $csv -Encoding utf8

    # warmup
    for ($w=0; $w -lt 10; $w++) {
        $q = Get-Random -InputObject $terms
        $u = "$LB/search?q=$([uri]::EscapeDataString($q))&limit=5"
        & $CURL -s -o NUL --max-time 30 $u | Out-Null
    }

    $lat = New-Object System.Collections.Generic.List[double]
    $sw = [System.Diagnostics.Stopwatch]::StartNew()

    for ($i=1; $i -le $N; $i++) {
        $q = Get-Random -InputObject $terms
        $url = "$LB/search?q=$([uri]::EscapeDataString($q))&limit=5"

        # single call, return: time_total,http_code
        $metrics = & $CURL -s -o NUL -w '%{time_total},%{http_code}' --max-time 30 $url
        $parts = $metrics -split ','
        $timeStr = $parts[0]
        $codeStr = if ($parts.Count -ge 2) { $parts[1] } else { "" }

        $t = Parse-DoubleInvariant $timeStr
        $lat.Add($t)

        "$i,$q,$t,$codeStr" | Add-Content -Path $csv -Encoding utf8
    }

    $sw.Stop()
    $total = $sw.Elapsed.TotalSeconds
    $qps = if ($total -gt 0) { [Math]::Round(($N / $total), 4) } else { 0 }

    $avg = if ($lat.Count -gt 0) { [Math]::Round(($lat | Measure-Object -Average).Average, 4) } else { 0 }
    $p50 = [Math]::Round((Percentile $lat.ToArray() 50), 4)
    $p95 = [Math]::Round((Percentile $lat.ToArray() 95), 4)
    $mx  = [Math]::Round(($lat | Measure-Object -Maximum).Maximum, 4)

    @(
        "pack=$PackName scale=$ScaleSearch",
        "requests=$N total_s=$([Math]::Round($total,3)) qps=$qps",
        "latency_s avg=$avg p50=$p50 p95=$p95 max=$mx"
    ) | Out-File $sum -Encoding utf8

    Write-Host "DONE qps=$qps avg=$avg p50=$p50 p95=$p95"
}

# ---------- RESOURCE SNAPSHOT ----------
function Snapshot-DockerStats([string]$Tag, [int]$ScaleSearch) {
    $out = Join-Path $RunDir ("S0_docker_stats_" + $Tag + "_S" + $ScaleSearch + ".csv")
    "# name,cpu,mem_usage,mem_perc" | Out-File $out -Encoding utf8
    docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}}" |
            Add-Content -Path $out -Encoding utf8
}

# ---------- MAIN ----------
$summaryCsv = Join-Path $RunDir "SUMMARY.csv"
"scale,pack,docs_expected,docs_ingested_ok,index_ok,search_requests" | Out-File $summaryCsv -Encoding utf8

foreach ($scale in $SCALES) {

    # SMALL
    $smallIds = Get-DatasetIds "SMALL" $SmallN
    Start-Local $scale
    Snapshot-DockerStats "START_SMALL" $scale
    $okInSmall = Ingest-Pack "SMALL" $smallIds $scale
    $okSmall = Wait-IndexingComplete "SMALL" $okInSmall $IndexTimeoutSmall $scale
    Snapshot-DockerStats "AFTER_INDEX_SMALL" $scale
    Run-SearchBenchmark "SMALL" $SearchRequests $scale
    Snapshot-DockerStats "AFTER_SEARCH_SMALL" $scale
    "$scale,SMALL,$($smallIds.Count),$okInSmall,$okSmall,$SearchRequests" | Add-Content -Path $summaryCsv -Encoding utf8

    # MEDIUM
    $mediumIds = Get-DatasetIds "MEDIUM" $MediumN
    Start-Local $scale
    Snapshot-DockerStats "START_MEDIUM" $scale
    $okInMed = Ingest-Pack "MEDIUM" $mediumIds $scale
    $okMed   = Wait-IndexingComplete "MEDIUM" $okInMed $IndexTimeoutMedium $scale
    Snapshot-DockerStats "AFTER_INDEX_MEDIUM" $scale
    Run-SearchBenchmark "MEDIUM" $SearchRequests $scale
    Snapshot-DockerStats "AFTER_SEARCH_MEDIUM" $scale
    "$scale,MEDIUM,$($mediumIds.Count),$okInMed,$okMed,$SearchRequests" | Add-Content -Path $summaryCsv -Encoding utf8

    # LARGE
    $largeIds = Get-DatasetIds "LARGE" $LargeN
    Start-Local $scale
    Snapshot-DockerStats "START_LARGE" $scale
    $okInLarge = Ingest-Pack "LARGE" $largeIds $scale
    $okLarge = Wait-IndexingComplete "LARGE" $okInLarge $IndexTimeoutLarge $scale
    Snapshot-DockerStats "AFTER_INDEX_LARGE" $scale
    Run-SearchBenchmark "LARGE" $SearchRequests $scale
    Snapshot-DockerStats "AFTER_SEARCH_LARGE" $scale
    "$scale,LARGE,$($largeIds.Count),$okInLarge,$okLarge,$SearchRequests" | Add-Content -Path $summaryCsv -Encoding utf8
}

Write-Host ""
Write-Host "========================================"
Write-Host "ALL DONE. Results are in: $RunDir"
Write-Host "========================================"
Write-Host ""

Pop-Location
