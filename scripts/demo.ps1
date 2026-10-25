# Guided live demo: starts a fresh 3-node, 3-shard cluster, writes data, kills
# a leader, shows the failover and recovery, then benchmarks it.
#   .\scripts\demo.ps1          # pauses before each step (for presenting)
#   .\scripts\demo.ps1 -Auto    # runs straight through (for rehearsing)
param([switch]$Auto)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
if (-not (Get-Command go -ErrorAction SilentlyContinue) -and (Test-Path C:\go\bin\go.exe)) {
    $env:Path = "C:\go\bin;$env:Path"
}
$cluster = "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003"

function Step($title, $say) {
    Write-Host ""
    Write-Host "==== $title ====" -ForegroundColor Cyan
    Write-Host $say -ForegroundColor DarkGray
    if (-not $Auto) { Read-Host "press Enter" | Out-Null }
}
function Run($cmd) {
    Write-Host "> $cmd" -ForegroundColor Yellow
    Invoke-Expression $cmd
}
function Shards($port) { Invoke-RestMethod "http://127.0.0.1:$port/shards" -TimeoutSec 2 }
function StartNode($id) {
    Start-Process -FilePath "$root\bin\kvnode.exe" -WindowStyle Hidden `
        -ArgumentList "-id $id -cluster $cluster -shards 3 -data `"$root\data\demo\node$id`"" `
        -RedirectStandardError "$root\logs\demo-node$id.log" | Out-Null
}

Step "1. Start the cluster" "Three nodes, each running three Raft groups (shards). Keys are hashed to a shard; each shard elects its own leader."
Get-Process kvnode -ErrorAction SilentlyContinue | Stop-Process
Remove-Item -Recurse -Force "$root\data\demo" -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force "$root\bin", "$root\logs" | Out-Null
go build -o bin\kvnode.exe .\cmd\kvnode
go build -o bin\kvctl.exe .\cmd\kvctl
1..3 | ForEach-Object { StartNode $_ }
do { Start-Sleep -Milliseconds 300; $s = Shards 8001 } until (@($s | Where-Object leader_addr).Count -eq 3)
Start-Sleep 2 # let leader balancing settle
Run ".\bin\kvctl.exe status"

Step "2. Write and read" "Write through node 1, read through nodes 2 and 3. Any node accepts any request and redirects to the right leader."
Run 'curl.exe -s -L -X PUT --data "hello interviewer" http://127.0.0.1:8001/kv/greeting'
Run "curl.exe -s -L http://127.0.0.1:8002/kv/greeting; ''"
Run 'curl.exe -s -L -X POST --data " (appended)" http://127.0.0.1:8003/kv/greeting/append'
Run "curl.exe -s -L http://127.0.0.1:8001/kv/greeting; ''"
Write-Host "> writing user1 .. user20 (spread across all three shards)" -ForegroundColor Yellow
1..20 | ForEach-Object { curl.exe -s -L -X PUT --data "value-$_" "http://127.0.0.1:8001/kv/user$_" }
Write-Host "done"

$victim = [int](((Shards 8001) | Where-Object shard -eq 0).leader_id)
$alive = @(1, 2, 3 | Where-Object { $_ -ne $victim })
$survivor = 8000 + $alive[0]
Step "3. Kill a leader" "Node $victim leads shard 0. I'll kill its process with no warning, like a machine crashing."
$proc = Get-CimInstance Win32_Process -Filter "Name='kvnode.exe'" | Where-Object { $_.CommandLine -match "-id $victim " }
Stop-Process -Id $proc.ProcessId -Force
$t = [Diagnostics.Stopwatch]::StartNew()
Write-Host "> killed node $victim (pid $($proc.ProcessId))" -ForegroundColor Yellow
do {
    Start-Sleep -Milliseconds 50
    $leader = ((Shards $survivor) | Where-Object shard -eq 0).leader_id
} until ($leader -and $leader -ne $victim -and $leader -ne -1)
Write-Host ("New leader for shard 0: node {0}, elected in {1:N2} s" -f $leader, $t.Elapsed.TotalSeconds) -ForegroundColor Green
Run ".\bin\kvctl.exe -addr 127.0.0.1:$survivor status"

Step "4. Nothing was lost" "Every key written before the crash is still readable, and new writes still work with one node down."
$missing = 0
1..20 | ForEach-Object { if ((curl.exe -s -L "http://127.0.0.1:$survivor/kv/user$_") -ne "value-$_") { $missing++ } }
Write-Host "20 keys checked, $missing missing" -ForegroundColor Green
Run "curl.exe -s -L -X PUT --data 'written while node $victim was down' http://127.0.0.1:$survivor/kv/during-failure"
Run "curl.exe -s -L http://127.0.0.1:$survivor/kv/during-failure; ''"

Step "5. The node comes back" "Restarting node ${victim}: it reloads its log from disk and catches up on the write it missed."
StartNode $victim
Start-Sleep 3
Run "curl.exe -s -L http://127.0.0.1:$(8000 + $victim)/kv/during-failure; ''"
Run ".\bin\kvctl.exe status"

Step "6. Performance" "Five-second benchmark: 64 concurrent clients writing, each request sent straight to its shard leader."
Run "go run ./cmd/kvbench -addr 127.0.0.1:8001 -workers 64 -duration 5s"

Step "7. Observability" "Every node exports Prometheus metrics: elections, fsync latency, batch sizes, requests."
(Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8001/metrics).Content -split "`n" |
    Select-String '^(raft_is_leader|raft_term|raft_elections_started_total|kv_requests_total\{op="put",result="ok"\})' |
    ForEach-Object { $_.Line }

Step "8. Clean up" "Stopping the cluster."
Get-Process kvnode -ErrorAction SilentlyContinue | Stop-Process
Write-Host "Demo finished." -ForegroundColor Green
