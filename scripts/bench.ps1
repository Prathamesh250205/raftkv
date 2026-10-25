# Starts a fresh 3-node cluster from a given kvnode binary, benchmarks it,
# then stops the cluster. kvbench routes each key to its shard's leader.
#   .\scripts\bench.ps1                          # writes, 1 shard
#   .\scripts\bench.ps1 -Shards 4 -Workers 128
#   .\scripts\bench.ps1 -Op get -NodeArgs "-log-reads"
#   .\scripts\bench.ps1 -DataRoot $env:TEMP\raftkv-bench   # keep data off a synced folder
param([string]$Binary = "bin\kvnode.exe", [string]$Duration = "10s", [int]$Workers = 32,
      [string]$Op = "put", [int]$Shards = 1, [string]$NodeArgs = "", [string]$DataRoot = "")

$root = Split-Path -Parent $PSScriptRoot
$bin = Join-Path $root $Binary
$cluster = "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003"
if (-not $DataRoot) { $DataRoot = "$root\data\bench" }

Get-Process kvnode* -ErrorAction SilentlyContinue | Stop-Process
Remove-Item -Recurse -Force $DataRoot -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force "$root\logs" | Out-Null

foreach ($i in 1..3) {
    Start-Process -FilePath $bin `
        -ArgumentList "-id $i -cluster $cluster -shards $Shards $NodeArgs -data `"$DataRoot\node$i`"" `
        -RedirectStandardError "$root\logs\bench-node$i.log" -WindowStyle Hidden | Out-Null
}

Start-Sleep -Seconds 3 # let elections and leader balancing settle
Push-Location $root
go run ./cmd/kvbench -addr 127.0.0.1:8001 -workers $Workers -duration $Duration -op $Op
Pop-Location

Get-Process kvnode* -ErrorAction SilentlyContinue | Stop-Process
