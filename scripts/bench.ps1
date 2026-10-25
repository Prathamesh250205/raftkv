# Starts a fresh 3-node cluster from a given kvnode binary, benchmarks writes
# against the leader, then stops the cluster.
#   .\scripts\bench.ps1                          # uses bin\kvnode.exe
#   .\scripts\bench.ps1 -Binary bin\old.exe -Duration 10s -Workers 64
#   .\scripts\bench.ps1 -Op get -NodeArgs "-log-reads"
param([string]$Binary = "bin\kvnode.exe", [string]$Duration = "10s", [int]$Workers = 32,
      [string]$Op = "put", [string]$NodeArgs = "")

$root = Split-Path -Parent $PSScriptRoot
$bin = Join-Path $root $Binary
$cluster = "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003"

Get-Process kvnode* -ErrorAction SilentlyContinue | Stop-Process
Remove-Item -Recurse -Force "$root\data\bench" -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force "$root\logs" | Out-Null

foreach ($i in 1..3) {
    Start-Process -FilePath $bin `
        -ArgumentList "-id $i -cluster $cluster $NodeArgs -data `"$root\data\bench\node$i`"" `
        -RedirectStandardError "$root\logs\bench-node$i.log" -WindowStyle Hidden | Out-Null
}

$leader = $null
foreach ($try in 1..50) {
    Start-Sleep -Milliseconds 200
    foreach ($p in 8001..8003) {
        try {
            $s = Invoke-RestMethod "http://127.0.0.1:$p/status" -TimeoutSec 1
            if ($s.state -eq "leader") { $leader = "127.0.0.1:$p" }
        } catch {}
    }
    if ($leader) { break }
}
if (-not $leader) { Write-Host "no leader elected"; return }

Push-Location $root
go run ./cmd/kvbench -addr $leader -workers $Workers -duration $Duration -op $Op
Pop-Location

Get-Process kvnode* -ErrorAction SilentlyContinue | Stop-Process
