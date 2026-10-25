# Builds kvnode and starts a local cluster in the background.
#   .\scripts\cluster.ps1            # 3 nodes on ports 8001-8003
#   .\scripts\cluster.ps1 -Nodes 5
#   .\scripts\cluster.ps1 -Stop
param([int]$Nodes = 3, [switch]$Stop)

$root = Split-Path -Parent $PSScriptRoot

if ($Stop) {
    Get-Process kvnode -ErrorAction SilentlyContinue | Stop-Process
    Write-Host "cluster stopped"
    return
}

New-Item -ItemType Directory -Force "$root\bin", "$root\logs" | Out-Null
Push-Location $root
go build -o bin\kvnode.exe .\cmd\kvnode
$built = $?
Pop-Location
if (-not $built) { return }

$cluster = (1..$Nodes | ForEach-Object { "$_=127.0.0.1:$(8000 + $_)" }) -join ','
foreach ($i in 1..$Nodes) {
    $p = Start-Process -FilePath "$root\bin\kvnode.exe" `
        -ArgumentList "-id $i -cluster $cluster -data `"$root\data\node$i`"" `
        -RedirectStandardError "$root\logs\node$i.log" `
        -WindowStyle Hidden -PassThru
    Write-Host "node $i  http://127.0.0.1:$(8000 + $i)  pid $($p.Id)  log logs\node$i.log"
}
