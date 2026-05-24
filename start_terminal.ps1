$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $repoRoot

$bridgeUrl = "http://127.0.0.1:8765"
$pyPath = Join-Path $repoRoot ".venv\Scripts\python.exe"
$python = if (Test-Path $pyPath) { $pyPath } else { "python" }

$bridgeOnline = $false
try {
    $resp = Invoke-WebRequest -Uri ($bridgeUrl + "/ping") -UseBasicParsing -TimeoutSec 2
    if ($resp.StatusCode -eq 200) { $bridgeOnline = $true }
} catch {
    $bridgeOnline = $false
}

if (-not $bridgeOnline) {
    Start-Process -FilePath $python -ArgumentList "bridge.py" -WorkingDirectory $repoRoot | Out-Null
    Start-Sleep -Milliseconds 900
}

Start-Process $bridgeUrl | Out-Null
Write-Host "Trading terminal opened at $bridgeUrl"
