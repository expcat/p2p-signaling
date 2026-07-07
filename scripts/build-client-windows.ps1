param(
    [ValidateSet("release", "debug")]
    [string]$Mode = "release"
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = (Resolve-Path (Join-Path $ScriptDir "..")).Path
$ClientDir = Join-Path $RootDir "clients"

Push-Location $ClientDir
try {
    if ($Mode -eq "release") {
        cargo build --release -p p2p-gui
        $Binary = Join-Path $ClientDir "target\release\p2p-gui.exe"
    } else {
        cargo build -p p2p-gui
        $Binary = Join-Path $ClientDir "target\debug\p2p-gui.exe"
    }

    Write-Host "Built client: $Binary"
} finally {
    Pop-Location
}
