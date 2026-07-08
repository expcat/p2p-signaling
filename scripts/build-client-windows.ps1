param(
    [ValidateSet("release", "debug")]
    [string]$Mode = "release"
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = (Resolve-Path (Join-Path $ScriptDir "..")).Path
$ClientDir = Join-Path $RootDir "clients"

. (Join-Path $ScriptDir "windows-client-common.ps1")

Push-Location $ClientDir
try {
    if ($Mode -eq "release") {
        Invoke-CargoBuild -CargoArguments @("build", "--release", "-p", "p2p-gui")
        $Binary = Join-Path $ClientDir "target\release\p2p-gui.exe"
    } else {
        Invoke-CargoBuild -CargoArguments @("build", "-p", "p2p-gui")
        $Binary = Join-Path $ClientDir "target\debug\p2p-gui.exe"
    }

    Write-Host "Built client: $Binary"
} finally {
    Pop-Location
}
