param(
    [string]$Server = $(if ($env:P2P_SIGNALING_SERVER) { $env:P2P_SIGNALING_SERVER } else { "p2p-signaling.yizhe.studio" }),
    [string]$Room = $(if ($env:P2P_SIGNALING_ROOM) { $env:P2P_SIGNALING_ROOM } else { "" }),
    [ValidateSet("host", "guest")]
    [string]$Role = $(if ($env:P2P_SIGNALING_ROLE) { $env:P2P_SIGNALING_ROLE } else { "host" }),
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

    $Arguments = @("--server", $Server, "--role", $Role)
    if ($Room) {
        $Arguments += @("--room", $Room)
    }

    & $Binary @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Binary exited with code $LASTEXITCODE"
    }
} finally {
    Pop-Location
}
