param(
    [string]$Server = $(if ($env:P2P_SIGNALING_SERVER) { $env:P2P_SIGNALING_SERVER } else { "p2p-signaling.yizhe.studio" }),
    [string]$Room = $(if ($env:P2P_SIGNALING_ROOM) { $env:P2P_SIGNALING_ROOM } else { "LOCALHOST" }),
    [ValidateSet("host", "guest")]
    [string]$Role = $(if ($env:P2P_SIGNALING_ROLE) { $env:P2P_SIGNALING_ROLE } else { "host" }),
    [ValidateSet("release", "debug")]
    [string]$Mode = "release"
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = Resolve-Path (Join-Path $ScriptDir "..")
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

    & $Binary --server $Server --room $Room --role $Role
} finally {
    Pop-Location
}
