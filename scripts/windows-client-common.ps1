function Test-ApplicationControlBlock {
    param(
        [Parameter(Mandatory = $true)]
        [string]$LogPath
    )

    if (-not (Test-Path -LiteralPath $LogPath)) {
        return $false
    }

    $LogText = Get-Content -LiteralPath $LogPath -Raw
    return $LogText -match "os error 4551" -or
        $LogText -match "Application control policy" -or
        $LogText -match "应用程序控制策略"
}

function Show-ApplicationControlTrustPrompt {
    Write-Host ""
    Write-Host "Windows blocked a Rust build helper executable." -ForegroundColor Yellow
    Write-Host "If Windows Security shows a prompt or notification, choose Trust, Allow, or Run anyway."
    Write-Host "Then return here and press Enter to retry the build."
    Write-Host ""
    Write-Host "If no prompt appears, open Windows Security > App & browser control > Protection history,"
    Write-Host "trust or allow the blocked Cargo build helper, then rerun this script."
    Write-Host ""
}

function Invoke-CargoBuild {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$CargoArguments
    )

    $CommandText = "cargo " + ($CargoArguments -join " ")

    for ($Attempt = 0; $Attempt -lt 2; $Attempt++) {
        $LogPath = Join-Path ([System.IO.Path]::GetTempPath()) ("p2p-signaling-cargo-{0}.log" -f ([System.Guid]::NewGuid()))
        try {
            & cargo @CargoArguments 2>&1 | Tee-Object -FilePath $LogPath
            $ExitCode = $LASTEXITCODE

            if ($ExitCode -eq 0) {
                return
            }

            if ((Test-ApplicationControlBlock -LogPath $LogPath) -and $Attempt -eq 0 -and [Environment]::UserInteractive) {
                Show-ApplicationControlTrustPrompt
                $Response = Read-Host "Press Enter after trusting the blocked file, or type q to quit"
                if ($Response -eq "q") {
                    throw "$CommandText failed with exit code $ExitCode because Windows application control blocked a Cargo build helper."
                }
                continue
            }

            if (Test-ApplicationControlBlock -LogPath $LogPath) {
                throw "$CommandText failed with exit code $ExitCode because Windows application control blocked a Cargo build helper."
            }

            throw "$CommandText failed with exit code $ExitCode"
        } finally {
            Remove-Item -LiteralPath $LogPath -Force -ErrorAction SilentlyContinue
        }
    }
}
