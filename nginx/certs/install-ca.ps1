param(
    [string]$CaPath = "$PSScriptRoot\ca.crt",
    [switch]$CurrentUserOnly
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $CaPath)) {
    throw "CA certificate not found at $CaPath"
}

$storeScope = if ($CurrentUserOnly) { "CurrentUser" } else { "LocalMachine" }

if (-not $CurrentUserOnly) {
    $isAdmin = ([Security.Principal.WindowsPrincipal] `
        [Security.Principal.WindowsIdentity]::GetCurrent()
    ).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
    if (-not $isAdmin) {
        throw "Run this script as Administrator, or pass -CurrentUserOnly"
    }
}

Import-Certificate -FilePath $CaPath `
    -CertStoreLocation "Cert:\$storeScope\Root" | Out-Null

Write-Host "Abellio internal root CA installed into $storeScope\Root"
Write-Host "Chrome and Edge trust it immediately. Restart the browser if a tab is already open."
Write-Host ""
Write-Host "Firefox keeps its own store. Either enable"
Write-Host "  security.enterprise_roots.enabled = true  in about:config,"
Write-Host "or import ca.crt via Settings > Privacy & Security > Certificates > Authorities."
