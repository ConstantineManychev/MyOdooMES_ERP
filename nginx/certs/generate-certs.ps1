param(
    [int]$CaValidityDays = 3650,
    [int]$ServerValidityDays = 825
)

$ErrorActionPreference = "Stop"
$certDir = $PSScriptRoot
$scriptName = "_generate.sh"
$scriptPath = Join-Path $certDir $scriptName

Write-Host "Generating internal CA and server certificate in $certDir"

$opensslScript = @"
set -e
cd /certs

if [ ! -f ca.key ]; then
    openssl req -x509 -nodes -new -newkey rsa:4096 -sha256 \
        -days $CaValidityDays -config ca.cnf \
        -keyout ca.key -out ca.crt
    echo 'Root CA created: ca.crt'
else
    echo 'Root CA already present, reusing ca.key/ca.crt'
fi

openssl req -nodes -new -newkey rsa:2048 -sha256 \
    -config server.cnf \
    -keyout nginx-selfsigned.key -out server.csr

openssl x509 -req -in server.csr \
    -CA ca.crt -CAkey ca.key -CAcreateserial \
    -days $ServerValidityDays -sha256 \
    -extfile server.cnf -extensions v3_server \
    -out nginx-selfsigned.crt

rm -f server.csr
echo 'Server certificate created: nginx-selfsigned.crt'
openssl x509 -in nginx-selfsigned.crt -noout -subject -dates -ext subjectAltName
"@

$lfScript = $opensslScript -replace "`r`n", "`n"
[System.IO.File]::WriteAllText($scriptPath, $lfScript, (New-Object System.Text.UTF8Encoding $false))

$dockerExit = 1
try {
    docker run --rm --entrypoint /bin/sh -v "${certDir}:/certs" alpine/openssl:latest "/certs/$scriptName"
    $dockerExit = $LASTEXITCODE
}
finally {
    Remove-Item $scriptPath -ErrorAction SilentlyContinue
}

if ($dockerExit -ne 0) {
    throw "Certificate generation failed"
}

Write-Host ""
Write-Host "Done. Next steps:"
Write-Host "  1. docker compose restart nginx"
Write-Host "  2. Install ca.crt on client machines: .\install-ca.ps1"
