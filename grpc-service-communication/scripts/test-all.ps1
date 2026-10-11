$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
Write-Host 'Running gRPC broker test suite (unit + reliability + integration, when present)...'
mvn --batch-mode clean test
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }