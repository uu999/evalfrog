$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repoRoot 'deployments\compose.yaml'

docker compose -f $composeFile down
if ($LASTEXITCODE -ne 0) {
    throw "docker compose down failed with exit code $LASTEXITCODE"
}
