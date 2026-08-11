$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repoRoot 'deployments\compose.yaml'

docker compose -f $composeFile up -d --build --wait
if ($LASTEXITCODE -ne 0) {
    throw "docker compose up failed with exit code $LASTEXITCODE"
}
docker compose -f $composeFile run --build --rm --no-deps evalfrog-cli doctor --profile local --config-dir /app/configs
if ($LASTEXITCODE -ne 0) {
    throw "evalfrog doctor failed with exit code $LASTEXITCODE"
}

Write-Output 'EvalFrog M0 stack is ready.'
