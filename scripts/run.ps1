param(
    [ValidateSet("run-all", "generate", "bronze", "silver", "gold")]
    [string]$Command = "run-all",
    [string]$BatchDate = (Get-Date -Format "yyyy-MM-dd")
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker was not found. Install/start Docker Desktop and reopen PowerShell."
}

docker info *> $null
if ($LASTEXITCODE -ne 0) {
    throw "Docker Desktop is not running or the Linux engine is unavailable."
}

docker compose build
if ($LASTEXITCODE -ne 0) {
    throw "Docker image build failed. The pipeline was not started."
}

docker compose run --rm pipeline $Command --batch-date $BatchDate
if ($LASTEXITCODE -ne 0) {
    throw "The medallion pipeline failed. Review the first Spark/Python error above."
}

Write-Host ""
Write-Host "Nervix-style one-go check complete: $Command finished successfully." -ForegroundColor Green
Write-Host "Generated data: data\landing and data\lakehouse" -ForegroundColor Cyan
Write-Host "Quality reports: data\quality" -ForegroundColor Cyan
