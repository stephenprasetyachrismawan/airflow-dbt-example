# Healthcare Data Warehouse - Complete Setup & Execution Script
# PowerShell script to setup and run the entire healthcare DW pipeline

Write-Host "===============================================================================" -ForegroundColor Cyan
Write-Host "HEALTHCARE DATA WAREHOUSE - COMPLETE SETUP & EXECUTION" -ForegroundColor Cyan
Write-Host "===============================================================================" -ForegroundColor Cyan
Write-Host ""

# Get script directory
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

Write-Host "Current directory: $scriptDir" -ForegroundColor Yellow
Write-Host ""

# STEP 1: Check Kaggle environment and archive directory
Write-Host "===============================================================================" -ForegroundColor Green
Write-Host "STEP 1: Checking Kaggle configuration..." -ForegroundColor Green
Write-Host "===============================================================================" -ForegroundColor Green

$archiveDir = "data/archive/STG_EHP_DATASET"
if (-not (Test-Path $archiveDir)) {
    New-Item -ItemType Directory -Path $archiveDir -Force | Out-Null
    Write-Host "Created archive directory: $archiveDir" -ForegroundColor Yellow
}

if ([string]::IsNullOrWhiteSpace($env:KAGGLE_USERNAME) -or [string]::IsNullOrWhiteSpace($env:KAGGLE_KEY)) {
    Write-Host "WARNING - KAGGLE_USERNAME / KAGGLE_KEY belum di-set." -ForegroundColor Yellow
    Write-Host "Pipeline tetap bisa jalan jika file archive sudah ada lokal." -ForegroundColor Yellow
} else {
    Write-Host "OK - Kaggle credentials terdeteksi" -ForegroundColor Green
}

Write-Host ""
Write-Host "Archive Files (if any):" -ForegroundColor Cyan
Get-ChildItem "$archiveDir/**/*.csv" -ErrorAction SilentlyContinue | ForEach-Object {
    $size = [math]::Round($_.Length / 1KB, 2)
    Write-Host "  - $($_.Name) ($size KB)" -ForegroundColor Green
}

# STEP 2: Check Docker
Write-Host ""
Write-Host "===============================================================================" -ForegroundColor Green
Write-Host "STEP 2: Checking Docker..." -ForegroundColor Green
Write-Host "===============================================================================" -ForegroundColor Green

$dockerVersion = docker --version
if ($LASTEXITCODE -eq 0) {
    Write-Host "OK - Docker is installed" -ForegroundColor Green
    Write-Host "  $dockerVersion" -ForegroundColor Cyan
} else {
    Write-Host "ERROR: Docker is not installed or not running" -ForegroundColor Red
    Write-Host "Please install Docker Desktop from https://www.docker.com/products/docker-desktop" -ForegroundColor Yellow
    exit 1
}

# STEP 3: Start Docker services
Write-Host ""
Write-Host "===============================================================================" -ForegroundColor Green
Write-Host "STEP 3: Starting Docker services..." -ForegroundColor Green
Write-Host "===============================================================================" -ForegroundColor Green

Write-Host "Running: docker-compose up -d" -ForegroundColor Cyan
docker-compose up -d

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to start Docker services" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Waiting 60 seconds for services to startup..." -ForegroundColor Yellow
Start-Sleep -Seconds 60

# STEP 4: Check service status
Write-Host ""
Write-Host "===============================================================================" -ForegroundColor Green
Write-Host "STEP 4: Checking service status..." -ForegroundColor Green
Write-Host "===============================================================================" -ForegroundColor Green

Write-Host ""
docker-compose ps

Write-Host ""
Write-Host "===============================================================================" -ForegroundColor Green
Write-Host "SETUP COMPLETE!" -ForegroundColor Green
Write-Host "===============================================================================" -ForegroundColor Green

Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Open http://localhost:8080 in your browser" -ForegroundColor Cyan
Write-Host "  2. Login with credentials: admin / admin" -ForegroundColor Cyan
Write-Host "  3. Find DAG 'healthcare_pipeline_duckdb'" -ForegroundColor Cyan
Write-Host "  4. Toggle the switch to ENABLE the DAG" -ForegroundColor Cyan
Write-Host "  5. Click play icon and TRIGGER the DAG" -ForegroundColor Cyan
Write-Host "  6. Monitor task execution (should take ~40 seconds)" -ForegroundColor Cyan
Write-Host ""
Write-Host "Services running:" -ForegroundColor Green
Write-Host "  - PostgreSQL (port 5432)" -ForegroundColor Cyan
Write-Host "  - Airflow WebServer (http://localhost:8080)" -ForegroundColor Cyan
Write-Host "  - Airflow Scheduler" -ForegroundColor Cyan
Write-Host ""
Write-Host "Database:" -ForegroundColor Green
Write-Host "  - DuckDB: duckdb/healthcare.duckdb" -ForegroundColor Cyan
Write-Host ""
Write-Host "===============================================================================" -ForegroundColor Cyan
