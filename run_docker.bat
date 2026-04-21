@echo off
REM Healthcare Data Warehouse - Docker Startup Script
REM This script starts all Docker services and initializes Airflow

echo ===============================================================================
echo HEALTHCARE DATA WAREHOUSE - DOCKER STARTUP
echo ===============================================================================
echo.
echo Starting Docker services...
echo.

REM Start Docker services
docker-compose up -d

if %ERRORLEVEL% neq 0 (
    echo.
    echo ERROR: Failed to start Docker services
    echo Make sure Docker Desktop is running
    pause
    exit /b 1
)

echo.
echo ===============================================================================
echo WAITING FOR SERVICES TO STARTUP (60 seconds)...
echo ===============================================================================
echo.

timeout /t 60 /nobreak

echo.
echo ===============================================================================
echo CHECKING SERVICE STATUS...
echo ===============================================================================
echo.

docker-compose ps

echo.
echo ===============================================================================
echo INSTALLATION COMPLETE
echo ===============================================================================
echo.
echo Services running:
echo  - PostgreSQL (port 5432)
echo  - Airflow WebServer (http://localhost:8080)
echo  - Airflow Scheduler
echo.
echo Next steps:
echo  1. Open http://localhost:8080 in your browser
echo  2. Login with: admin / admin
echo  3. Enable and trigger DAG 'healthcare_pipeline_duckdb'
echo.
echo ===============================================================================
pause
