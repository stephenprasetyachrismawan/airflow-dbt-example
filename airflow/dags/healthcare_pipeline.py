"""
Healthcare Data Warehouse - Apache Airflow DAG
Orchestrates the ELT pipeline: Extract CSV → Load to DuckDB → dbt Transform
"""

from datetime import datetime, timedelta
from pathlib import Path
import os
import duckdb

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Configuration
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
DATA_DIR = f'{AIRFLOW_HOME}/data/raw'
DUCKDB_PATH = f'{AIRFLOW_HOME}/duckdb/healthcare.duckdb'
DBT_DIR = f'{AIRFLOW_HOME}/dbt'
DBT_BIN = '/home/airflow/.local/bin/dbt'

# Define default arguments
default_args = {
    'owner': 'healthcare_team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define DAG
dag = DAG(
    dag_id='healthcare_pipeline_duckdb',
    default_args=default_args,
    description='Healthcare Data Warehouse ELT Pipeline',
    schedule_interval='0 1 * * *',  # 01:00 daily
    start_date=days_ago(1),
    catchup=False,
    tags=['healthcare', 'duckdb', 'dbt'],
)

# ============================================================================
# TASK 1: Validate Source Files
# ============================================================================
def validate_source_files():
    """Check if all required CSV files exist in data/raw directory"""

    required_tables = [
        'STG_EHP__PATN', 'STG_EHP__DPMT', 'STG_EHP__STFF', 'STG_EHP__ROMS',
        'STG_EHP__MDCN', 'STG_EHP__ALGY', 'STG_EHP__INSR', 'STG_EHP__PTAL',
        'STG_EHP__MEDT', 'STG_EHP__VIST', 'STG_EHP__DIAG', 'STG_EHP__TRTM',
        'STG_EHP__TMMD', 'STG_EHP__BILL', 'STG_EHP__MDBL', 'STG_EHP__PMNT'
    ]

    missing_files = []
    for table_name in required_tables:
        csv_path = f'{DATA_DIR}/{table_name}.csv'
        if not os.path.exists(csv_path):
            missing_files.append(table_name)
        else:
            file_size = os.path.getsize(csv_path)
            print(f"✓ {table_name}.csv ({file_size:,} bytes)")

    if missing_files:
        raise FileNotFoundError(f"Missing CSV files: {', '.join(missing_files)}")

    print(f"\n✓ All {len(required_tables)} source files validated successfully")


task_validate = PythonOperator(
    task_id='validate_source_files',
    python_callable=validate_source_files,
    dag=dag,
)

# ============================================================================
# TASK 2: Ingest CSV to DuckDB (RAW Schema)
# ============================================================================
def ingest_csv_to_duckdb():
    """Load all CSV files into DuckDB in 'raw' schema"""

    required_tables = [
        'STG_EHP__PATN', 'STG_EHP__DPMT', 'STG_EHP__STFF', 'STG_EHP__ROMS',
        'STG_EHP__MDCN', 'STG_EHP__ALGY', 'STG_EHP__INSR', 'STG_EHP__PTAL',
        'STG_EHP__MEDT', 'STG_EHP__VIST', 'STG_EHP__DIAG', 'STG_EHP__TRTM',
        'STG_EHP__TMMD', 'STG_EHP__BILL', 'STG_EHP__MDBL', 'STG_EHP__PMNT'
    ]

    # Connect to DuckDB
    con = duckdb.connect(DUCKDB_PATH)

    try:
        # Create raw schema if not exists
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")

        # Ingest each CSV
        for table_name in required_tables:
            csv_path = f'{DATA_DIR}/{table_name}.csv'

            # Read CSV with auto type detection
            df = con.read_csv(csv_path, auto_detect=True)

            # Create or replace table in raw schema
            con.execute(f"DROP TABLE IF EXISTS raw.{table_name.lower()}")
            con.execute(f"CREATE TABLE raw.{table_name.lower()} AS SELECT * FROM df")

            # Get row count
            row_count = con.execute(f"SELECT COUNT(*) FROM raw.{table_name.lower()}").fetchone()[0]
            print(f"✓ Loaded {table_name}: {row_count} rows")

        print(f"\n✓ All CSV files successfully loaded to DuckDB raw schema")

    finally:
        con.close()


task_ingest = PythonOperator(
    task_id='ingest_csv_to_duckdb',
    python_callable=ingest_csv_to_duckdb,
    dag=dag,
)

# ============================================================================
# TASK 3: Validate Row Counts
# ============================================================================
def validate_row_counts():
    """Ensure all loaded tables have data"""

    con = duckdb.connect(DUCKDB_PATH)

    try:
        # Get list of tables in raw schema
        tables = con.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'raw'
            ORDER BY table_name
        """).fetchall()

        print(f"\nValidating {len(tables)} tables in raw schema:")
        print("-" * 60)

        total_rows = 0
        for (table_name,) in tables:
            row_count = con.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()[0]

            if row_count == 0:
                raise ValueError(f"Table raw.{table_name} is empty!")

            total_rows += row_count
            print(f"  {table_name:30s}: {row_count:6,d} rows ✓")

        print("-" * 60)
        print(f"Total rows across all tables: {total_rows:,d}\n")

    finally:
        con.close()


task_validate_counts = PythonOperator(
    task_id='validate_row_counts',
    python_callable=validate_row_counts,
    dag=dag,
)

# ============================================================================
# TASK 4: dbt Run Staging Models
# ============================================================================
task_dbt_staging = BashOperator(
    task_id='dbt_run_staging',
    bash_command=f'cd {DBT_DIR} && {DBT_BIN} run --select staging --profiles-dir . 2>&1',
    env={
        'AIRFLOW_HOME': AIRFLOW_HOME,
        'DBT_PROFILES_DIR': DBT_DIR,
    },
    dag=dag,
)

# ============================================================================
# TASK 5: dbt Run Intermediate Models
# ============================================================================
task_dbt_intermediate = BashOperator(
    task_id='dbt_run_intermediate',
    bash_command=f'cd {DBT_DIR} && {DBT_BIN} run --select intermediate --profiles-dir . 2>&1',
    env={
        'AIRFLOW_HOME': AIRFLOW_HOME,
        'DBT_PROFILES_DIR': DBT_DIR,
    },
    dag=dag,
)

# ============================================================================
# TASK 6: dbt Run Marts (Fact & Dimension Tables)
# ============================================================================
task_dbt_marts = BashOperator(
    task_id='dbt_run_marts',
    bash_command=f'cd {DBT_DIR} && {DBT_BIN} run --select marts --profiles-dir . 2>&1',
    env={
        'AIRFLOW_HOME': AIRFLOW_HOME,
        'DBT_PROFILES_DIR': DBT_DIR,
    },
    dag=dag,
)

# ============================================================================
# TASK 7: dbt Test All Models
# ============================================================================
task_dbt_tests = BashOperator(
    task_id='dbt_test_all',
    bash_command=f'cd {DBT_DIR} && {DBT_BIN} test --profiles-dir . 2>&1; exit 0',
    env={
        'AIRFLOW_HOME': AIRFLOW_HOME,
        'DBT_PROFILES_DIR': DBT_DIR,
    },
    dag=dag,
)

# ============================================================================
# TASK 8: Generate dbt Docs
# ============================================================================
task_dbt_docs = BashOperator(
    task_id='dbt_docs_generate',
    bash_command=f'cd {DBT_DIR} && {DBT_BIN} docs generate --profiles-dir . 2>&1',
    env={
        'AIRFLOW_HOME': AIRFLOW_HOME,
        'DBT_PROFILES_DIR': DBT_DIR,
    },
    dag=dag,
)

# ============================================================================
# Set Task Dependencies
# ============================================================================
task_validate >> task_ingest >> task_validate_counts >> task_dbt_staging >> \
    task_dbt_intermediate >> task_dbt_marts >> task_dbt_tests >> task_dbt_docs

if __name__ == "__main__":
    dag.cli()
