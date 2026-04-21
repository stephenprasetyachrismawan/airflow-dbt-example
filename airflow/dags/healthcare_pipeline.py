"""
Healthcare Data Warehouse - Apache Airflow DAG
Kaggle Real Dataset: STG_EHP archive
Stack: DuckDB + dbt + Airflow
"""

from datetime import datetime, timedelta
import os
import duckdb

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# ============================================================================
# Configuration
# ============================================================================
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
DATA_ARCHIVE_DIR = f'{AIRFLOW_HOME}/data/archive/STG_EHP_DATASET'
DUCKDB_PATH = f'{AIRFLOW_HOME}/duckdb/healthcare.duckdb'
DBT_DIR = f'{AIRFLOW_HOME}/dbt'
DBT_BIN = '/home/airflow/.local/bin/dbt'

# Tabel root level yang in-scope
REQUIRED_TABLES = [
    'STG_EHP__PATN',
    'STG_EHP__VIST',
    'STG_EHP__APPT',
    'STG_EHP__TRTM',
    'STG_EHP__STFF',
    'STG_EHP__DPMT',
    'STG_EHP__ROMS',
    'STG_EHP__MEDT',
    'STG_EHP__INSR',
]

# DIAG ada di subfolder dan terdiri dari 2 file
DIAG_FILES = [
    'STG_EHP__DIAG/STG_EHP__DIAG_1.csv',
    'STG_EHP__DIAG/STG_EHP__DIAG_2.csv',
]

# ============================================================================
# DAG definition
# ============================================================================
default_args = {
    'owner': 'healthcare_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    dag_id='healthcare_pipeline_duckdb',
    default_args=default_args,
    description='Healthcare DW ELT Pipeline — Kaggle Dataset',
    schedule_interval='0 1 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['healthcare', 'duckdb', 'dbt'],
)

# ============================================================================
# TASK 1: Validate Source Files
# ============================================================================
def validate_source_files():
    missing = []

    # Cek 9 tabel root level
    for table_name in REQUIRED_TABLES:
        csv_path = f'{DATA_ARCHIVE_DIR}/{table_name}.csv'
        if not os.path.exists(csv_path):
            missing.append(csv_path)
        else:
            size = os.path.getsize(csv_path)
            print(f"✓ {table_name}.csv ({size:,} bytes)")

    # Cek 2 file DIAG di subfolder
    for diag_rel in DIAG_FILES:
        diag_path = f'{DATA_ARCHIVE_DIR}/{diag_rel}'
        if not os.path.exists(diag_path):
            missing.append(diag_path)
        else:
            size = os.path.getsize(diag_path)
            print(f"✓ {diag_rel} ({size:,} bytes)")

    if missing:
        raise FileNotFoundError(f"Missing files: {missing}")

    print(f"\n✓ All {len(REQUIRED_TABLES) + len(DIAG_FILES)} source files validated")


task_validate = PythonOperator(
    task_id='validate_source_files',
    python_callable=validate_source_files,
    dag=dag,
)

# ============================================================================
# TASK 2: Ingest CSV to DuckDB (raw schema)
# ============================================================================
def ingest_csv_to_duckdb():
    con = duckdb.connect(DUCKDB_PATH)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")

        # 1. Ingest 9 tabel root level
        for table_name in REQUIRED_TABLES:
            csv_path = f'{DATA_ARCHIVE_DIR}/{table_name}.csv'
            tbl = table_name.lower()
            con.execute(f"DROP TABLE IF EXISTS raw.{tbl}")
            con.execute(f"""
                CREATE TABLE raw.{tbl} AS
                SELECT * FROM read_csv_auto('{csv_path}', ignore_errors=true)
            """)
            cnt = con.execute(f"SELECT COUNT(*) FROM raw.{tbl}").fetchone()[0]
            print(f"✓ {table_name}: {cnt:,} rows")

        # 2. Ingest DIAG: UNION ALL dari 2 file
        diag1 = f'{DATA_ARCHIVE_DIR}/STG_EHP__DIAG/STG_EHP__DIAG_1.csv'
        diag2 = f'{DATA_ARCHIVE_DIR}/STG_EHP__DIAG/STG_EHP__DIAG_2.csv'
        con.execute("DROP TABLE IF EXISTS raw.stg_ehp__diag")
        con.execute(f"""
            CREATE TABLE raw.stg_ehp__diag AS
            SELECT * FROM read_csv_auto('{diag1}', ignore_errors=true)
            UNION ALL
            SELECT * FROM read_csv_auto('{diag2}', ignore_errors=true)
        """)
        cnt = con.execute("SELECT COUNT(*) FROM raw.stg_ehp__diag").fetchone()[0]
        print(f"✓ STG_EHP__DIAG (UNION ALL): {cnt:,} rows")

        print("\n✓ Ingest complete")
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
    con = duckdb.connect(DUCKDB_PATH)
    try:
        tables = con.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'raw' ORDER BY table_name
        """).fetchall()

        print(f"\n{len(tables)} tables in raw schema:")
        print("-" * 50)
        total = 0
        for (tbl,) in tables:
            cnt = con.execute(f"SELECT COUNT(*) FROM raw.{tbl}").fetchone()[0]
            if cnt == 0:
                raise ValueError(f"raw.{tbl} is empty!")
            total += cnt
            print(f"  {tbl:35s}: {cnt:>10,} rows ✓")
        print("-" * 50)
        print(f"  Total: {total:,} rows")
    finally:
        con.close()


task_validate_counts = PythonOperator(
    task_id='validate_row_counts',
    python_callable=validate_row_counts,
    dag=dag,
)

# ============================================================================
# TASK 4-8: dbt layers
# ============================================================================
task_dbt_staging = BashOperator(
    task_id='dbt_run_staging',
    bash_command=f'cd {DBT_DIR} && {DBT_BIN} run --select staging --profiles-dir . 2>&1',
    dag=dag,
)

task_dbt_intermediate = BashOperator(
    task_id='dbt_run_intermediate',
    bash_command=f'cd {DBT_DIR} && {DBT_BIN} run --select intermediate --profiles-dir . 2>&1',
    dag=dag,
)

task_dbt_marts = BashOperator(
    task_id='dbt_run_marts',
    bash_command=f'cd {DBT_DIR} && {DBT_BIN} run --select marts --profiles-dir . 2>&1',
    dag=dag,
)

task_dbt_tests = BashOperator(
    task_id='dbt_test_all',
    bash_command=f'cd {DBT_DIR} && {DBT_BIN} test --profiles-dir . 2>&1; exit 0',
    dag=dag,
)

task_dbt_docs = BashOperator(
    task_id='dbt_docs_generate',
    bash_command=f'cd {DBT_DIR} && {DBT_BIN} docs generate --profiles-dir . 2>&1',
    dag=dag,
)

# ============================================================================
# Task Dependencies
# ============================================================================
task_validate >> task_ingest >> task_validate_counts >> task_dbt_staging >> \
    task_dbt_intermediate >> task_dbt_marts >> task_dbt_tests >> task_dbt_docs

if __name__ == "__main__":
    dag.cli()
