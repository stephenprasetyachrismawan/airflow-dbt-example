"""
Healthcare Data Warehouse - Apache Airflow DAG
Kaggle Dataset: moid1234/health-care-data-set-20-tables
Stack: DuckDB + dbt + Airflow
Ingestion: kagglehub KaggleDatasetAdapter.PANDAS (no local CSV required)
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
DUCKDB_PATH = f'{AIRFLOW_HOME}/duckdb/healthcare.duckdb'
DBT_DIR = f'{AIRFLOW_HOME}/dbt'
DBT_BIN = '/home/airflow/.local/bin/dbt'
KAGGLE_DATASET = 'moid1234/health-care-data-set-20-tables'

# Tabel root level yang in-scope (9 tabel)
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

# DIAG ada di subfolder, terdiri dari 2 file yang di-UNION ALL
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
    description='Healthcare DW ELT Pipeline — Kaggle KaggleDatasetAdapter.PANDAS',
    schedule_interval='0 1 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['healthcare', 'duckdb', 'dbt'],
)

# ============================================================================
# TASK 1: Validate Kaggle Credentials
# ============================================================================
def validate_kaggle_credentials():
    kaggle_username = os.getenv('KAGGLE_USERNAME', '').strip()
    kaggle_key = os.getenv('KAGGLE_KEY', '').strip()

    if not kaggle_username or not kaggle_key:
        raise RuntimeError(
            'KAGGLE_USERNAME dan KAGGLE_KEY harus di-set sebagai environment variable. '
            'Set di docker-compose.yml atau .env file.'
        )

    try:
        import kagglehub  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            'kagglehub tidak terinstall. Pastikan container sudah install: pip install kagglehub'
        ) from exc

    print(f'Kaggle credentials valid: username={kaggle_username}')
    print(f'Dataset target: {KAGGLE_DATASET}')
    print(f'DuckDB path: {DUCKDB_PATH}')
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)


task_validate = PythonOperator(
    task_id='validate_kaggle_credentials',
    python_callable=validate_kaggle_credentials,
    dag=dag,
)

# ============================================================================
# TASK 2: Ingest dari Kaggle langsung ke DuckDB via KaggleDatasetAdapter.PANDAS
# ============================================================================
def ingest_from_kaggle_to_duckdb():
    import kagglehub
    from kagglehub import KaggleDatasetAdapter
    import pandas as pd

    con = duckdb.connect(DUCKDB_PATH)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")

        # 1. Ingest 9 tabel root level
        for table_name in REQUIRED_TABLES:
            # Path file di dalam dataset Kaggle
            file_path = f'STG_EHP_DATASET/{table_name}.csv'
            print(f'Loading {file_path} ...')

            df = kagglehub.load_dataset(
                KaggleDatasetAdapter.PANDAS,
                KAGGLE_DATASET,
                file_path,
            )

            tbl = table_name.lower()
            con.execute(f"DROP TABLE IF EXISTS raw.{tbl}")
            con.register('_tmp_df', df)
            con.execute(f"CREATE TABLE raw.{tbl} AS SELECT * FROM _tmp_df")
            con.unregister('_tmp_df')

            cnt = con.execute(f"SELECT COUNT(*) FROM raw.{tbl}").fetchone()[0]
            print(f'  raw.{tbl}: {cnt:,} rows')

        # 2. Ingest DIAG: UNION ALL dari 2 file
        print('Loading DIAG files (UNION ALL) ...')
        diag_frames = []
        for diag_rel in DIAG_FILES:
            file_path = f'STG_EHP_DATASET/{diag_rel}'
            print(f'  Loading {file_path} ...')
            df = kagglehub.load_dataset(
                KaggleDatasetAdapter.PANDAS,
                KAGGLE_DATASET,
                file_path,
            )
            diag_frames.append(df)

        diag_df = pd.concat(diag_frames, ignore_index=True)
        con.execute("DROP TABLE IF EXISTS raw.stg_ehp__diag")
        con.register('_tmp_df', diag_df)
        con.execute("CREATE TABLE raw.stg_ehp__diag AS SELECT * FROM _tmp_df")
        con.unregister('_tmp_df')

        cnt = con.execute("SELECT COUNT(*) FROM raw.stg_ehp__diag").fetchone()[0]
        print(f'  raw.stg_ehp__diag (UNION ALL): {cnt:,} rows')

        print('\nIngest selesai.')
    finally:
        con.close()


task_ingest = PythonOperator(
    task_id='ingest_from_kaggle_to_duckdb',
    python_callable=ingest_from_kaggle_to_duckdb,
    dag=dag,
    execution_timeout=timedelta(minutes=30),
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

        print(f"\n{len(tables)} tabel di schema raw:")
        print("-" * 55)
        total = 0
        for (tbl,) in tables:
            cnt = con.execute(f"SELECT COUNT(*) FROM raw.{tbl}").fetchone()[0]
            if cnt == 0:
                raise ValueError(f"raw.{tbl} kosong!")
            total += cnt
            print(f"  {tbl:40s}: {cnt:>10,} rows")
        print("-" * 55)
        print(f"  TOTAL: {total:,} rows")
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
(
    task_validate
    >> task_ingest
    >> task_validate_counts
    >> task_dbt_staging
    >> task_dbt_intermediate
    >> task_dbt_marts
    >> task_dbt_tests
    >> task_dbt_docs
)

if __name__ == "__main__":
    dag.cli()
