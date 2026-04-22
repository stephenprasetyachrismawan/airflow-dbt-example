# Healthcare DW — Connection Information

## DuckDB (Direct)
- File: `duckdb/healthcare.duckdb`
- SQLAlchemy URI: `duckdb:////absolute/path/to/duckdb/healthcare.duckdb`
- DuckDB CLI: `duckdb duckdb/healthcare.duckdb`

## Apache Superset
- URL: http://localhost:8088
- Username: admin
- Password: admin
- Tambahkan database baru dengan URI: `duckdb:////opt/airflow/duckdb/healthcare.duckdb`

## Airflow UI
- URL: http://localhost:8080
- Username: admin / Password: admin

## dbt Docs
- URL: http://localhost:8081

## Schema Data Mart

| Tabel | Layer | Deskripsi |
|---|---|---|
| `analytics_marts.dim_date` | Dimensi | Dimensi tanggal (2020–2026) |
| `analytics_marts.dim_patient` | Dimensi | Master data pasien |
| `analytics_marts.dim_doctor` | Dimensi | Master data dokter/staf medis |
| `analytics_marts.dim_department` | Dimensi | Master data departemen |
| `analytics_marts.dim_room` | Dimensi | Master data ruangan |
| `analytics_marts.fct_visit` | Fakta | Transaksi kunjungan pasien |
| `analytics_marts.fct_billing` | Fakta | Transaksi billing dan pembayaran |

## Cara Menjalankan Ulang Pipeline

```bash
# Start semua service
docker-compose up -d

# Trigger DAG Airflow
docker exec healthcare_airflow_webserver airflow dags trigger healthcare_pipeline_duckdb

# Jalankan dbt test
docker exec healthcare_airflow_webserver bash -c "cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt test --profiles-dir ."
```

## Cara Menjalankan Scripts Validasi

```bash
# Verifikasi tabel
docker exec healthcare_airflow_webserver python3 /opt/airflow/scripts/check_referential_integrity.py

# Data quality check
docker exec healthcare_airflow_webserver python3 /opt/airflow/scripts/data_quality_check.py

# Business queries
docker exec healthcare_airflow_webserver python3 /opt/airflow/analytics/run_business_queries.py

# Performance benchmark
docker exec healthcare_airflow_webserver python3 /opt/airflow/scripts/query_performance.py

# Final report
docker exec healthcare_airflow_webserver python3 /opt/airflow/scripts/generate_final_report.py
```
