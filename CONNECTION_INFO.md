# Healthcare DW — Informasi Koneksi

## DuckDB (Direct)
- File: `duckdb/healthcare.duckdb`
- SQLAlchemy URI: `duckdb:////absolute/path/to/duckdb/healthcare.duckdb`
- DuckDB CLI: `duckdb duckdb/healthcare.duckdb`

## Apache Superset
- URL: http://localhost:8088
- Username: admin
- Password: admin
- Tambahkan database baru dengan URI: `duckdb:////opt/airflow/duckdb/healthcare.duckdb`
- Package yang dibutuhkan: `duckdb-engine`

## Airflow UI
- URL: http://localhost:8080
- Username: admin / Password: admin

## dbt Docs
- URL: http://localhost:8081

## Schema Data Mart (`analytics_marts`)

| Tabel | Layer | SCD | Deskripsi |
|---|---|---|---|
| `analytics_marts.dim_tanggal` | Dimensi | Type 0 | Kalender 2020–2027 (role-playing dimension) |
| `analytics_marts.dim_pasien` | Dimensi | Type 2 | Master data pasien (di-enrich dari INSR) |
| `analytics_marts.dim_dokter` | Dimensi | Type 2 | Master data dokter (hanya ROLE_CD='0') |
| `analytics_marts.dim_departemen` | Dimensi | Type 1 | Master data departemen (conformed) |
| `analytics_marts.dim_ruangan` | Dimensi | Type 1 | Master data ruangan |
| `analytics_marts.fct_kunjungan` | Fakta | — | Transaksi kunjungan pasien (grain: 1 kunjungan) |
| `analytics_marts.fct_diagnosis` | Fakta | — | Transaksi diagnosis pasien (grain: 1 diagnosis/kunjungan) |
| `analytics_marts.fct_tindakan_medis` | Fakta | — | Transaksi tindakan medis (grain: 1 tindakan/kunjungan) |

### Unknown Member
Setiap dimensi memiliki baris dengan surrogate key = **-1** sebagai "unknown member"
untuk menampung FK yang tidak memiliki pasangan di dimensi.

### Dataset Sumber
Dataset asli dari Kaggle: `moid1234/health-care-data-set-20-tables`
- Lokasi: `data/archive/STG_EHP_DATASET/`
- Total: 5,5 juta+ baris di 20 tabel sumber
- Tabel in-scope: VIST, DIAG, TRTM, APPT, PATN, STFF, DPMT, ROMS, MEDT, INSR

## Cara Menjalankan Ulang Pipeline

```bash
# Start semua service
docker-compose up -d

# Trigger DAG Airflow
docker exec healthcare_airflow_webserver airflow dags trigger healthcare_pipeline_duckdb

# Jalankan dbt test
docker exec healthcare_airflow_webserver bash -c \
  "cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt test --profiles-dir ."
```

## Cara Menjalankan Scripts Validasi

```bash
# Cek referential integrity (16 FK checks)
docker exec healthcare_airflow_webserver python3 /opt/airflow/scripts/check_referential_integrity.py

# Data quality check (completeness, uniqueness, validity, distribusi)
docker exec healthcare_airflow_webserver python3 /opt/airflow/scripts/data_quality_check.py

# Jalankan 5 business queries (BQ1-BQ5)
docker exec healthcare_airflow_webserver python3 /opt/airflow/analytics/run_business_queries.py

# Performance benchmark sebelum dan sesudah indexing
docker exec healthcare_airflow_webserver python3 /opt/airflow/scripts/query_performance.py

# Final validation report
docker exec healthcare_airflow_webserver python3 /opt/airflow/scripts/generate_final_report.py

# Interactive data mart viewer
docker exec -it healthcare_airflow_webserver python3 /opt/airflow/view_datamart.py
```
