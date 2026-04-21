# Healthcare Data Warehouse - DuckDB + Apache Airflow + dbt

Project untuk tugas **Data Warehouse & Business Intelligence** di UGM dengan stack modern:
- **Database**: DuckDB (lightweight, embedded, SQL)
- **Orchestration**: Apache Airflow (local executor dengan PostgreSQL metadata)
- **Transformation**: dbt (data build tool)
- **BI Tool**: Metabase atau Apache Superset (siap untuk integrasi)

---

## 📋 Struktur Project

```
healthcare_dw/
├── docker-compose.yml              # Konfigurasi Docker services
├── generate_dummy_data.py           # Script untuk generate dummy data
├── data/
│   └── raw/                         # CSV files dari source (akan di-generate)
│       ├── STG_EHP__PATN.csv
│       ├── STG_EHP__DPMT.csv
│       ├── STG_EHP__STFF.csv
│       └── ... (13 tabel lainnya)
├── duckdb/
│   └── healthcare.duckdb            # File database DuckDB (akan dibuat otomatis)
├── airflow/
│   └── dags/
│       └── healthcare_pipeline.py   # DAG untuk ELT pipeline
├── dbt/
│   ├── dbt_project.yml              # Konfigurasi dbt project
│   ├── profiles.yml                 # Konfigurasi koneksi DuckDB
│   └── models/
│       ├── staging/                 # Layer 1: Staging (cleanup & standardize)
│       │   ├── sources.yml
│       │   ├── stg_patient.sql
│       │   ├── stg_visit.sql
│       │   ├── stg_diagnosis.sql
│       │   ├── stg_treatment.sql
│       │   ├── stg_billing.sql
│       │   ├── stg_payment.sql
│       │   ├── stg_doctor.sql
│       │   ├── stg_department.sql
│       │   ├── stg_room.sql
│       │   └── stg_medicine.sql
│       ├── intermediate/            # Layer 2: Intermediate (enrich & join)
│       │   ├── int_visit_enriched.sql
│       │   └── int_billing_enriched.sql
│       └── marts/                   # Layer 3: Marts (fact & dimension)
│           ├── schema.yml           # Tests & documentation
│           ├── dim_date.sql
│           ├── dim_patient.sql
│           ├── dim_doctor.sql
│           ├── dim_room.sql
│           ├── dim_department.sql
│           ├── fct_visit.sql
│           └── fct_billing.sql
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites
- **Windows 10/11** dengan Docker Desktop terinstall
- **Python 3.10+**
- **Git**

### Step 1: Buka Windows PowerShell / Command Prompt

Navigasi ke folder project:
```powershell
cd "C:\Users\HP\Documents\Kuliah\s2\semester2\dwib\healthcare\DWIB Healthcare Data Mart preparation\healthcare_dw"
```

### Step 2: Generate Dummy Data

```powershell
python generate_dummy_data.py
```

Expected output:
```
================================================================================
HEALTHCARE DATA WAREHOUSE - DUMMY DATA GENERATOR
================================================================================

[1/16] Generating STG_EHP__PATN (Patients)...
   ✓ Generated 20 patient records
[2/16] Generating STG_EHP__DPMT (Departments)...
   ✓ Generated 20 department records
...
[16/16] Generating STG_EHP__PMNT (Payments)...
   ✓ Generated 100 payment records

================================================================================
SUMMARY - Dummy Data Generation Complete
================================================================================

Table                       Rows
STG_EHP__PATN                 20
STG_EHP__DPMT                 20
...
Total records generated: 1695 rows across 16 tables
================================================================================
```

✅ Verifikasi: Cek folder `data/raw/` - seharusnya ada 16 file CSV

### Step 3: Jalankan Docker Compose

```powershell
docker-compose up -d
```

Wait ~60 detik, kemudian verifikasi:

```powershell
docker ps
```

Output seharusnya menampilkan:
- `healthcare_postgres` (status: healthy)
- `healthcare_airflow_webserver` (status: healthy)
- `healthcare_airflow_scheduler` (status: running)

### Step 4: Akses Airflow UI

Buka browser ke: **http://localhost:8080**

Login dengan:
- **Username**: `admin`
- **Password**: `admin`

Airflow akan menampilkan DAG `healthcare_pipeline_duckdb` di list.

### Step 5: Trigger DAG Manual

1. Cari DAG `healthcare_pipeline_duckdb` di Airflow UI
2. Toggle switch untuk **enable** DAG
3. Klik tombol **play** → **Trigger DAG** → **Trigger**
4. Monitor task execution:

   ```
   validate_source_files      ✓ Success
   ingest_csv_to_duckdb       ✓ Success
   validate_row_counts        ✓ Success
   dbt_run_staging            ✓ Success
   dbt_run_intermediate       ✓ Success
   dbt_run_marts              ✓ Success
   dbt_test_all               ✓ Success
   dbt_docs_generate          ✓ Success
   ```

### Step 6: Verifikasi Data di DuckDB

Buka PowerShell baru (jangan tutup yang pertama) dan jalankan:

```powershell
python
```

Kemudian ketik:

```python
import duckdb

con = duckdb.connect('duckdb/healthcare.duckdb')

# Lihat semua schema
schemas = con.execute("""
    SELECT DISTINCT table_schema 
    FROM information_schema.tables 
    ORDER BY table_schema
""").fetchdf()
print("Schemas:", schemas)

# Lihat tabel di marts
marts_tables = con.execute("""
    SELECT table_name, COUNT(*) as row_count
    FROM information_schema.tables it
    JOIN (
        SELECT table_schema, table_name FROM information_schema.tables
        WHERE table_schema = 'marts'
    ) t USING (table_schema, table_name)
    GROUP BY table_name
    ORDER BY table_name
""").fetchdf()
print("\nMarts Tables:")
print(marts_tables)

# Lihat data sample dari fct_visit
visit_sample = con.execute("""
    SELECT * FROM marts.fct_visit LIMIT 5
""").fetchdf()
print("\nfct_visit sample:")
print(visit_sample.head())

con.close()
```

---

## 📊 Data Model

### Staging Layer (Layer 1)
Raw data dibersihkan dan distandardisasi:
- `stg_patient` - Patient master
- `stg_visit` - Visit records dengan calculated field `length_of_stay_hours`
- `stg_diagnosis` - Diagnosis records
- `stg_treatment` - Treatment records
- `stg_billing` - Union dari BILL dan MDBL
- `stg_payment` - Payment records
- `stg_doctor` - Staff yang role = 'DOC'
- `stg_department` - Department master
- `stg_room` - Room master
- `stg_medicine` - Medicine/drug master

### Intermediate Layer (Layer 2)
Data enriched dengan join dan business logic:
- `int_visit_enriched` - Visit + Patient + Room + Department (dengan length_of_stay_hours)
- `int_billing_enriched` - Billing + Payment + Insurance (dengan outstanding_amount calculation)

### Marts Layer (Layer 3)
Dimensi dan Fakta untuk BI:

**Dimensions:**
- `dim_date` - Calendar 2020-01-01 hingga 2026-12-31
- `dim_patient` - Patient dengan surrogate key
- `dim_doctor` - Doctor/Staff dengan surrogate key
- `dim_room` - Room + Department join
- `dim_department` - Department master

**Facts:**
- `fct_visit` - Fact table untuk visit analytics
  - Measures: `length_of_stay_hours`, `is_inpatient`
  - Keys: patient_key, doctor_key, room_key, department_key, visit_date_key
  
- `fct_billing` - Fact table untuk billing analytics
  - Measures: `billing_amount`, `payment_amount`, `outstanding_amount`
  - Keys: patient_key, billing_date_key

---

## 🔄 Pipeline Architecture

```
CSV Files (data/raw/)
    ↓
TASK 1: validate_source_files
    ↓ (Cek semua 16 CSV ada)
TASK 2: ingest_csv_to_duckdb
    ↓ (Load CSV ke raw schema)
TASK 3: validate_row_counts
    ↓ (Pastikan no empty tables)
TASK 4: dbt_run_staging
    ↓ (Create views di staging schema)
TASK 5: dbt_run_intermediate
    ↓ (Create views di intermediate schema)
TASK 6: dbt_run_marts
    ↓ (Create tables di marts schema)
TASK 7: dbt_test_all
    ↓ (Run dbt tests)
TASK 8: dbt_docs_generate
    ↓ (Generate dbt documentation)
Analytics Ready ✓
```

---

## 📈 Expected Data Volumes

| Table | Rows |
|-------|------|
| STG_EHP__PATN (Patient) | 20 |
| STG_EHP__DPMT (Department) | 20 |
| STG_EHP__STFF (Staff) | 40 |
| STG_EHP__ROMS (Room) | 60 |
| STG_EHP__MDCN (Medicine) | 40 |
| STG_EHP__ALGY (Allergy) | 10 |
| STG_EHP__INSR (Insurance) | 50 |
| STG_EHP__PTAL (Patient Allergy) | ~33 |
| STG_EHP__MEDT (Medical Team) | 50 |
| STG_EHP__VIST (Visit) | 100 |
| STG_EHP__DIAG (Diagnosis) | 100 |
| STG_EHP__TRTM (Treatment) | 100 |
| STG_EHP__TMMD (Treatment Med) | 100 |
| STG_EHP__BILL (Billing) | 100 |
| STG_EHP__MDBL (Medical Billing) | 50 |
| STG_EHP__PMNT (Payment) | 100 |
| **Total** | **~1,695** |

---

## 🐛 Troubleshooting

### Docker tidak jalan
```powershell
# Check Docker Desktop running
docker ps

# Jika error "Docker daemon is not running", buka Docker Desktop app
```

### Airflow UI tidak accessible
```powershell
# Check health
docker-compose ps

# Lihat logs
docker-compose logs airflow-webserver
```

### DAG error
```powershell
# Lihat logs lebih detail
docker-compose exec airflow-webserver airflow dags test healthcare_pipeline_duckdb
```

### dbt connection error
```powershell
# Test connection
docker-compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt debug --profiles-dir ."
```

---

## 📚 References

- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [DuckDB SQL](https://duckdb.org/docs/sql/introduction)
- [Faker Library](https://faker.readthedocs.io/)

---

## 💡 Next Steps (Opsional)

Setelah pipeline berhasil:

1. **Integrasikan Metabase/Superset** untuk visualisasi
   - Connection string: `duckdb:///opt/airflow/duckdb/healthcare.duckdb`

2. **Tambahkan data real** dari Kaggle
   - Update `generate_dummy_data.py` untuk load dari CSV Kaggle
   - Pastikan column names match dengan schema yang ada

3. **Kustomisasi dbt models** untuk business logic spesifik
   - Tambah metrics untuk KPI (e.g., average LOS, billing completion rate)
   - Tambah more_models untuk analisis khusus

4. **Setup scheduled runs** di Airflow
   - Default: `0 1 * * *` (daily 01:00)
   - Sesuaikan dengan data ingestion schedule

---

**Created for**: UGM DWIB Course - Healthcare Data Warehouse  
**Last Updated**: April 2026
