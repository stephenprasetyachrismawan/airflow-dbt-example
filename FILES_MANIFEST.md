# Files Manifest - Healthcare DW Project

Dokumentasi lengkap semua file yang sudah dibuat dan siap digunakan.

---

## 📋 Overview

**Total Files Created**: 33 files  
**Location**: `C:\Users\HP\Documents\Kuliah\s2\semester2\dwib\healthcare\DWIB Healthcare Data Mart preparation\healthcare_dw\`

---

## 📁 File Structure & Details

### 🔧 Root Configuration Files

| File | Purpose | Status |
|------|---------|--------|
| `docker-compose.yml` | Docker services config (Postgres, Airflow Web, Airflow Scheduler) | ✅ Ready |
| `.gitignore` | Git ignore patterns | ✅ Ready |
| `generate_dummy_data.py` | Script untuk generate 16 tabel dengan realistic data (Faker) | ✅ Ready |
| `README.md` | Project documentation lengkap | ✅ Ready |
| `SETUP_CHECKLIST.md` | Step-by-step setup guide dengan checklist | ✅ Ready |
| `FILES_MANIFEST.md` | File ini - dokumentasi manifest | ✅ Ready |

---

### 📂 Airflow Configuration

#### `airflow/dags/healthcare_pipeline.py`
**Purpose**: Main DAG untuk orchestration ELT pipeline

**Tasks** (8 sequential tasks):
1. `validate_source_files` - Validasi semua CSV ada
2. `ingest_csv_to_duckdb` - Load CSV ke DuckDB raw schema
3. `validate_row_counts` - Pastikan no empty tables
4. `dbt_run_staging` - Run dbt staging models
5. `dbt_run_intermediate` - Run dbt intermediate models
6. `dbt_run_marts` - Run dbt mart models
7. `dbt_test_all` - Run dbt data quality tests
8. `dbt_docs_generate` - Generate dbt documentation

**Schedule**: `0 1 * * *` (daily at 01:00)  
**Retries**: 2 dengan 5 menit delay  
**Status**: ✅ Ready

---

### 🔗 dbt Configuration

#### `dbt/dbt_project.yml`
**Purpose**: Main dbt project configuration

**Key Settings**:
- Project name: `healthcare_dw`
- Version: 1.0.0
- Materialization:
  - Staging: `view`
  - Intermediate: `view`
  - Marts: `table`
- Schema:
  - Staging: `staging`
  - Intermediate: `intermediate`
  - Marts: `marts`

**Status**: ✅ Ready

#### `dbt/profiles.yml`
**Purpose**: Database connection config

**Connection**:
- Type: `duckdb`
- Path: `/opt/airflow/duckdb/healthcare.duckdb`
- Schema: `analytics`
- Threads: 4

**Status**: ✅ Ready

---

### 📊 dbt Models - Staging Layer

#### Directory: `dbt/models/staging/`

| File | Source Table | Description |
|------|--------------|-------------|
| `sources.yml` | N/A (metadata) | Mendefinisikan 16 raw tables sebagai sources untuk dbt |
| `stg_patient.sql` | raw.stg_ehp__patn | Patient master dengan col standardization |
| `stg_visit.sql` | raw.stg_ehp__vist | Visit records + calculated `length_of_stay_hours` |
| `stg_diagnosis.sql` | raw.stg_ehp__diag | Diagnosis records dengan col rename |
| `stg_treatment.sql` | raw.stg_ehp__trtm | Treatment records dengan col rename |
| `stg_billing.sql` | raw.stg_ehp__bill + raw.stg_ehp__mdbl | Union dari 2 tabel billing |
| `stg_payment.sql` | raw.stg_ehp__pmnt | Payment records dengan col rename |
| `stg_doctor.sql` | raw.stg_ehp__stff | Staff filtered untuk role='DOC' only |
| `stg_department.sql` | raw.stg_ehp__dpmt | Department master dengan col rename |
| `stg_room.sql` | raw.stg_ehp__roms | Room master dengan col rename |
| `stg_medicine.sql` | raw.stg_ehp__mdcn | Medicine master dengan col rename |

**Materialization**: `view` (lightweight, transformasi on-the-fly)  
**Status**: ✅ Ready (11 files)

---

### 📊 dbt Models - Intermediate Layer

#### Directory: `dbt/models/intermediate/`

| File | Input Models | Purpose |
|------|--------------|---------|
| `int_visit_enriched.sql` | stg_visit + stg_patient + stg_room + stg_department | Join visit dengan dimensi, hitung LOS |
| `int_billing_enriched.sql` | stg_billing + stg_payment + stg_patient | Join billing & payment, hitung outstanding |

**Materialization**: `view` (lightweight, reusable logic)  
**Status**: ✅ Ready (2 files)

---

### 📊 dbt Models - Marts Layer (Facts & Dimensions)

#### Directory: `dbt/models/marts/`

##### Dimension Models:

| File | Description | Rows Expected |
|------|-------------|----------------|
| `dim_date.sql` | Calendar dimension (2020-01-01 to 2026-12-31) | ~2,557 |
| `dim_patient.sql` | Patient dimension dengan surrogate key | 20 |
| `dim_doctor.sql` | Doctor dimension dengan surrogate key | 40 |
| `dim_room.sql` | Room dimension joined dengan department | 60 |
| `dim_department.sql` | Department dimension | 20 |

##### Fact Models:

| File | Description | Keys | Measures |
|------|-------------|------|----------|
| `fct_visit.sql` | Visit fact table | patient, doctor, room, dept, date | length_of_stay_hours, is_inpatient |
| `fct_billing.sql` | Billing fact table | patient, date | billing_amount, payment_amount, outstanding |

##### Schema Documentation:

| File | Purpose |
|------|---------|
| `schema.yml` | Tests & documentation untuk semua mart models |

**Materialization**: `table` (persistent, optimized for query)  
**Status**: ✅ Ready (8 files)

---

### 📁 Data Directories

#### `data/raw/`
**Purpose**: Storage untuk CSV files dari dummy data generator

**Expected Files** (16 tables):
1. `STG_EHP__PATN.csv` - Patient master (20 rows)
2. `STG_EHP__DPMT.csv` - Department (20 rows)
3. `STG_EHP__STFF.csv` - Staff (40 rows)
4. `STG_EHP__ROMS.csv` - Room (60 rows)
5. `STG_EHP__MDCN.csv` - Medicine (40 rows)
6. `STG_EHP__ALGY.csv` - Allergy (10 rows)
7. `STG_EHP__INSR.csv` - Insurance (50 rows)
8. `STG_EHP__PTAL.csv` - Patient Allergy (33 rows)
9. `STG_EHP__MEDT.csv` - Medical Team (50 rows)
10. `STG_EHP__VIST.csv` - Visit (100 rows)
11. `STG_EHP__DIAG.csv` - Diagnosis (100 rows)
12. `STG_EHP__TRTM.csv` - Treatment (100 rows)
13. `STG_EHP__TMMD.csv` - Treatment Medicine (100 rows)
14. `STG_EHP__BILL.csv` - Billing (100 rows)
15. `STG_EHP__MDBL.csv` - Medical Billing (50 rows)
16. `STG_EHP__PMNT.csv` - Payment (100 rows)

**Total Expected Data**: ~1,695 rows across 16 tables  
**Status**: 📋 Will be generated by `python generate_dummy_data.py`

#### `duckdb/`
**Purpose**: Storage untuk DuckDB database file

**Expected File**:
- `healthcare.duckdb` - Main database file (akan dibuat otomatis oleh DAG)
- `healthcare.duckdb.wal` - Write-ahead log (auto-created)

**Status**: 📋 Will be created by DAG task `ingest_csv_to_duckdb`

---

## 🔄 Data Flow Diagram

```
CSV Files (data/raw/)
    ↓
[Docker] Airflow DAG
    ↓
Task 1: validate_source_files
    ↓
Task 2: ingest_csv_to_duckdb → RAW schema (16 views)
    ↓
Task 3: validate_row_counts
    ↓
Task 4: dbt_run_staging → STAGING schema (11 views)
    ↓
Task 5: dbt_run_intermediate → INTERMEDIATE schema (2 views)
    ↓
Task 6: dbt_run_marts → MARTS schema (7 tables)
    ├── Dimensions: dim_date, dim_patient, dim_doctor, dim_room, dim_department
    └── Facts: fct_visit, fct_billing
    ↓
Task 7: dbt_test_all → Validasi data quality
    ↓
Task 8: dbt_docs_generate → Generate documentation
    ↓
Ready for Analytics ✓
```

---

## 📊 Schema Architecture

### Schema: `raw` (Auto-generated from CSV)
- 16 tables dari CSV files
- Format: semua nama tabel lowercase
- PK: defined dalam sources.yml

### Schema: `staging` (10 views)
- `stg_patient` - cleaned patient data
- `stg_visit` - visit dengan calculated fields
- `stg_diagnosis` - diagnosis records
- `stg_treatment` - treatment records
- `stg_billing` - combined billing (bill + mdbl)
- `stg_payment` - payment records
- `stg_doctor` - filtered doctors
- `stg_department` - department master
- `stg_room` - room master
- `stg_medicine` - medicine master

### Schema: `intermediate` (2 views)
- `int_visit_enriched` - visit + patient + room + department
- `int_billing_enriched` - billing + payment + patient dengan outstanding calc

### Schema: `marts` (7 tables)
**Dimensions** (5):
- `dim_date` - 2,557 rows (calendar)
- `dim_patient` - 20 rows
- `dim_doctor` - 40 rows
- `dim_room` - 60 rows
- `dim_department` - 20 rows

**Facts** (2):
- `fct_visit` - 100 rows (measures: los_hours, is_inpatient)
- `fct_billing` - 100 rows (measures: amount, payment, outstanding)

---

## 🔍 File Dependencies

```
docker-compose.yml
    ├─ Links to: airflow/dags/, dbt/, data/, duckdb/

airflow/dags/healthcare_pipeline.py
    ├─ Reads: data/raw/*.csv
    ├─ Writes: duckdb/healthcare.duckdb (raw schema)
    └─ Executes: dbt/ commands

dbt/dbt_project.yml
    ├─ References: dbt/profiles.yml
    └─ Contains: models/, sources.yml

dbt/profiles.yml
    └─ Connects to: duckdb/healthcare.duckdb

dbt/models/staging/sources.yml
    └─ Defines: raw.* tables as sources

dbt/models/staging/*.sql
    ├─ Reads from: sources (raw schema)
    └─ Writes to: staging schema (views)

dbt/models/intermediate/*.sql
    ├─ Reads from: stg_* (staging views)
    └─ Writes to: intermediate schema (views)

dbt/models/marts/*.sql
    ├─ Reads from: int_* (intermediate views)
    ├─ Reads from: stg_* (staging views)
    └─ Writes to: marts schema (tables)

dbt/models/marts/schema.yml
    ├─ Documents: all mart models
    ├─ Defines: tests & relationships
    └─ References: dim_* and fct_* tables
```

---

## 📦 Docker Configuration

### Service: `postgres`
- **Image**: postgres:15-alpine
- **Purpose**: Airflow metadata database
- **Port**: 5432 (internal)
- **Volume**: postgres_data (persisted)
- **Credentials**: airflow/airflow

### Service: `airflow-webserver`
- **Image**: apache/airflow:2.8.0-python3.10
- **Purpose**: Airflow UI & REST API
- **Port**: 8080 (exposed)
- **Volumes**: 
  - dags/ → /opt/airflow/dags
  - dbt/ → /opt/airflow/dbt
  - data/ → /opt/airflow/data
  - duckdb/ → /opt/airflow/duckdb
- **Entrypoint**: pip install dependencies + airflow webserver

### Service: `airflow-scheduler`
- **Image**: apache/airflow:2.8.0-python3.10
- **Purpose**: DAG scheduling & task execution
- **Volumes**: Same as webserver
- **Entrypoint**: pip install dependencies + airflow scheduler

**Network**: healthcare_network (custom bridge)

---

## 🚀 Execution Flow

1. **Pre-execution**:
   - ✅ All files created
   - ✅ Folder structure ready
   - ⏳ CSV files (run `python generate_dummy_data.py`)
   - ⏳ DuckDB database (auto-created by DAG)

2. **Docker startup**:
   - `docker-compose up -d`
   - Postgres starts
   - Airflow webserver initializes
   - Airflow scheduler starts

3. **DAG execution**:
   - Trigger via UI
   - 8 tasks run sequentially
   - Each task has retry logic (2x with 5min delay)
   - All data flows into marts schema

4. **Analytics ready**:
   - Query marts tables directly
   - Connect BI tools (Metabase/Superset)
   - Generate reports

---

## ✅ Quality Checks

### In `schema.yml` (marts layer):
- **Unique tests**: Semua surrogate keys harus unique
- **Not null tests**: Primary keys harus not null
- **Relationship tests**: Foreign keys harus valid
- **Custom tests**: Business rule validations

### In DAG (Task 7):
- `dbt test` menjalankan semua tests
- Fails DAG jika ada test yang gagal
- Output tersimpan di dbt/target/

---

## 📚 Documentation Files

| File | Content |
|------|---------|
| `README.md` | Project overview, quick start, data model explanation |
| `SETUP_CHECKLIST.md` | Step-by-step setup guide dengan expected outputs |
| `FILES_MANIFEST.md` | File ini - dokumentasi lengkap manifest |
| `docker-compose.yml` | Docker config with comments |
| `dbt_project.yml` | dbt config dengan comments |
| `profiles.yml` | Connection config |
| `sources.yml` | Raw table definitions |
| `schema.yml` | Model definitions & tests |

---

## 🎯 Next Steps After Setup

1. ✅ **Verify** - Run SETUP_CHECKLIST.md
2. ✅ **Query** - Explore marts tables with SQL
3. ✅ **Integrate** - Connect Metabase/Superset to DuckDB
4. ✅ **Customize** - Modify dbt models untuk custom metrics
5. ✅ **Scale** - Swap dummy data dengan real Kaggle data
6. ✅ **Schedule** - DAG runs daily at 01:00 (customizable)

---

## 📊 Summary Statistics

| Category | Count |
|----------|-------|
| **Total Files** | 33 |
| **Configuration Files** | 6 |
| **Python Files** | 2 |
| **SQL Files** | 21 |
| **YAML Files** | 2 |
| **Markdown Docs** | 3 |
| **Staging Models** | 10 |
| **Intermediate Models** | 2 |
| **Mart Models** | 8 |
| **Directories Created** | 6 |
| **Expected CSV Tables** | 16 |
| **Expected DuckDB Schemas** | 4 |
| **Total Expected Rows** | ~1,695 |

---

## 🔐 Security Notes

- ⚠️ **WARNING**: Credentials hardcoded untuk development saja!
  - Airflow default: `admin/admin`
  - Postgres: `airflow/airflow`
  - **JANGAN USE DI PRODUCTION** - gunakan secrets management!

- ⚠️ DuckDB file tidak encrypted - pastikan akses terbatas

- ✅ Semua query menggunakan parameterized queries untuk prevent SQL injection

---

## Version & Changelog

**Version**: 1.0  
**Created**: April 2026  
**Last Updated**: April 2026  
**Status**: ✅ PRODUCTION READY (for learning/development)

---

**Next File to Read**: `SETUP_CHECKLIST.md` untuk langkah-langkah eksekusi!
