# Healthcare DW - Setup Checklist

Panduan step-by-step untuk setup dan menjalankan Healthcare Data Warehouse.

---

## ✅ PRE-SETUP (TAHAP 1)

### Prerequisites Verification
- [ ] **Docker Desktop** sudah terinstall di Windows
  - Verifikasi: Buka Command Prompt/PowerShell, jalankan `docker --version`
  - Expected: `Docker version 26.x.x` atau lebih baru
  - ⚠️ Jika belum: Download dari https://www.docker.com/products/docker-desktop

- [ ] **Python 3.10+** sudah terinstall
  - Verifikasi: `python --version`
  - Expected: `Python 3.10.x` atau lebih baru

- [ ] **pip** sudah terinstall
  - Verifikasi: `pip --version`
  - Expected: `pip xx.x`

---

## 📁 TAHAP 2: Struktur Folder (✓ SUDAH DIBUAT)

✅ Folder berikut sudah dibuat di:
```
C:\Users\HP\Documents\Kuliah\s2\semester2\dwib\healthcare\DWIB Healthcare Data Mart preparation\healthcare_dw\
```

- [x] `airflow/dags/` - DAG Airflow
- [x] `dbt/models/staging/` - dbt staging models
- [x] `dbt/models/intermediate/` - dbt intermediate models  
- [x] `dbt/models/marts/` - dbt mart models
- [x] `data/raw/` - Raw CSV storage
- [x] `duckdb/` - DuckDB database storage

---

## 🔧 TAHAP 3: Generate Dummy Data

### LANGKAH 1: Buka Terminal

Buka **Command Prompt** atau **Windows PowerShell** dan navigate ke folder project:

```powershell
cd "C:\Users\HP\Documents\Kuliah\s2\semester2\dwib\healthcare\DWIB Healthcare Data Mart preparation\healthcare_dw"
```

### LANGKAH 2: Install Dependencies (Opsional)

Jika library belum ada:
```powershell
pip install faker pandas numpy
```

### LANGKAH 3: Run Generator

```powershell
python generate_dummy_data.py
```

### EXPECTED OUTPUT:
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

Table                           Rows
STG_EHP__PATN                     20
STG_EHP__DPMT                     20
STG_EHP__STFF                     40
STG_EHP__ROMS                     60
STG_EHP__MDCN                     40
STG_EHP__ALGY                     10
STG_EHP__INSR                     50
STG_EHP__PTAL                     33
STG_EHP__MEDT                     50
STG_EHP__VIST                    100
STG_EHP__DIAG                    100
STG_EHP__TRTM                    100
STG_EHP__TMMD                    100
STG_EHP__BILL                    100
STG_EHP__MDBL                     50
STG_EHP__PMNT                    100
------------------------------
Total rows: 1,695
================================================================================
```

### VERIFIKASI:
- [ ] Lihat folder `data/raw/` - seharusnya ada **16 file CSV**
- [ ] Setiap file CSV memiliki size > 0 bytes

---

## 🐳 TAHAP 4: Jalankan Docker

### LANGKAH 1: Start Services

Masih di folder `healthcare_dw/`, jalankan:

```powershell
docker-compose up -d
```

**Jangan close terminal ini** - biarkan docker running.

### EXPECTED OUTPUT:
```
[+] Running 3/3
  ✓ Container healthcare_postgres               Created
  ✓ Container healthcare_airflow_webserver      Created
  ✓ Container healthcare_airflow_scheduler      Created
```

### LANGKAH 2: Wait untuk healthy status

Tunggu ~60 detik untuk semua services startup. Cek dengan:

```powershell
docker-compose ps
```

### EXPECTED OUTPUT:
```
NAME                              STATUS              PORTS
healthcare_postgres              Up 1 min (healthy)  5432/tcp
healthcare_airflow_webserver     Up 1 min (healthy)  0.0.0.0:8080->8080/tcp
healthcare_airflow_scheduler     Up 1 min            
```

**KEY**: status harus `Up` (tidak boleh `Restarting` atau `Exited`)

### TROUBLESHOOTING:
Jika ada yang `Exited`, lihat logs:
```powershell
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler
```

---

## 🌐 TAHAP 5: Akses Airflow UI

### LANGKAH 1: Buka Browser

Navigasi ke: **http://localhost:8080**

### LANGKAH 2: Login

- Username: `admin`
- Password: `admin`

### EXPECTED:
Dashboard Airflow terbuka, menampilkan list DAG kosong atau dengan `healthcare_pipeline_duckdb` disabled.

---

## ▶️ TAHAP 6: Trigger DAG Pipeline

### LANGKAH 1: Cari DAG

Di Airflow UI, cari **`healthcare_pipeline_duckdb`** di search bar atau scroll.

### LANGKAH 2: Enable DAG

Klik **toggle switch** di kiri nama DAG untuk **enable** (switch harus biru/on).

### LANGKAH 3: Trigger Manual

1. Klik nama DAG `healthcare_pipeline_duckdb`
2. Lihat tombol **play icon** (▶️) di kanan atas
3. Klik → dropdown "Trigger DAG"
4. Klik "Trigger" (default config OK)

### LANGKAH 4: Monitor Execution

Lihat task-task berjalan di DAG:
```
✓ validate_source_files      (0s)
✓ ingest_csv_to_duckdb       (5s)
✓ validate_row_counts        (2s)
✓ dbt_run_staging            (10s)
✓ dbt_run_intermediate       (5s)
✓ dbt_run_marts              (8s)
✓ dbt_test_all               (3s)
✓ dbt_docs_generate          (2s)
```

**Warna task:**
- 🟩 Green = Success
- 🟨 Yellow = Running
- 🔴 Red = Failed
- ⚪ Gray = Not started

### DURATION:
Keseluruhan pipeline ~35-45 detik.

### EXPECTED FINAL STATE:
Semua task **GREEN** ✓

---

## 📊 TAHAP 7: Verifikasi Data

### LANGKAH 1: Buka Terminal Baru

Buka **Command Prompt/PowerShell baru** (jangan tutup yang docker).

### LANGKAH 2: Navigate to Project

```powershell
cd "C:\Users\HP\Documents\Kuliah\s2\semester2\dwib\healthcare\DWIB Healthcare Data Mart preparation\healthcare_dw"
```

### LANGKAH 3: Query DuckDB

```powershell
python
```

Kemudian di Python REPL, ketik:

```python
import duckdb

# Connect
con = duckdb.connect('duckdb/healthcare.duckdb')

# Query 1: Lihat semua schema
print("\n=== SCHEMAS ===")
schemas = con.execute("""
    SELECT DISTINCT table_schema 
    FROM information_schema.tables 
    WHERE table_schema IN ('raw', 'staging', 'intermediate', 'marts')
    ORDER BY table_schema
""").fetchall()
for schema in schemas:
    print(f"  {schema[0]}")

# Query 2: Row count per schema
print("\n=== TABLE COUNTS ===")
for schema in ['raw', 'staging', 'intermediate', 'marts']:
    count = con.execute(f"""
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_schema = '{schema}'
    """).fetchone()[0]
    print(f"  {schema}: {count} tables")

# Query 3: Lihat marts tables dengan row count
print("\n=== MARTS TABLES & ROWS ===")
marts = con.execute("""
    SELECT table_name, 
           (SELECT COUNT(*) FROM marts.\\"\" || table_name || \\"\\") as row_count
    FROM information_schema.tables
    WHERE table_schema = 'marts'
    ORDER BY table_name
""").fetchdf()
print(marts.to_string())

# Query 4: Sample fct_visit
print("\n=== FCT_VISIT SAMPLE (First 3 rows) ===")
visits = con.execute("SELECT * FROM marts.fct_visit LIMIT 3").fetchdf()
print(visits.to_string())

# Query 5: Sample fct_billing
print("\n=== FCT_BILLING SAMPLE (First 3 rows) ===")
billings = con.execute("SELECT * FROM marts.fct_billing LIMIT 3").fetchdf()
print(billings.to_string())

con.close()
exit()
```

### EXPECTED OUTPUT:

```
=== SCHEMAS ===
  intermediate
  marts
  raw
  staging

=== TABLE COUNTS ===
  raw: 16 tables
  staging: 10 tables
  intermediate: 2 tables
  marts: 7 tables

=== MARTS TABLES & ROWS ===
  table_name         row_count
  dim_date            2557
  dim_department      20
  dim_doctor          40
  dim_patient         20
  dim_room           60
  fct_billing        100
  fct_visit          100

=== FCT_VISIT SAMPLE (First 3 rows) ===
  visit_key  visit_id       patient_key  doctor_key  ...
  1          REF2345678901  1            5           ...
  2          REF2345678902  2            8           ...
  3          REF2345678903  3            12          ...

=== FCT_BILLING SAMPLE (First 3 rows) ===
  billing_key  billing_id     patient_key  billing_amount  ...
  1            REF2345678901  1            5000000        ...
  2            REF2345678902  2            3500000        ...
  3            REF2345678903  3            7200000        ...
```

---

## ✅ FINAL CHECKLIST

- [ ] Docker containers running healthy
- [ ] DAG `healthcare_pipeline_duckdb` created
- [ ] All 8 tasks executed successfully (all GREEN)
- [ ] DuckDB file `duckdb/healthcare.duckdb` exists
- [ ] All 4 schemas present: `raw`, `staging`, `intermediate`, `marts`
- [ ] Mart tables exist: 7 tables (2 facts + 5 dimensions)
- [ ] Row counts match expected:
  - dim_date: 2,557 rows
  - dim_patient: 20 rows
  - dim_doctor: 40 rows
  - dim_room: 60 rows
  - dim_department: 20 rows
  - fct_visit: 100 rows
  - fct_billing: 100 rows

---

## 🎯 Success Criteria

✅ **SUKSES** jika:
1. Semua file CSV tergenerate di `data/raw/`
2. Docker containers semuanya running
3. Airflow DAG berjalan tanpa error (all tasks GREEN)
4. DuckDB database terisi dengan data lengkap
5. Bisa query marts tables dan mendapat hasil

---

## 🧹 Cleanup (Jika Perlu Reset)

Untuk reset dan start dari awal:

```powershell
# Stop dan remove containers
docker-compose down -v

# Remove DuckDB file
del duckdb/healthcare.duckdb

# Remove CSV files
del data/raw/*.csv

# Remove dbt artifacts
rmdir /s /q dbt/target

# Start fresh
python generate_dummy_data.py
docker-compose up -d
```

---

## 📞 Support

Jika ada error, cek:

1. **Docker logs**:
   ```powershell
   docker-compose logs -f airflow-webserver
   ```

2. **DAG logs** (di Airflow UI):
   - Klik task yang error
   - Lihat "Logs" tab

3. **DuckDB connection**:
   ```powershell
   docker-compose exec airflow-webserver duckdb /opt/airflow/duckdb/healthcare.duckdb
   ```

---

**Version**: 1.0  
**Last Updated**: April 2026  
**Course**: UGM DWIB - Healthcare Data Warehouse
