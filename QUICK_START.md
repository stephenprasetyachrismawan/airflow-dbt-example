# 🚀 QUICK START - Healthcare Data Warehouse

**Total execution time: ~5-10 minutes**

---

## 📋 Prerequisites Check

Before starting, verify you have:

- ✅ Docker Desktop **installed and running** on Windows
- ✅ Python 3.10+ installed
- ✅ All 33 project files in place
- ✅ CSV files generated (should be in `data/raw/`)

---

## ⚡ FASTEST WAY - Run Everything in One Command

### **Option 1: Using PowerShell Script (RECOMMENDED)**

Open **Windows PowerShell** as Administrator and paste this:

```powershell
cd "C:\Users\HP\Documents\Kuliah\s2\semester2\dwib\healthcare\DWIB Healthcare Data Mart preparation\healthcare_dw"
powershell.exe -ExecutionPolicy Bypass -File setup_and_run.ps1
```

This will:
1. ✅ Verify CSV files (generate if needed)
2. ✅ Check Docker installation
3. ✅ Start all containers
4. ✅ Wait for services to be healthy
5. ✅ Display status and next steps

**Expected output:**
```
OK - All 16 CSV files found in data/raw/
OK - Docker is installed
Running: docker-compose up -d
Waiting 60 seconds for services to startup...

NAME                             STATUS              PORTS
healthcare_postgres             Up 1 min (healthy)  5432/tcp
healthcare_airflow_webserver    Up 1 min (healthy)  0.0.0.0:8080->8080/tcp
healthcare_airflow_scheduler    Up 1 min            

SETUP COMPLETE!
```

---

### **Option 2: Using Batch Script**

Open **Command Prompt** and paste:

```cmd
cd C:\Users\HP\Documents\Kuliah\s2\semester2\dwib\healthcare\DWIB Healthcare Data Mart preparation\healthcare_dw
run_docker.bat
```

---

### **Option 3: Manual Step-by-Step**

If you prefer manual execution, open **Windows PowerShell** in the project folder:

```powershell
cd "C:\Users\HP\Documents\Kuliah\s2\semester2\dwib\healthcare\DWIB Healthcare Data Mart preparation\healthcare_dw"
```

Then run commands one by one:

**Step 1: Generate CSV (if needed)**
```powershell
python generate_dummy_data.py
```

**Step 2: Start Docker**
```powershell
docker-compose up -d
```

**Step 3: Wait for startup**
```powershell
Start-Sleep -Seconds 60
```

**Step 4: Check status**
```powershell
docker-compose ps
```

**Expected status:** All containers showing `Up` and `healthy`

---

## 🌐 Access Airflow UI

After containers are running, open your browser:

**URL**: http://localhost:8080

**Login Credentials:**
- **Username**: `admin`
- **Password**: `admin`

---

## ▶️ Trigger the DAG

### Step 1: Find the DAG
In Airflow UI, look for DAG named: **`healthcare_pipeline_duckdb`**

### Step 2: Enable the DAG
Click the **toggle switch** next to the DAG name (should turn blue/on)

### Step 3: Trigger the DAG
1. Click on the DAG name to open it
2. Click the **play icon** (▶️) in the top right
3. Select "Trigger DAG"
4. Click "Trigger" button (use default settings)

### Step 4: Monitor Execution
Watch the task execution flow. You should see all 8 tasks execute:

```
✓ validate_source_files      (green)
✓ ingest_csv_to_duckdb       (green)
✓ validate_row_counts        (green)
✓ dbt_run_staging            (green)
✓ dbt_run_intermediate       (green)
✓ dbt_run_marts              (green)
✓ dbt_test_all               (green)
✓ dbt_docs_generate          (green)
```

**Expected duration**: 35-45 seconds

---

## ✅ Verify Success

Once DAG completes, verify the data:

### Using Python REPL:

Open **PowerShell** in project folder:

```powershell
python
```

Then paste:

```python
import duckdb

con = duckdb.connect('duckdb/healthcare.duckdb')

# Check marts tables
print("MARTS TABLES:")
tables = con.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'marts'
    ORDER BY table_name
""").fetchdf()
print(tables)

# Check row counts
print("\nROW COUNTS:")
for table in ['dim_date', 'dim_patient', 'dim_doctor', 'dim_room', 'dim_department', 'fct_visit', 'fct_billing']:
    count = con.execute(f"SELECT COUNT(*) FROM marts.{table}").fetchone()[0]
    print(f"  {table}: {count} rows")

# Sample data
print("\nFCT_VISIT SAMPLE:")
print(con.execute("SELECT * FROM marts.fct_visit LIMIT 3").fetchdf().head())

con.close()
exit()
```

**Expected output:**
```
MARTS TABLES:
      table_name
           dim_date
        dim_doctor
       dim_patient
          dim_room
    dim_department
        fct_billing
         fct_visit

ROW COUNTS:
  dim_date: 2557 rows
  dim_patient: 20 rows
  dim_doctor: 40 rows
  dim_room: 60 rows
  dim_department: 20 rows
  fct_visit: 100 rows
  fct_billing: 100 rows

FCT_VISIT SAMPLE:
  visit_key  visit_id  patient_key  ...  dw_load_date
          1  REF1234           1  ...         ...
          2  REF5678           2  ...         ...
          3  REF9012           3  ...         ...
```

---

## 🐳 Docker Commands Reference

### View logs:
```powershell
docker-compose logs airflow-webserver   # Webserver logs
docker-compose logs airflow-scheduler   # Scheduler logs
docker-compose logs postgres            # Database logs
```

### Stop services:
```powershell
docker-compose down
```

### Restart services:
```powershell
docker-compose restart
```

### Remove everything (reset):
```powershell
docker-compose down -v
```

---

## ❌ Troubleshooting

### "Docker daemon is not running"
**Solution**: Open Docker Desktop app and wait for it to start

### "docker-compose: command not found"
**Solution**: Docker Desktop should include docker-compose. Try:
```powershell
docker-compose --version
```

### Containers won't start / keep restarting
**Solution**: Check logs
```powershell
docker-compose logs
```

### Airflow UI not accessible at http://localhost:8080
**Solution**: 
1. Verify containers running: `docker-compose ps`
2. Wait 2-3 more minutes for startup
3. Check firewall - port 8080 might be blocked

### DAG fails with "module not found" errors
**Solution**: These are auto-installed. Check logs:
```powershell
docker-compose logs airflow-scheduler
```

### DuckDB file not created
**Solution**: Make sure DAG task `ingest_csv_to_duckdb` completes successfully. Check task logs in Airflow UI.

---

## 📊 What You'll Have After Completion

✅ **4 Database Schemas:**
- `raw` - 16 tables from CSV
- `staging` - 10 cleaned views
- `intermediate` - 2 enriched views
- `marts` - 7 analytics-ready tables (5 dimensions + 2 facts)

✅ **Healthcare Domain Data:**
- 20 patients
- 40 doctors
- 60 rooms
- 100+ visits, diagnoses, treatments
- 100+ billing & payment records

✅ **Ready for Analytics:**
- Query fact tables directly
- Connect BI tools (Metabase, Superset, Power BI, Tableau)
- Build dashboards
- Create reports

---

## 📚 Next Steps

1. **Explore the data:**
   - Query marts tables with SQL
   - Check relationships between dimensions and facts

2. **Customize for your needs:**
   - Modify dbt models in `dbt/models/`
   - Add custom metrics
   - Create more fact/dimension tables

3. **Integrate BI tool:**
   - Connection string: `duckdb:///path/to/duckdb/healthcare.duckdb`
   - Create dashboards
   - Build reports

4. **Scale to production:**
   - Replace dummy data with real Kaggle dataset
   - Add scheduling (DAG already set for daily 01:00)
   - Implement monitoring

---

## 💡 Remember

- **All files are in one place**: `healthcare_dw/` folder
- **Everything is containerized**: No conflicts with system
- **Easy to reset**: Just run `docker-compose down -v` and restart
- **Documented**: Check `README.md` and `SETUP_CHECKLIST.md` for details

---

## ✨ You're All Set!

Just run the PowerShell script and watch the magic happen! 🚀

```powershell
powershell.exe -ExecutionPolicy Bypass -File setup_and_run.ps1
```

**Questions?** Check the documentation files in the project folder.

**Happy Data Warehousing!** 📊✨

---

**Created**: April 2026  
**For**: UGM DWIB - Healthcare Data Warehouse  
**Duration**: ~5 minutes from start to complete pipeline execution
