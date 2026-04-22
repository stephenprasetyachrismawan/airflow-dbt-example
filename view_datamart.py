"""
Healthcare DW — Interactive Data Mart Viewer
Jalankan: python view_datamart.py
Atau via Docker: docker exec healthcare_airflow_webserver python3 /opt/airflow/view_datamart.py
"""
import duckdb
import os

# Coba path di dalam Docker dulu, fallback ke path lokal
DOCKER_PATH = '/opt/airflow/duckdb/healthcare.duckdb'
LOCAL_PATH = os.path.join(os.path.dirname(__file__), 'duckdb', 'healthcare.duckdb')
DUCKDB_PATH = DOCKER_PATH if os.path.exists(DOCKER_PATH) else LOCAL_PATH

con = duckdb.connect(DUCKDB_PATH, read_only=True)

MENU = """
╔══════════════════════════════════════════════════════╗
║      HEALTHCARE DATA MART — INTERACTIVE VIEWER       ║
╠══════════════════════════════════════════════════════╣
║  DIMENSI                                             ║
║  1. dim_date        — Dimensi Tanggal (2020-2026)    ║
║  2. dim_patient     — Dimensi Pasien                 ║
║  3. dim_doctor      — Dimensi Dokter                 ║
║  4. dim_department  — Dimensi Departemen             ║
║  5. dim_room        — Dimensi Ruangan                ║
║                                                      ║
║  FAKTA                                               ║
║  6. fct_visit       — Fakta Kunjungan                ║
║  7. fct_billing     — Fakta Billing                  ║
║                                                      ║
║  BUSINESS QUERIES                                    ║
║  8.  BQ1: Tren Kunjungan per Bulan                   ║
║  9.  BQ2: Kunjungan per Departemen                   ║
║  10. BQ3: Avg LOS per Departemen (Rawat Inap)        ║
║  11. BQ4: Top 5 Departemen per Bulan                 ║
║  12. BQ5: Distribusi Pembayaran per Bulan            ║
║                                                      ║
║  LAINNYA                                             ║
║  13. Row counts semua tabel                          ║
║  14. Custom SQL query                                ║
║  0.  Keluar                                          ║
╚══════════════════════════════════════════════════════╝
"""

QUERIES = {
    '1':  ("dim_date — 10 baris pertama", "SELECT * FROM analytics_marts.dim_date WHERE calendar_date IS NOT NULL LIMIT 10"),
    '2':  ("dim_patient — 10 baris pertama", "SELECT patient_key, pat_id, full_name, gender, dob, blood_type, marital_status FROM analytics_marts.dim_patient LIMIT 10"),
    '3':  ("dim_doctor — semua", "SELECT doctor_key, doctor_id, full_name, role_description, status_description FROM analytics_marts.dim_doctor"),
    '4':  ("dim_department — semua", "SELECT * FROM analytics_marts.dim_department"),
    '5':  ("dim_room — 10 baris pertama", "SELECT room_key, room_id, room_type_description, room_purpose_description, room_capacity, department_name FROM analytics_marts.dim_room LIMIT 10"),
    '6':  ("fct_visit — 10 baris pertama", "SELECT visit_key, visit_id, patient_key, doctor_key, department_key, visit_date_key, is_inpatient, length_of_stay_hours, visit_status_description FROM analytics_marts.fct_visit LIMIT 10"),
    '7':  ("fct_billing — 10 baris pertama", "SELECT billing_key, billing_id, patient_key, billing_date_key, billing_amount, payment_amount, outstanding_amount, payment_status FROM analytics_marts.fct_billing LIMIT 10"),
    '8':  ("BQ1: Tren Kunjungan per Bulan", """
        SELECT d.year AS tahun, d.month AS bulan, d.month_name AS nama_bulan,
               COUNT(*) AS total_kunjungan,
               SUM(CASE WHEN v.is_inpatient THEN 1 ELSE 0 END) AS rawat_inap,
               SUM(CASE WHEN NOT v.is_inpatient THEN 1 ELSE 0 END) AS rawat_jalan,
               ROUND(SUM(CASE WHEN v.is_inpatient THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct_inap
        FROM analytics_marts.fct_visit v
        JOIN analytics_marts.dim_date d ON v.visit_date_key=d.date_key
        WHERE d.calendar_date >= (CURRENT_DATE - INTERVAL '12 months')
        GROUP BY d.year, d.month, d.month_name ORDER BY d.year, d.month
    """),
    '9':  ("BQ2: Kunjungan per Departemen", """
        SELECT dep.department_name, COUNT(*) AS total_kunjungan,
               ROUND(COUNT(*)*100.0/(SELECT COUNT(*) FROM analytics_marts.fct_visit WHERE department_key<>-1),1) AS pct
        FROM analytics_marts.fct_visit v
        JOIN analytics_marts.dim_department dep ON v.department_key=dep.department_key
        WHERE dep.department_key<>-1
        GROUP BY dep.department_name ORDER BY total_kunjungan DESC
    """),
    '10': ("BQ3: Avg LOS per Departemen (Rawat Inap)", """
        SELECT dep.department_name, COUNT(*) AS total_rawat_inap,
               ROUND(AVG(v.length_of_stay_hours),2) AS avg_los_jam,
               ROUND(AVG(v.length_of_stay_hours)/24.0,2) AS avg_los_hari,
               ROUND(MIN(v.length_of_stay_hours),1) AS min_jam,
               ROUND(MAX(v.length_of_stay_hours),1) AS max_jam
        FROM analytics_marts.fct_visit v
        JOIN analytics_marts.dim_department dep ON v.department_key=dep.department_key
        WHERE v.is_inpatient=TRUE AND v.length_of_stay_hours>0 AND dep.department_key<>-1
        GROUP BY dep.department_name ORDER BY avg_los_jam DESC
    """),
    '11': ("BQ4: Top 5 Departemen per Bulan", """
        SELECT d.year AS tahun, d.month AS bulan, d.month_name AS nama_bulan,
               v.department_name, COUNT(*) AS total,
               RANK() OVER (PARTITION BY d.year,d.month ORDER BY COUNT(*) DESC) AS rank_bulan
        FROM analytics_marts.fct_visit v
        JOIN analytics_marts.dim_date d ON v.visit_date_key=d.date_key
        WHERE d.calendar_date IS NOT NULL
        GROUP BY d.year,d.month,d.month_name,v.department_name
        QUALIFY rank_bulan<=5
        ORDER BY tahun,bulan,total DESC LIMIT 60
    """),
    '12': ("BQ5: Distribusi Pembayaran per Bulan", """
        SELECT d.year AS tahun, d.month AS bulan, d.month_name AS nama_bulan,
               COUNT(*) AS total_billing,
               SUM(CASE WHEN b.payment_status='Paid' THEN 1 ELSE 0 END) AS paid,
               SUM(CASE WHEN b.payment_status='Partially Paid' THEN 1 ELSE 0 END) AS partial,
               ROUND(SUM(b.billing_amount),0) AS total_tagihan_idr,
               ROUND(SUM(b.payment_amount),0) AS total_bayar_idr,
               ROUND(SUM(CASE WHEN b.payment_status='Paid' THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct_paid
        FROM analytics_marts.fct_billing b
        JOIN analytics_marts.dim_date d ON b.billing_date_key=d.date_key
        WHERE d.calendar_date IS NOT NULL
        GROUP BY d.year,d.month,d.month_name ORDER BY tahun,bulan
    """),
    '13': ("Row counts semua tabel", None),
}

def show_row_counts():
    tables = ['dim_date','dim_patient','dim_doctor','dim_department','dim_room','fct_visit','fct_billing']
    print("\n" + "="*50)
    print("ROW COUNTS — analytics_marts")
    print("="*50)
    total = 0
    for t in tables:
        n = con.execute(f"SELECT COUNT(*) FROM analytics_marts.{t}").fetchone()[0]
        total += n
        layer = "DIM" if t.startswith('dim') else "FCT"
        print(f"  [{layer}] {t:<30}: {n:>8,}")
    print("-"*50)
    print(f"  {'TOTAL':<35}: {total:>8,}")
    print("="*50)

def run_query(title, sql):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")
    df = con.execute(sql).fetchdf()
    if df.empty:
        print("  (tidak ada data)")
    else:
        print(df.to_string(index=False))
        print(f"\n  → {len(df)} rows")
    print()

def main():
    print(MENU)
    while True:
        try:
            choice = input("Pilih menu [0-14]: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nKeluar.")
            break

        if choice == '0':
            print("Selesai.")
            break
        elif choice == '13':
            show_row_counts()
        elif choice == '14':
            print("Masukkan SQL query (ketik END di baris baru untuk menjalankan):")
            lines = []
            while True:
                line = input()
                if line.strip().upper() == 'END':
                    break
                lines.append(line)
            sql = '\n'.join(lines)
            if sql.strip():
                try:
                    run_query("Custom Query", sql)
                except Exception as e:
                    print(f"ERROR: {e}")
        elif choice in QUERIES:
            title, sql = QUERIES[choice]
            if sql:
                try:
                    run_query(title, sql)
                except Exception as e:
                    print(f"ERROR: {e}")
        else:
            print("Pilihan tidak valid.")

        print(MENU)

if __name__ == '__main__':
    main()
    con.close()
