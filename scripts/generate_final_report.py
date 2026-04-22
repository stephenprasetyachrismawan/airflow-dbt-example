"""
Tahap 6F — Generate Final Validation Report
Menghasilkan laporan ringkas dalam format teks untuk disertakan di laporan PDF.
"""
import duckdb
from datetime import datetime

DUCKDB_PATH = '/opt/airflow/duckdb/healthcare.duckdb'
con = duckdb.connect(DUCKDB_PATH, read_only=True)

print("=" * 80)
print("HEALTHCARE DATA MART — FINAL VALIDATION REPORT")
print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)

# 1. Row counts
print("\n1. DATA MART ROW COUNTS")
print("-" * 55)
tables = [
    ('dim_date', 'DIM'),
    ('dim_patient', 'DIM'),
    ('dim_doctor', 'DIM'),
    ('dim_department', 'DIM'),
    ('dim_room', 'DIM'),
    ('fct_visit', 'FCT'),
    ('fct_billing', 'FCT'),
]
total_rows = 0
for t, layer in tables:
    n = con.execute(f"SELECT COUNT(*) FROM analytics_marts.{t}").fetchone()[0]
    total_rows += n
    print(f"  [{layer}] {t:<35}: {n:>12,} rows")
print(f"\n  TOTAL: {total_rows:,} rows")

# 2. Rentang tanggal data
print("\n2. RENTANG TANGGAL DATA")
print("-" * 55)
date_range = con.execute("""
    SELECT
        MIN(visit_start_time)::date AS min_date,
        MAX(visit_start_time)::date AS max_date
    FROM analytics_marts.fct_visit
    WHERE visit_start_time IS NOT NULL
""").fetchone()
if date_range[0]:
    print(f"  Kunjungan pertama : {date_range[0]}")
    print(f"  Kunjungan terakhir: {date_range[1]}")

# 3. Distribusi rawat inap vs rawat jalan
print("\n3. DISTRIBUSI KUNJUNGAN")
print("-" * 55)
dist = con.execute("""
    SELECT
        SUM(CASE WHEN is_inpatient THEN 1 ELSE 0 END)     AS rawat_inap,
        SUM(CASE WHEN NOT is_inpatient THEN 1 ELSE 0 END) AS rawat_jalan,
        COUNT(*)                                           AS total
    FROM analytics_marts.fct_visit
""").fetchone()
if dist[2] > 0:
    print(f"  Rawat Inap  : {dist[0]:>12,} ({dist[0] * 100 / dist[2]:.1f}%)")
    print(f"  Rawat Jalan : {dist[1]:>12,} ({dist[1] * 100 / dist[2]:.1f}%)")
    print(f"  Total       : {dist[2]:>12,}")

# 4. Distribusi status kunjungan
print("\n4. DISTRIBUSI STATUS KUNJUNGAN")
print("-" * 55)
status_dist = con.execute("""
    SELECT visit_status_description, COUNT(*) AS total
    FROM analytics_marts.fct_visit
    WHERE visit_status_description IS NOT NULL
    GROUP BY visit_status_description
    ORDER BY total DESC
""").fetchdf()
for _, row in status_dist.iterrows():
    print(f"  {row['visit_status_description']:<30}: {int(row['total']):>12,}")

# 5. Ringkasan billing
print("\n5. RINGKASAN BILLING")
print("-" * 55)
billing = con.execute("""
    SELECT
        COUNT(*)                                                AS total_billing,
        ROUND(SUM(billing_amount), 0)                          AS total_tagihan_idr,
        ROUND(SUM(payment_amount), 0)                          AS total_bayar_idr,
        ROUND(SUM(outstanding_amount), 0)                      AS total_sisa_idr,
        SUM(CASE WHEN payment_status = 'Paid' THEN 1 ELSE 0 END) AS total_paid,
        SUM(CASE WHEN payment_status = 'Partially Paid'
                 THEN 1 ELSE 0 END)                            AS total_partial,
        SUM(CASE WHEN payment_status = 'Outstanding'
                 THEN 1 ELSE 0 END)                            AS total_outstanding
    FROM analytics_marts.fct_billing
""").fetchone()
print(f"  Total transaksi  : {billing[0]:>12,}")
print(f"  Total tagihan    : Rp {billing[1]:>15,.0f}")
print(f"  Total dibayar    : Rp {billing[2]:>15,.0f}")
print(f"  Sisa tagihan     : Rp {billing[3]:>15,.0f}")
print(f"  Status Paid      : {billing[4]:>12,}")
print(f"  Status Partial   : {billing[5]:>12,}")
print(f"  Status Outstanding: {billing[6]:>11,}")

# 6. Jumlah dimensi unik
print("\n6. JUMLAH ENTITAS UNIK")
print("-" * 55)
entity_counts = [
    ('Pasien unik', "SELECT COUNT(*) - 1 FROM analytics_marts.dim_patient"),
    ('Dokter unik', "SELECT COUNT(*) - 1 FROM analytics_marts.dim_doctor"),
    ('Departemen unik', "SELECT COUNT(*) - 1 FROM analytics_marts.dim_department"),
    ('Ruangan unik', "SELECT COUNT(*) - 1 FROM analytics_marts.dim_room"),
    ('Rentang tanggal (hari)', "SELECT COUNT(*) FROM analytics_marts.dim_date WHERE date_key != -1"),
]
for label, sql in entity_counts:
    n = con.execute(sql).fetchone()[0]
    print(f"  {label:<30}: {n:>12,}")

print("\n" + "=" * 80)
print("Data mart siap untuk koneksi ke BI tools.")
print("Connection URI: duckdb:////opt/airflow/duckdb/healthcare.duckdb")
print("=" * 80)

con.close()
