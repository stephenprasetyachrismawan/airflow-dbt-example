"""
Tahap 6F — Generate Final Validation Report Healthcare Data Mart
Menghasilkan laporan ringkas dalam format teks untuk disertakan di laporan PDF.
"""
import duckdb
from datetime import datetime

DUCKDB_PATH = '/opt/airflow/duckdb/healthcare.duckdb'
con = duckdb.connect(DUCKDB_PATH, read_only=True)

print("=" * 80)
print("HEALTHCARE DATA MART -- FINAL VALIDATION REPORT")
print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)

# ─────────────────────────────────────────
# 1. Row counts semua 8 tabel
# ─────────────────────────────────────────
print("\n1. DATA MART ROW COUNTS")
print("-" * 60)
tables = [
    ("dim_tanggal",        "DIM"),
    ("dim_pasien",         "DIM"),
    ("dim_dokter",         "DIM"),
    ("dim_departemen",     "DIM"),
    ("dim_ruangan",        "DIM"),
    ("fct_kunjungan",      "FCT"),
    ("fct_diagnosis",      "FCT"),
    ("fct_tindakan_medis", "FCT"),
]
total_rows = 0
for t, layer in tables:
    n = con.execute(f"SELECT COUNT(*) FROM analytics_marts.{t}").fetchone()[0]
    total_rows += n
    print(f"  [{layer}] {t:<35}: {n:>12,} rows")
print(f"\n  TOTAL: {total_rows:,} rows")

# ─────────────────────────────────────────
# 2. Rentang tanggal data
# ─────────────────────────────────────────
print("\n2. RENTANG TANGGAL DATA KUNJUNGAN")
print("-" * 60)
date_range = con.execute("""
    SELECT
        MIN(waktu_masuk)::date AS min_date,
        MAX(waktu_masuk)::date AS max_date
    FROM analytics_marts.fct_kunjungan
    WHERE waktu_masuk IS NOT NULL
""").fetchone()
if date_range[0]:
    print(f"  Kunjungan pertama : {date_range[0]}")
    print(f"  Kunjungan terakhir: {date_range[1]}")
else:
    print("  (tidak ada data waktu_masuk)")

# ─────────────────────────────────────────
# 3. Distribusi rawat inap vs rawat jalan
# ─────────────────────────────────────────
print("\n3. DISTRIBUSI KUNJUNGAN")
print("-" * 60)
dist = con.execute("""
    SELECT
        SUM(CASE WHEN is_rawat_inap = TRUE  THEN 1 ELSE 0 END) AS rawat_inap,
        SUM(CASE WHEN is_rawat_inap = FALSE THEN 1 ELSE 0 END) AS rawat_jalan,
        COUNT(*) AS total
    FROM analytics_marts.fct_kunjungan
""").fetchone()
if dist[2] > 0:
    print(f"  Rawat Inap  : {dist[0]:>12,} ({dist[0] * 100.0 / dist[2]:.1f}%)")
    print(f"  Rawat Jalan : {dist[1]:>12,} ({dist[1] * 100.0 / dist[2]:.1f}%)")
    print(f"  Total       : {dist[2]:>12,}")

# ─────────────────────────────────────────
# 4. Distribusi status kunjungan
# ─────────────────────────────────────────
print("\n4. DISTRIBUSI STATUS KUNJUNGAN")
print("-" * 60)
status_dist = con.execute("""
    SELECT deskripsi_status_kunjungan, COUNT(*) AS total
    FROM analytics_marts.fct_kunjungan
    WHERE deskripsi_status_kunjungan IS NOT NULL
    GROUP BY deskripsi_status_kunjungan
    ORDER BY total DESC
""").fetchdf()
for _, row in status_dist.iterrows():
    print(f"  {str(row['deskripsi_status_kunjungan']):<30}: {int(row['total']):>12,}")

# ─────────────────────────────────────────
# 5. Jumlah entitas unik per dimensi
# ─────────────────────────────────────────
print("\n5. JUMLAH ENTITAS UNIK")
print("-" * 60)
entity_counts = [
    ("Pasien unik",          "SELECT COUNT(*) - 1 FROM analytics_marts.dim_pasien WHERE pasien_key <> -1"),
    ("Dokter unik",          "SELECT COUNT(*) - 1 FROM analytics_marts.dim_dokter WHERE dokter_key <> -1"),
    ("Departemen unik",      "SELECT COUNT(*) - 1 FROM analytics_marts.dim_departemen WHERE departemen_key <> -1"),
    ("Ruangan unik",         "SELECT COUNT(*) - 1 FROM analytics_marts.dim_ruangan WHERE ruangan_key <> -1"),
    ("Rentang tanggal (hari)","SELECT COUNT(*) FROM analytics_marts.dim_tanggal WHERE tanggal_key <> -1"),
]
for label, sql in entity_counts:
    n = con.execute(sql).fetchone()[0]
    print(f"  {label:<30}: {n:>12,}")

# ─────────────────────────────────────────
# 6. Ringkasan diagnosis
# ─────────────────────────────────────────
print("\n6. RINGKASAN DIAGNOSIS")
print("-" * 60)
diag = con.execute("""
    SELECT
        COUNT(*) AS total_diagnosis,
        SUM(CASE WHEN is_confirmed = TRUE THEN 1 ELSE 0 END) AS confirmed,
        SUM(CASE WHEN is_diagnosis_utama = TRUE THEN 1 ELSE 0 END) AS utama,
        COUNT(DISTINCT refr_no) AS kunjungan_dengan_diagnosis
    FROM analytics_marts.fct_diagnosis
""").fetchone()
print(f"  Total diagnosis              : {diag[0]:>12,}")
print(f"  Diagnosis confirmed          : {diag[1]:>12,}")
print(f"  Diagnosis utama              : {diag[2]:>12,}")
print(f"  Kunjungan dengan diagnosis   : {diag[3]:>12,}")

print("\n  Top 5 Departemen berdasarkan jumlah diagnosis:")
top_diag = con.execute("""
    SELECT d.nama_departemen, COUNT(*) AS total
    FROM analytics_marts.fct_diagnosis diag
    JOIN analytics_marts.dim_departemen d ON diag.departemen_key = d.departemen_key
    WHERE diag.departemen_key <> -1
    GROUP BY d.nama_departemen
    ORDER BY total DESC
    LIMIT 5
""").fetchdf()
for _, row in top_diag.iterrows():
    print(f"    {str(row['nama_departemen']):<30}: {int(row['total']):>10,}")

# ─────────────────────────────────────────
# 7. Ringkasan tindakan medis
# ─────────────────────────────────────────
print("\n7. RINGKASAN TINDAKAN MEDIS")
print("-" * 60)
tind = con.execute("""
    SELECT
        COUNT(*) AS total_tindakan,
        SUM(CASE WHEN is_selesai = TRUE  THEN 1 ELSE 0 END) AS selesai,
        SUM(CASE WHEN is_operasi = TRUE  THEN 1 ELSE 0 END) AS operasi,
        SUM(CASE WHEN is_obat    = TRUE  THEN 1 ELSE 0 END) AS pemberian_obat,
        COUNT(DISTINCT refr_no) AS kunjungan_dengan_tindakan
    FROM analytics_marts.fct_tindakan_medis
""").fetchone()
print(f"  Total tindakan               : {tind[0]:>12,}")
print(f"  Tindakan selesai             : {tind[1]:>12,}")
print(f"  Tindakan operasi             : {tind[2]:>12,}")
print(f"  Tindakan pemberian obat      : {tind[3]:>12,}")
print(f"  Kunjungan dengan tindakan    : {tind[4]:>12,}")

# ─────────────────────────────────────────
# 8. Connection info
# ─────────────────────────────────────────
print("\n8. KONEKSI BI TOOLS")
print("-" * 60)
print("  Connection URI (duckdb-engine):")
print("    duckdb:////opt/airflow/duckdb/healthcare.duckdb")
print("  Schema: analytics_marts")
print("  Tabel tersedia:")
for t, layer in tables:
    print(f"    [{layer}] analytics_marts.{t}")

print("\n" + "=" * 80)
print("Data mart siap untuk koneksi ke BI tools (Apache Superset, Metabase, dll).")
print("=" * 80)

con.close()
