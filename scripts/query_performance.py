"""
Tahap 6D — Pengukuran Performa Query Healthcare Data Mart
Mengukur waktu eksekusi BQ1-BQ5 SEBELUM dan SESUDAH indexing (3 run per query).
Catatan: DuckDB menggunakan columnar storage; B-tree index mungkin tidak memberikan
         improvement signifikan, namun tetap diukur sesuai requirement tugas.
"""
import duckdb
import time
import statistics

DUCKDB_PATH = '/opt/airflow/duckdb/healthcare.duckdb'
# Index memerlukan koneksi read-write
con = duckdb.connect(DUCKDB_PATH, read_only=False)

BENCHMARKS = [
    ("BQ1 - Tren Kunjungan 12 Bulan", """
        SELECT t.tahun, t.bulan, t.nama_bulan,
               COUNT(*) AS total,
               SUM(CASE WHEN k.is_rawat_inap = TRUE THEN 1 ELSE 0 END) AS rawat_inap,
               SUM(CASE WHEN k.is_rawat_inap = FALSE THEN 1 ELSE 0 END) AS rawat_jalan
        FROM analytics_marts.fct_kunjungan k
        JOIN analytics_marts.dim_tanggal t ON k.tanggal_masuk_key = t.tanggal_key
        WHERE t.tanggal >= (CURRENT_DATE - INTERVAL '12 months')
        GROUP BY t.tahun, t.bulan, t.nama_bulan
        ORDER BY t.tahun, t.bulan
    """),
    ("BQ2 - Kunjungan per Departemen", """
        SELECT d.nama_departemen, COUNT(*) AS total
        FROM analytics_marts.fct_kunjungan k
        JOIN analytics_marts.dim_departemen d ON k.departemen_key = d.departemen_key
        WHERE k.departemen_key <> -1
        GROUP BY d.nama_departemen
        ORDER BY total DESC
    """),
    ("BQ3 - Avg LOS per Departemen", """
        SELECT d.nama_departemen,
               ROUND(AVG(k.durasi_kunjungan_jam), 2) AS avg_los_jam,
               COUNT(*) AS total
        FROM analytics_marts.fct_kunjungan k
        JOIN analytics_marts.dim_departemen d ON k.departemen_key = d.departemen_key
        WHERE k.is_rawat_inap = TRUE
          AND k.durasi_kunjungan_jam > 0
          AND k.departemen_key <> -1
        GROUP BY d.nama_departemen
        ORDER BY avg_los_jam DESC
    """),
    ("BQ4 - Top 5 Dokter per Bulan", """
        SELECT t.tahun, t.bulan, dr.nama_lengkap, COUNT(*) AS total,
               RANK() OVER (PARTITION BY t.tahun, t.bulan ORDER BY COUNT(*) DESC) AS rn
        FROM analytics_marts.fct_kunjungan k
        JOIN analytics_marts.dim_dokter  dr ON k.dokter_key        = dr.dokter_key
        JOIN analytics_marts.dim_tanggal t  ON k.tanggal_masuk_key = t.tanggal_key
        WHERE k.dokter_key <> -1 AND t.tanggal IS NOT NULL
        GROUP BY t.tahun, t.bulan, dr.nama_lengkap
        QUALIFY rn <= 5
        ORDER BY t.tahun, t.bulan, total DESC
        LIMIT 100
    """),
    ("BQ5 - Konversi Appointment", """
        SELECT t.tahun, t.bulan, t.nama_bulan,
               COUNT(*) AS total_dari_appointment,
               SUM(CASE WHEN k.is_appointment_converted = TRUE THEN 1 ELSE 0 END) AS total_converted,
               ROUND(
                   SUM(CASE WHEN k.is_appointment_converted = TRUE THEN 1 ELSE 0 END)
                   * 100.0 / NULLIF(COUNT(*), 0), 1
               ) AS conversion_rate_persen
        FROM analytics_marts.fct_kunjungan k
        JOIN analytics_marts.dim_tanggal t ON k.tanggal_masuk_key = t.tanggal_key
        WHERE k.is_dari_appointment = TRUE AND t.tanggal IS NOT NULL
        GROUP BY t.tahun, t.bulan, t.nama_bulan
        ORDER BY t.tahun, t.bulan
    """),
]

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_fct_kunjungan_tanggal  ON analytics_marts.fct_kunjungan(tanggal_masuk_key)",
    "CREATE INDEX IF NOT EXISTS idx_fct_kunjungan_dept     ON analytics_marts.fct_kunjungan(departemen_key)",
    "CREATE INDEX IF NOT EXISTS idx_fct_kunjungan_dokter   ON analytics_marts.fct_kunjungan(dokter_key)",
    "CREATE INDEX IF NOT EXISTS idx_fct_diagnosis_tanggal  ON analytics_marts.fct_diagnosis(tanggal_diagnosis_key)",
    "CREATE INDEX IF NOT EXISTS idx_fct_tindakan_tanggal   ON analytics_marts.fct_tindakan_medis(tanggal_tindakan_key)",
]


def benchmark_all(label, runs=3):
    """Jalankan semua BQ N kali, return list (label, min, avg, max) dalam ms."""
    results = []
    for bq_label, sql in BENCHMARKS:
        times = []
        for _ in range(runs):
            start = time.perf_counter()
            con.execute(sql).fetchdf()
            elapsed_ms = (time.perf_counter() - start) * 1000
            times.append(elapsed_ms)
        results.append((bq_label, min(times), statistics.mean(times), max(times)))
    return results


print("=" * 85)
print("HEALTHCARE DW — PENGUKURAN PERFORMA QUERY (3 runs per query)")
print("=" * 85)

# ─────────────────────────────────────────
# FASE 1: SEBELUM INDEXING
# ─────────────────────────────────────────
print("\nFASE 1: SEBELUM INDEXING")
print(f"\n{'Query':<40} {'Min (ms)':>10} {'Avg (ms)':>10} {'Max (ms)':>10}")
print("-" * 75)
before = benchmark_all("sebelum")
for lbl, mn, avg, mx in before:
    print(f"{lbl:<40} {mn:>10.1f} {avg:>10.1f} {mx:>10.1f}")

# ─────────────────────────────────────────
# BUAT INDEX
# ─────────────────────────────────────────
print("\nMembuat index pada fact tables...")
for idx_sql in INDEXES:
    idx_name = idx_sql.split("EXISTS")[1].split("ON")[0].strip()
    try:
        con.execute(idx_sql)
        print(f"  [OK] {idx_name}")
    except Exception as e:
        print(f"  [SKIP] {idx_name}: {e}")

print("\nMenjalankan PRAGMA analyze...")
try:
    con.execute("PRAGMA analyze")
    print("  [OK] PRAGMA analyze selesai")
except Exception as e:
    print(f"  [INFO] PRAGMA analyze: {e}")

# ─────────────────────────────────────────
# FASE 2: SESUDAH INDEXING
# ─────────────────────────────────────────
print("\nFASE 2: SESUDAH INDEXING")
print(f"\n{'Query':<40} {'Min (ms)':>10} {'Avg (ms)':>10} {'Max (ms)':>10}")
print("-" * 75)
after = benchmark_all("sesudah")
for lbl, mn, avg, mx in after:
    print(f"{lbl:<40} {mn:>10.1f} {avg:>10.1f} {mx:>10.1f}")

# ─────────────────────────────────────────
# PERBANDINGAN
# ─────────────────────────────────────────
print("\n" + "=" * 85)
print("PERBANDINGAN SEBELUM vs SESUDAH INDEXING")
print(f"\n{'Query':<40} {'Avg Sblm (ms)':>14} {'Avg Ssdh (ms)':>14} {'Improvement':>12}")
print("-" * 85)
for (lbl, _, avg_b, _), (_, _, avg_a, _) in zip(before, after):
    if avg_b > 0:
        imp = (avg_b - avg_a) / avg_b * 100
        imp_str = f"{imp:+.1f}%"
    else:
        imp_str = "N/A"
    print(f"{lbl:<40} {avg_b:>14.1f} {avg_a:>14.1f} {imp_str:>12}")
print("=" * 85)

print("""
CATATAN PERFORMA:
- DuckDB menggunakan columnar storage (Parquet-style) — tidak bergantung B-tree index
- Sebagian besar analitik query sudah dioptimasi secara native oleh DuckDB query planner
- Fact tables sudah dalam format TABLE (bukan VIEW), sehingga tidak ada overhead re-komputasi
- CREATE INDEX di DuckDB memberikan min-max index (zonemap), bukan B-tree tradisional
- Improvement mungkin kecil atau bahkan negatif (cache effect) — ini normal untuk DuckDB
- Untuk dataset besar (jutaan baris), manfaat index lebih terasa pada equality filter

INDEX YANG DIBUAT:
  idx_fct_kunjungan_tanggal  — fct_kunjungan(tanggal_masuk_key)
  idx_fct_kunjungan_dept     — fct_kunjungan(departemen_key)
  idx_fct_kunjungan_dokter   — fct_kunjungan(dokter_key)
  idx_fct_diagnosis_tanggal  — fct_diagnosis(tanggal_diagnosis_key)
  idx_fct_tindakan_tanggal   — fct_tindakan_medis(tanggal_tindakan_key)
""")

con.close()
