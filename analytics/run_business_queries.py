"""
Tahap 6 — Menjalankan Business Queries Healthcare Data Mart
BQ1: Tren kunjungan per bulan (12 bulan terakhir)
BQ2: Departemen terbanyak dan tersedikit kunjungan
BQ3: Rata-rata Length of Stay per departemen (rawat inap)
BQ4: Top 5 dokter beban tertinggi per bulan
BQ5: Konversi appointment ke kunjungan aktual
"""
import duckdb

DUCKDB_PATH = '/opt/airflow/duckdb/healthcare.duckdb'
con = duckdb.connect(DUCKDB_PATH, read_only=True)


def run_query(title, sql):
    print(f"\n{'=' * 75}")
    print(f"  {title}")
    print(f"{'=' * 75}")
    df = con.execute(sql).fetchdf()
    if df.empty:
        print("  (tidak ada data)")
    else:
        print(df.to_string(index=False))
        print(f"\n  -> {len(df)} baris")
    print()


# ─────────────────────────────────────────
# BQ1: Tren Kunjungan per Bulan (12 bulan terakhir)
# ─────────────────────────────────────────
BQ1 = """
    SELECT
        t.tahun,
        t.bulan,
        t.nama_bulan,
        COUNT(*) AS total_kunjungan,
        SUM(CASE WHEN k.is_rawat_inap = TRUE  THEN 1 ELSE 0 END) AS total_rawat_inap,
        SUM(CASE WHEN k.is_rawat_inap = FALSE THEN 1 ELSE 0 END) AS total_rawat_jalan,
        ROUND(
            SUM(CASE WHEN k.is_rawat_inap = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
        ) AS pct_rawat_inap
    FROM analytics_marts.fct_kunjungan k
    JOIN analytics_marts.dim_tanggal t ON k.tanggal_masuk_key = t.tanggal_key
    WHERE t.tanggal >= (CURRENT_DATE - INTERVAL '12 months')
    GROUP BY t.tahun, t.bulan, t.nama_bulan
    ORDER BY t.tahun, t.bulan
"""

# ─────────────────────────────────────────
# BQ2: Departemen Terbanyak dan Tersedikit Kunjungan
# ─────────────────────────────────────────
BQ2 = """
    SELECT
        d.nama_departemen,
        COUNT(*) AS total_kunjungan,
        ROUND(
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1
        ) AS persentase,
        RANK() OVER (ORDER BY COUNT(*) DESC) AS rank_tertinggi,
        RANK() OVER (ORDER BY COUNT(*) ASC)  AS rank_terendah
    FROM analytics_marts.fct_kunjungan k
    JOIN analytics_marts.dim_departemen d ON k.departemen_key = d.departemen_key
    WHERE k.departemen_key <> -1
    GROUP BY d.nama_departemen
    ORDER BY total_kunjungan DESC
"""

# ─────────────────────────────────────────
# BQ3: Rata-rata LOS per Departemen (Rawat Inap)
# ─────────────────────────────────────────
BQ3 = """
    SELECT
        d.nama_departemen,
        COUNT(*) AS total_rawat_inap,
        ROUND(AVG(k.durasi_kunjungan_jam), 2) AS avg_los_jam,
        ROUND(AVG(k.durasi_kunjungan_jam) / 24.0, 2) AS avg_los_hari,
        ROUND(MIN(k.durasi_kunjungan_jam), 1) AS min_jam,
        ROUND(MAX(k.durasi_kunjungan_jam), 1) AS max_jam
    FROM analytics_marts.fct_kunjungan k
    JOIN analytics_marts.dim_departemen d ON k.departemen_key = d.departemen_key
    WHERE k.is_rawat_inap = TRUE
      AND k.durasi_kunjungan_jam IS NOT NULL
      AND k.durasi_kunjungan_jam > 0
      AND k.departemen_key <> -1
    GROUP BY d.nama_departemen
    ORDER BY avg_los_jam DESC
"""

# ─────────────────────────────────────────
# BQ4: Top 5 Dokter Beban Tertinggi per Bulan
# ─────────────────────────────────────────
BQ4 = """
    SELECT
        t.tahun,
        t.bulan,
        t.nama_bulan,
        dr.nama_lengkap AS nama_dokter,
        dr.nama_departemen,
        COUNT(*) AS total_kunjungan,
        RANK() OVER (PARTITION BY t.tahun, t.bulan ORDER BY COUNT(*) DESC) AS rank_bulan
    FROM analytics_marts.fct_kunjungan k
    JOIN analytics_marts.dim_dokter  dr ON k.dokter_key       = dr.dokter_key
    JOIN analytics_marts.dim_tanggal t  ON k.tanggal_masuk_key = t.tanggal_key
    WHERE k.dokter_key <> -1
      AND t.tanggal IS NOT NULL
    GROUP BY t.tahun, t.bulan, t.nama_bulan, dr.nama_lengkap, dr.nama_departemen
    QUALIFY rank_bulan <= 5
    ORDER BY t.tahun, t.bulan, total_kunjungan DESC
    LIMIT 100
"""

# ─────────────────────────────────────────
# BQ5: Konversi Appointment -> Kunjungan Aktual
# ─────────────────────────────────────────
BQ5 = """
    SELECT
        t.tahun,
        t.bulan,
        t.nama_bulan,
        COUNT(*) AS total_dari_appointment,
        SUM(CASE WHEN k.is_appointment_converted = TRUE THEN 1 ELSE 0 END) AS total_converted,
        ROUND(
            SUM(CASE WHEN k.is_appointment_converted = TRUE THEN 1 ELSE 0 END)
            * 100.0 / NULLIF(COUNT(*), 0), 1
        ) AS conversion_rate_persen
    FROM analytics_marts.fct_kunjungan k
    JOIN analytics_marts.dim_tanggal t ON k.tanggal_masuk_key = t.tanggal_key
    WHERE k.is_dari_appointment = TRUE
      AND t.tanggal IS NOT NULL
    GROUP BY t.tahun, t.bulan, t.nama_bulan
    ORDER BY t.tahun, t.bulan
"""

print("=" * 75)
print("  HEALTHCARE DATA MART — BUSINESS QUERIES (BQ1-BQ5)")
print("=" * 75)

run_query("BQ1: Tren Kunjungan per Bulan (12 Bulan Terakhir)", BQ1)
run_query("BQ2: Departemen Terbanyak dan Tersedikit Kunjungan", BQ2)
run_query("BQ3: Rata-rata Length of Stay per Departemen (Rawat Inap)", BQ3)
run_query("BQ4: Top 5 Dokter Beban Tertinggi per Bulan", BQ4)
run_query("BQ5: Konversi Appointment ke Kunjungan Aktual", BQ5)

con.close()
