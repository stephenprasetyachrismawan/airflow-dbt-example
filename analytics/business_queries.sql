-- ============================================================
-- HEALTHCARE DATA WAREHOUSE — BUSINESS QUERIES
-- Schema: marts | Database: DuckDB
-- ============================================================

-- ============================================================
-- BQ1: Tren Kunjungan per Bulan (12 bulan terakhir)
-- ============================================================
SELECT
    t.tahun,
    t.bulan,
    t.nama_bulan,
    COUNT(*)                                            AS total_kunjungan,
    SUM(CASE WHEN k.is_rawat_inap THEN 1 ELSE 0 END)   AS total_rawat_inap,
    SUM(CASE WHEN NOT k.is_rawat_inap THEN 1 ELSE 0 END) AS total_rawat_jalan
FROM analytics_marts.fct_kunjungan k
JOIN analytics_marts.dim_tanggal t ON k.tanggal_masuk_key = t.tanggal_key
WHERE k.kode_status_kunjungan = '1'   -- Admitted / Completed
  AND t.tanggal >= (CURRENT_DATE - INTERVAL '12 months')
GROUP BY t.tahun, t.bulan, t.nama_bulan
ORDER BY t.tahun ASC, t.bulan ASC;


-- ============================================================
-- BQ2: Departemen dengan Kunjungan Terbanyak dan Tersedikit
-- ============================================================
WITH dept_counts AS (
    SELECT
        d.nama_departemen,
        COUNT(*) AS total_kunjungan
    FROM analytics_marts.fct_kunjungan k
    JOIN analytics_marts.dim_departemen d ON k.departemen_key = d.departemen_key
    WHERE d.dep_id != 'UNKNOWN'
    GROUP BY d.nama_departemen
),
total AS (
    SELECT SUM(total_kunjungan) AS grand_total FROM dept_counts
)

SELECT
    dc.nama_departemen,
    dc.total_kunjungan,
    ROUND(dc.total_kunjungan * 100.0 / t.grand_total, 2)    AS persentase_dari_total,
    RANK() OVER (ORDER BY dc.total_kunjungan DESC)           AS rank_tertinggi,
    RANK() OVER (ORDER BY dc.total_kunjungan ASC)            AS rank_terendah
FROM dept_counts dc
CROSS JOIN total t
ORDER BY dc.total_kunjungan DESC;


-- ============================================================
-- BQ3: Rata-rata Length of Stay (LOS) per Departemen
-- ============================================================
SELECT
    d.nama_departemen,
    ROUND(AVG(k.durasi_kunjungan_jam), 2)   AS avg_los_jam,
    ROUND(MIN(k.durasi_kunjungan_jam), 2)   AS min_los_jam,
    ROUND(MAX(k.durasi_kunjungan_jam), 2)   AS max_los_jam,
    COUNT(*)                                AS total_kunjungan_rawat_inap
FROM analytics_marts.fct_kunjungan k
JOIN analytics_marts.dim_departemen d ON k.departemen_key = d.departemen_key
WHERE k.is_rawat_inap = TRUE
  AND k.durasi_kunjungan_jam IS NOT NULL
  AND d.dep_id != 'UNKNOWN'
GROUP BY d.nama_departemen
ORDER BY avg_los_jam DESC;


-- ============================================================
-- BQ4: Dokter dengan Beban Kerja Tertinggi per Bulan (Top 5)
-- ============================================================
WITH dokter_per_bulan AS (
    SELECT
        t.tahun,
        t.bulan,
        t.nama_bulan,
        d.nama_lengkap      AS nama_dokter,
        d.nama_departemen   AS nama_departemen_dokter,
        COUNT(*)            AS total_kunjungan_ditangani,
        RANK() OVER (
            PARTITION BY t.tahun, t.bulan
            ORDER BY COUNT(*) DESC
        ) AS rank_dokter
    FROM analytics_marts.fct_kunjungan k
    JOIN analytics_marts.dim_dokter d     ON k.dokter_key = d.dokter_key
    JOIN analytics_marts.dim_tanggal t    ON k.tanggal_masuk_key = t.tanggal_key
    WHERE k.dokter_key != -1
      AND k.kode_status_kunjungan = '1'
      AND t.tanggal IS NOT NULL
    GROUP BY t.tahun, t.bulan, t.nama_bulan, d.nama_lengkap, d.nama_departemen
)

SELECT
    tahun,
    bulan,
    nama_bulan,
    nama_dokter,
    nama_departemen_dokter,
    total_kunjungan_ditangani
FROM dokter_per_bulan
WHERE rank_dokter <= 5
ORDER BY tahun ASC, bulan ASC, total_kunjungan_ditangani DESC;


-- ============================================================
-- BQ5: Konversi Appointment ke Kunjungan Aktual
-- ============================================================
SELECT
    t.tahun,
    t.bulan,
    t.nama_bulan,
    COUNT(*)                                                AS total_kunjungan_dari_appt,
    SUM(CASE WHEN k.is_appointment_converted THEN 1 ELSE 0 END) AS total_converted,
    ROUND(
        SUM(CASE WHEN k.is_appointment_converted THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    )                                                       AS conversion_rate_persen
FROM analytics_marts.fct_kunjungan k
JOIN analytics_marts.dim_tanggal t ON k.tanggal_masuk_key = t.tanggal_key
WHERE k.is_dari_appointment = TRUE
  AND t.tanggal IS NOT NULL
GROUP BY t.tahun, t.bulan, t.nama_bulan
ORDER BY t.tahun ASC, t.bulan ASC;
