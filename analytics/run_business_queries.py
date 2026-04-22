"""
Tahap 6A — Eksekusi Business Queries & Simpan Output
Menjalankan BQ1–BQ5 dan menyimpan hasilnya ke CSV dan teks untuk laporan.

Business Questions:
- BQ1: Tren kunjungan per bulan (rawat inap vs rawat jalan)
- BQ2: Departemen dengan kunjungan terbanyak dan tersedikit
- BQ3: Rata-rata Length of Stay (LOS) per departemen
- BQ4: Dokter dengan beban kerja tertinggi per bulan (Top 5)
- BQ5: Distribusi status pembayaran billing per bulan
"""
import duckdb
import pandas as pd
import time
from datetime import datetime
import os

DUCKDB_PATH = '/opt/airflow/duckdb/healthcare.duckdb'
OUTPUT_DIR = '/opt/airflow/analytics/query_results'
os.makedirs(OUTPUT_DIR, exist_ok=True)

con = duckdb.connect(DUCKDB_PATH, read_only=True)

QUERIES = {
    'BQ1_tren_kunjungan_per_bulan': {
        'description': 'Tren kunjungan per bulan dalam 12 bulan terakhir (rawat inap vs rawat jalan)',
        'sql': """
            SELECT
                d.year                                                          AS tahun,
                d.month                                                         AS bulan,
                d.month_name                                                    AS nama_bulan,
                COUNT(*)                                                        AS total_kunjungan,
                SUM(CASE WHEN v.is_inpatient THEN 1 ELSE 0 END)               AS total_rawat_inap,
                SUM(CASE WHEN NOT v.is_inpatient THEN 1 ELSE 0 END)           AS total_rawat_jalan,
                ROUND(
                    SUM(CASE WHEN v.is_inpatient THEN 1 ELSE 0 END) * 100.0
                    / NULLIF(COUNT(*), 0), 2
                )                                                               AS pct_rawat_inap
            FROM analytics_marts.fct_visit v
            JOIN analytics_marts.dim_date d ON v.visit_date_key = d.date_key
            WHERE d.calendar_date IS NOT NULL
              AND d.calendar_date >= (CURRENT_DATE - INTERVAL '12 months')
            GROUP BY d.year, d.month, d.month_name
            ORDER BY d.year ASC, d.month ASC
        """
    },
    'BQ2_departemen_terbanyak_tersedikit': {
        'description': 'Departemen dengan kunjungan terbanyak dan tersedikit',
        'sql': """
            WITH dept_counts AS (
                SELECT
                    dep.department_name                AS nama_departemen,
                    COUNT(*)                           AS total_kunjungan
                FROM analytics_marts.fct_visit v
                JOIN analytics_marts.dim_department dep
                    ON v.department_key = dep.department_key
                WHERE dep.department_key != -1
                GROUP BY dep.department_name
            ),
            total AS (
                SELECT SUM(total_kunjungan) AS grand_total FROM dept_counts
            )
            SELECT
                dc.nama_departemen,
                dc.total_kunjungan,
                ROUND(dc.total_kunjungan * 100.0 / t.grand_total, 2)           AS persentase,
                RANK() OVER (ORDER BY dc.total_kunjungan DESC)                 AS rank_terbanyak,
                RANK() OVER (ORDER BY dc.total_kunjungan ASC)                  AS rank_tersedikit
            FROM dept_counts dc
            CROSS JOIN total t
            ORDER BY dc.total_kunjungan DESC
        """
    },
    'BQ3_avg_los_per_departemen': {
        'description': 'Rata-rata Length of Stay (LOS) pasien rawat inap per departemen',
        'sql': """
            SELECT
                dep.department_name                                             AS nama_departemen,
                COUNT(*)                                                        AS total_rawat_inap,
                ROUND(AVG(v.length_of_stay_hours), 2)                          AS avg_los_jam,
                ROUND(AVG(v.length_of_stay_hours) / 24.0, 2)                  AS avg_los_hari,
                ROUND(MIN(v.length_of_stay_hours), 2)                          AS min_los_jam,
                ROUND(MAX(v.length_of_stay_hours), 2)                          AS max_los_jam,
                ROUND(STDDEV(v.length_of_stay_hours), 2)                       AS stddev_los_jam
            FROM analytics_marts.fct_visit v
            JOIN analytics_marts.dim_department dep
                ON v.department_key = dep.department_key
            WHERE v.is_inpatient = TRUE
              AND v.length_of_stay_hours IS NOT NULL
              AND v.length_of_stay_hours > 0
              AND dep.department_key != -1
            GROUP BY dep.department_name
            ORDER BY avg_los_jam DESC
        """
    },
    'BQ4_dokter_beban_tertinggi': {
        'description': 'Top 5 dokter dengan beban kerja tertinggi per bulan',
        'sql': """
            WITH dokter_per_bulan AS (
                SELECT
                    d.year                                                      AS tahun,
                    d.month                                                     AS bulan,
                    d.month_name                                                AS nama_bulan,
                    dr.full_name                                                AS nama_dokter,
                    v.department_name                                           AS departemen_dokter,
                    COUNT(*)                                                    AS total_kunjungan,
                    RANK() OVER (
                        PARTITION BY d.year, d.month
                        ORDER BY COUNT(*) DESC
                    )                                                           AS rank_bulan
                FROM analytics_marts.fct_visit v
                JOIN analytics_marts.dim_doctor dr   ON v.doctor_key = dr.doctor_key
                JOIN analytics_marts.dim_date d      ON v.visit_date_key = d.date_key
                WHERE v.doctor_key != -1
                  AND d.calendar_date IS NOT NULL
                GROUP BY d.year, d.month, d.month_name, dr.full_name, v.department_name
            )
            SELECT
                tahun,
                bulan,
                nama_bulan,
                nama_dokter,
                departemen_dokter,
                total_kunjungan,
                rank_bulan
            FROM dokter_per_bulan
            WHERE rank_bulan <= 5
            ORDER BY tahun ASC, bulan ASC, total_kunjungan DESC
            LIMIT 100
        """
    },
    'BQ5_distribusi_pembayaran': {
        'description': 'Distribusi status pembayaran billing per bulan',
        'sql': """
            SELECT
                d.year                                                          AS tahun,
                d.month                                                         AS bulan,
                d.month_name                                                    AS nama_bulan,
                COUNT(*)                                                        AS total_billing,
                SUM(CASE WHEN b.payment_status = 'Paid' THEN 1 ELSE 0 END)    AS total_paid,
                SUM(CASE WHEN b.payment_status = 'Partially Paid'
                         THEN 1 ELSE 0 END)                                    AS total_partial,
                SUM(CASE WHEN b.payment_status = 'Outstanding'
                         THEN 1 ELSE 0 END)                                    AS total_outstanding,
                ROUND(SUM(b.billing_amount), 2)                                AS total_tagihan_idr,
                ROUND(SUM(b.payment_amount), 2)                                AS total_bayar_idr,
                ROUND(SUM(b.outstanding_amount), 2)                            AS total_sisa_idr,
                ROUND(
                    SUM(CASE WHEN b.payment_status = 'Paid' THEN 1 ELSE 0 END)
                    * 100.0 / NULLIF(COUNT(*), 0), 2
                )                                                               AS pct_paid
            FROM analytics_marts.fct_billing b
            JOIN analytics_marts.dim_date d ON b.billing_date_key = d.date_key
            WHERE d.calendar_date IS NOT NULL
            GROUP BY d.year, d.month, d.month_name
            ORDER BY d.year ASC, d.month ASC
        """
    }
}

print("=" * 80)
print("HEALTHCARE DW — EKSEKUSI BUSINESS QUERIES")
print(f"Dijalankan: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)

summary_log = []

for query_id, query_info in QUERIES.items():
    print(f"\n{'=' * 80}")
    print(f"  {query_id}")
    print(f"  {query_info['description']}")
    print(f"{'=' * 80}")

    start = time.time()
    try:
        df = con.execute(query_info['sql']).fetchdf()
        elapsed = time.time() - start

        print(df.to_string(index=False))
        print(f"\n  → {len(df)} rows dikembalikan dalam {elapsed:.3f} detik")

        csv_path = f"{OUTPUT_DIR}/{query_id}.csv"
        df.to_csv(csv_path, index=False)
        print(f"  → Disimpan ke: {csv_path}")

        summary_log.append({
            'query': query_id,
            'rows': len(df),
            'elapsed_sec': round(elapsed, 3),
            'status': 'SUCCESS'
        })

    except Exception as e:
        elapsed = time.time() - start
        print(f"  → ERROR: {e}")
        summary_log.append({
            'query': query_id,
            'rows': 0,
            'elapsed_sec': round(elapsed, 3),
            'status': f'ERROR: {e}'
        })

print("\n" + "=" * 80)
print("RINGKASAN EKSEKUSI")
print("=" * 80)
print(f"{'Query':<45} {'Rows':>8} {'Waktu(s)':>10} {'Status':>12}")
print("-" * 80)
for s in summary_log:
    print(f"{s['query']:<45} {s['rows']:>8} {s['elapsed_sec']:>10} {s['status']:>12}")

con.close()
print("\nSemua query selesai. Hasil tersimpan di:", OUTPUT_DIR)
