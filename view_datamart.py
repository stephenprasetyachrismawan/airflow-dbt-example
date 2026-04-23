"""
Healthcare DW -- Interactive Data Mart Viewer
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
+======================================================+
|      HEALTHCARE DATA MART -- INTERACTIVE VIEWER      |
+======================================================+
|  DIMENSI                                             |
|  1. dim_tanggal      -- Dimensi Tanggal (2020-2027)  |
|  2. dim_pasien       -- Dimensi Pasien               |
|  3. dim_dokter       -- Dimensi Dokter               |
|  4. dim_departemen   -- Dimensi Departemen           |
|  5. dim_ruangan      -- Dimensi Ruangan              |
|                                                      |
|  FAKTA                                               |
|  6. fct_kunjungan      -- Fakta Kunjungan            |
|  7. fct_diagnosis      -- Fakta Diagnosis            |
|  8. fct_tindakan_medis -- Fakta Tindakan Medis       |
|                                                      |
|  BUSINESS QUERIES                                    |
|  9.  BQ1: Tren Kunjungan per Bulan                   |
|  10. BQ2: Kunjungan per Departemen                   |
|  11. BQ3: Avg LOS per Departemen (Rawat Inap)        |
|  12. BQ4: Top 5 Dokter Beban Tertinggi per Bulan     |
|  13. BQ5: Konversi Appointment -> Kunjungan          |
|                                                      |
|  LAINNYA                                             |
|  14. Row counts semua tabel                          |
|  15. Custom SQL query                                |
|  0.  Keluar                                          |
+======================================================+
"""

QUERIES = {
    '1': ("dim_tanggal -- 10 baris pertama",
          "SELECT tanggal_key, tanggal, hari, bulan, tahun, nama_hari, nama_bulan, is_weekend FROM analytics_marts.dim_tanggal WHERE tanggal IS NOT NULL LIMIT 10"),
    '2': ("dim_pasien -- 10 baris pertama",
          "SELECT pasien_key, pat_id, nama_lengkap, jenis_kelamin, tanggal_lahir, golongan_darah, status_pernikahan FROM analytics_marts.dim_pasien WHERE pasien_key <> -1 LIMIT 10"),
    '3': ("dim_dokter -- semua",
          "SELECT dokter_key, stf_id, nama_lengkap, nama_departemen, deskripsi_peran, status_karyawan FROM analytics_marts.dim_dokter WHERE dokter_key <> -1"),
    '4': ("dim_departemen -- semua",
          "SELECT * FROM analytics_marts.dim_departemen WHERE departemen_key <> -1"),
    '5': ("dim_ruangan -- 10 baris pertama",
          "SELECT ruangan_key, rom_id, nama_departemen, deskripsi_tipe, deskripsi_tujuan, kapasitas FROM analytics_marts.dim_ruangan WHERE ruangan_key <> -1 LIMIT 10"),
    '6': ("fct_kunjungan -- 10 baris pertama",
          "SELECT kunjungan_key, pasien_key, dokter_key, departemen_key, tanggal_masuk_key, is_rawat_inap, durasi_kunjungan_jam, deskripsi_status_kunjungan FROM analytics_marts.fct_kunjungan LIMIT 10"),
    '7': ("fct_diagnosis -- 10 baris pertama",
          "SELECT diagnosis_key, pasien_key, dokter_key, departemen_key, tanggal_diagnosis_key, kode_diagnosis, deskripsi_diagnosis, is_confirmed, is_diagnosis_utama FROM analytics_marts.fct_diagnosis LIMIT 10"),
    '8': ("fct_tindakan_medis -- 10 baris pertama",
          "SELECT tindakan_key, pasien_key, dokter_key, departemen_key, tanggal_tindakan_key, deskripsi_tipe_tindakan, is_selesai, is_operasi, is_obat FROM analytics_marts.fct_tindakan_medis LIMIT 10"),
    '9': ("BQ1: Tren Kunjungan per Bulan (12 Bulan Terakhir)", """
        SELECT t.tahun, t.bulan, t.nama_bulan,
               COUNT(*) AS total_kunjungan,
               SUM(CASE WHEN k.is_rawat_inap = TRUE  THEN 1 ELSE 0 END) AS rawat_inap,
               SUM(CASE WHEN k.is_rawat_inap = FALSE THEN 1 ELSE 0 END) AS rawat_jalan,
               ROUND(SUM(CASE WHEN k.is_rawat_inap = TRUE THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct_inap
        FROM analytics_marts.fct_kunjungan k
        JOIN analytics_marts.dim_tanggal t ON k.tanggal_masuk_key = t.tanggal_key
        WHERE t.tanggal >= (CURRENT_DATE - INTERVAL '12 months')
        GROUP BY t.tahun, t.bulan, t.nama_bulan
        ORDER BY t.tahun, t.bulan
    """),
    '10': ("BQ2: Kunjungan per Departemen", """
        SELECT d.nama_departemen, COUNT(*) AS total_kunjungan,
               ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER (),1) AS pct
        FROM analytics_marts.fct_kunjungan k
        JOIN analytics_marts.dim_departemen d ON k.departemen_key = d.departemen_key
        WHERE k.departemen_key <> -1
        GROUP BY d.nama_departemen
        ORDER BY total_kunjungan DESC
    """),
    '11': ("BQ3: Avg LOS per Departemen (Rawat Inap)", """
        SELECT d.nama_departemen, COUNT(*) AS total_rawat_inap,
               ROUND(AVG(k.durasi_kunjungan_jam),2) AS avg_los_jam,
               ROUND(AVG(k.durasi_kunjungan_jam)/24.0,2) AS avg_los_hari,
               ROUND(MIN(k.durasi_kunjungan_jam),1) AS min_jam,
               ROUND(MAX(k.durasi_kunjungan_jam),1) AS max_jam
        FROM analytics_marts.fct_kunjungan k
        JOIN analytics_marts.dim_departemen d ON k.departemen_key = d.departemen_key
        WHERE k.is_rawat_inap = TRUE
          AND k.durasi_kunjungan_jam > 0
          AND k.departemen_key <> -1
        GROUP BY d.nama_departemen
        ORDER BY avg_los_jam DESC
    """),
    '12': ("BQ4: Top 5 Dokter Beban Tertinggi per Bulan", """
        SELECT t.tahun, t.bulan, t.nama_bulan,
               dr.nama_lengkap AS nama_dokter, dr.nama_departemen,
               COUNT(*) AS total,
               RANK() OVER (PARTITION BY t.tahun,t.bulan ORDER BY COUNT(*) DESC) AS rank_bulan
        FROM analytics_marts.fct_kunjungan k
        JOIN analytics_marts.dim_dokter  dr ON k.dokter_key        = dr.dokter_key
        JOIN analytics_marts.dim_tanggal t  ON k.tanggal_masuk_key = t.tanggal_key
        WHERE k.dokter_key <> -1 AND t.tanggal IS NOT NULL
        GROUP BY t.tahun,t.bulan,t.nama_bulan,dr.nama_lengkap,dr.nama_departemen
        QUALIFY rank_bulan<=5
        ORDER BY t.tahun,t.bulan,total DESC LIMIT 60
    """),
    '13': ("BQ5: Konversi Appointment -> Kunjungan Aktual", """
        SELECT t.tahun, t.bulan, t.nama_bulan,
               COUNT(*) AS total_dari_appointment,
               SUM(CASE WHEN k.is_appointment_converted=TRUE THEN 1 ELSE 0 END) AS total_converted,
               ROUND(SUM(CASE WHEN k.is_appointment_converted=TRUE THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0),1) AS conversion_rate_persen
        FROM analytics_marts.fct_kunjungan k
        JOIN analytics_marts.dim_tanggal t ON k.tanggal_masuk_key=t.tanggal_key
        WHERE k.is_dari_appointment=TRUE AND t.tanggal IS NOT NULL
        GROUP BY t.tahun,t.bulan,t.nama_bulan
        ORDER BY t.tahun,t.bulan
    """),
    '14': ("Row counts semua tabel", None),
}


def show_row_counts():
    tables = [
        'dim_tanggal', 'dim_pasien', 'dim_dokter', 'dim_departemen', 'dim_ruangan',
        'fct_kunjungan', 'fct_diagnosis', 'fct_tindakan_medis'
    ]
    print("\n" + "=" * 55)
    print("ROW COUNTS -- analytics_marts")
    print("=" * 55)
    total = 0
    for t in tables:
        n = con.execute(f"SELECT COUNT(*) FROM analytics_marts.{t}").fetchone()[0]
        total += n
        layer = "DIM" if t.startswith('dim') else "FCT"
        print(f"  [{layer}] {t:<35}: {n:>8,}")
    print("-" * 55)
    print(f"  {'TOTAL':<40}: {total:>8,}")
    print("=" * 55)


def run_query(title, sql):
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")
    df = con.execute(sql).fetchdf()
    if df.empty:
        print("  (tidak ada data)")
    else:
        print(df.to_string(index=False))
        print(f"\n  -> {len(df)} rows")
    print()


def main():
    print(MENU)
    while True:
        try:
            choice = input("Pilih menu [0-15]: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nKeluar.")
            break

        if choice == '0':
            print("Selesai.")
            break
        elif choice == '14':
            show_row_counts()
        elif choice == '15':
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
                show_row_counts()
        else:
            print("Pilihan tidak valid.")

        print(MENU)


if __name__ == '__main__':
    main()
    con.close()
