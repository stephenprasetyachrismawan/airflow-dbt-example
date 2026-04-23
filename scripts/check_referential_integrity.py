"""
Tahap 5 — Cek Referential Integrity Data Mart Healthcare
Memverifikasi bahwa semua foreign key di fact tables memiliki pasangan di dimension tables.
16 FK checks: fct_kunjungan (6), fct_diagnosis (5), fct_tindakan_medis (5)
"""
import duckdb
import sys

DUCKDB_PATH = '/opt/airflow/duckdb/healthcare.duckdb'
con = duckdb.connect(DUCKDB_PATH, read_only=True)

print("=" * 70)
print("HEALTHCARE DATA MART — CEK REFERENTIAL INTEGRITY")
print("=" * 70)

CHECKS = [
    # fct_kunjungan (6 FK)
    ("fct_kunjungan",      "pasien_key",           "dim_pasien",     "pasien_key"),
    ("fct_kunjungan",      "dokter_key",            "dim_dokter",     "dokter_key"),
    ("fct_kunjungan",      "ruangan_key",           "dim_ruangan",    "ruangan_key"),
    ("fct_kunjungan",      "departemen_key",        "dim_departemen", "departemen_key"),
    ("fct_kunjungan",      "tanggal_masuk_key",     "dim_tanggal",    "tanggal_key"),
    ("fct_kunjungan",      "tanggal_keluar_key",    "dim_tanggal",    "tanggal_key"),
    # fct_diagnosis (5 FK)
    ("fct_diagnosis",      "pasien_key",            "dim_pasien",     "pasien_key"),
    ("fct_diagnosis",      "dokter_key",            "dim_dokter",     "dokter_key"),
    ("fct_diagnosis",      "ruangan_key",           "dim_ruangan",    "ruangan_key"),
    ("fct_diagnosis",      "departemen_key",        "dim_departemen", "departemen_key"),
    ("fct_diagnosis",      "tanggal_diagnosis_key", "dim_tanggal",    "tanggal_key"),
    # fct_tindakan_medis (5 FK)
    ("fct_tindakan_medis", "pasien_key",            "dim_pasien",     "pasien_key"),
    ("fct_tindakan_medis", "dokter_key",            "dim_dokter",     "dokter_key"),
    ("fct_tindakan_medis", "ruangan_key",           "dim_ruangan",    "ruangan_key"),
    ("fct_tindakan_medis", "departemen_key",        "dim_departemen", "departemen_key"),
    ("fct_tindakan_medis", "tanggal_tindakan_key",  "dim_tanggal",    "tanggal_key"),
]

total_orphan = 0
all_pass = True

print(f"\n{'No':<4} {'Fact Table':<25} {'FK Column':<28} {'Dim Table':<20} {'Orphans':>8} {'Status'}")
print("-" * 100)

for i, (fact_tbl, fk_col, dim_tbl, dim_pk) in enumerate(CHECKS, 1):
    sql = f"""
        SELECT COUNT(*) AS orphan_count
        FROM analytics_marts.{fact_tbl} f
        LEFT JOIN analytics_marts.{dim_tbl} d
            ON f.{fk_col} = d.{dim_pk}
        WHERE d.{dim_pk} IS NULL
    """
    orphans = con.execute(sql).fetchone()[0]
    total_orphan += orphans
    status = "PASS" if orphans == 0 else "FAIL"
    if orphans > 0:
        all_pass = False
    print(f"{i:<4} {fact_tbl:<25} {fk_col:<28} {dim_tbl:<20} {orphans:>8,} {status}")

print("-" * 100)
print(f"\nTotal orphan records: {total_orphan:,}")
print(f"Hasil keseluruhan   : {'SEMUA PASS' if all_pass else 'ADA KEGAGALAN'}")

print("\n" + "=" * 70)
print("DETAIL STATISTIK FACT TABLES")
print("=" * 70)

stats = [
    ("fct_kunjungan",      "kunjungan_key"),
    ("fct_diagnosis",      "diagnosis_key"),
    ("fct_tindakan_medis", "tindakan_key"),
]
for tbl, pk in stats:
    n = con.execute(f"SELECT COUNT(*) FROM analytics_marts.{tbl}").fetchone()[0]
    n_unknown_pasien = con.execute(
        f"SELECT COUNT(*) FROM analytics_marts.{tbl} WHERE pasien_key = -1"
    ).fetchone()[0]
    print(f"  {tbl:<30}: {n:>10,} rows | unknown_pasien: {n_unknown_pasien:>8,}")

print("=" * 70)
con.close()

if not all_pass:
    sys.exit(1)
