"""
Tahap 6 — Data Quality Check Healthcare Data Mart
Memeriksa completeness, uniqueness, not-null, referential integrity,
validity range, consistency unknown member, dan distribusi data.
"""
import duckdb

DUCKDB_PATH = '/opt/airflow/duckdb/healthcare.duckdb'
con = duckdb.connect(DUCKDB_PATH, read_only=True)

PASS = "PASS"
FAIL = "FAIL"
WARN = "WARN"

results = []


def check(label, sql, expect_zero=True, warn_only=False):
    val = con.execute(sql).fetchone()[0]
    if expect_zero:
        status = PASS if val == 0 else (WARN if warn_only else FAIL)
    else:
        status = PASS if val > 0 else FAIL
    results.append((label, val, status))
    return val, status


print("=" * 75)
print("HEALTHCARE DATA MART — DATA QUALITY CHECK")
print("=" * 75)

# ─────────────────────────────────────────
# [1] COMPLETENESS
# ─────────────────────────────────────────
print("\n[1] COMPLETENESS — Semua tabel harus berisi data")
TABLES = [
    "dim_tanggal", "dim_pasien", "dim_dokter", "dim_departemen", "dim_ruangan",
    "fct_kunjungan", "fct_diagnosis", "fct_tindakan_medis"
]
for t in TABLES:
    n = con.execute(f"SELECT COUNT(*) FROM analytics_marts.{t}").fetchone()[0]
    status = PASS if n > 0 else FAIL
    results.append((f"{t} tidak kosong", n, status))
    print(f"  {t:<35}: {n:>12,} rows  [{status}]")

# ─────────────────────────────────────────
# [2] UNIQUENESS
# ─────────────────────────────────────────
print("\n[2] UNIQUENESS — Surrogate keys harus unik")
SK_CHECKS = [
    ("dim_tanggal",        "tanggal_key"),
    ("dim_pasien",         "pasien_key"),
    ("dim_dokter",         "dokter_key"),
    ("dim_departemen",     "departemen_key"),
    ("dim_ruangan",        "ruangan_key"),
    ("fct_kunjungan",      "kunjungan_key"),
    ("fct_diagnosis",      "diagnosis_key"),
    ("fct_tindakan_medis", "tindakan_key"),
]
for tbl, col in SK_CHECKS:
    val, status = check(
        f"{tbl}.{col} unik",
        f"SELECT COUNT(*) - COUNT(DISTINCT {col}) FROM analytics_marts.{tbl}"
    )
    print(f"  {tbl}.{col:<30}: duplikat={val:>8,}  [{status}]")

# ─────────────────────────────────────────
# [3] NOT NULL
# ─────────────────────────────────────────
print("\n[3] NOT NULL — Primary keys tidak boleh NULL")
for tbl, col in SK_CHECKS:
    val, status = check(
        f"{tbl}.{col} not null",
        f"SELECT COUNT(*) FROM analytics_marts.{tbl} WHERE {col} IS NULL"
    )
    print(f"  {tbl}.{col:<30}: nulls={val:>8,}  [{status}]")

# ─────────────────────────────────────────
# [4] REFERENTIAL INTEGRITY
# ─────────────────────────────────────────
print("\n[4] REFERENTIAL INTEGRITY — FK harus ada pasangannya di dimensi")
FK_CHECKS = [
    ("fct_kunjungan",      "pasien_key",           "dim_pasien",     "pasien_key"),
    ("fct_kunjungan",      "dokter_key",            "dim_dokter",     "dokter_key"),
    ("fct_kunjungan",      "ruangan_key",           "dim_ruangan",    "ruangan_key"),
    ("fct_kunjungan",      "departemen_key",        "dim_departemen", "departemen_key"),
    ("fct_kunjungan",      "tanggal_masuk_key",     "dim_tanggal",    "tanggal_key"),
    ("fct_kunjungan",      "tanggal_keluar_key",    "dim_tanggal",    "tanggal_key"),
    ("fct_diagnosis",      "pasien_key",            "dim_pasien",     "pasien_key"),
    ("fct_diagnosis",      "dokter_key",            "dim_dokter",     "dokter_key"),
    ("fct_diagnosis",      "ruangan_key",           "dim_ruangan",    "ruangan_key"),
    ("fct_diagnosis",      "departemen_key",        "dim_departemen", "departemen_key"),
    ("fct_diagnosis",      "tanggal_diagnosis_key", "dim_tanggal",    "tanggal_key"),
    ("fct_tindakan_medis", "pasien_key",            "dim_pasien",     "pasien_key"),
    ("fct_tindakan_medis", "dokter_key",            "dim_dokter",     "dokter_key"),
    ("fct_tindakan_medis", "ruangan_key",           "dim_ruangan",    "ruangan_key"),
    ("fct_tindakan_medis", "departemen_key",        "dim_departemen", "departemen_key"),
    ("fct_tindakan_medis", "tanggal_tindakan_key",  "dim_tanggal",    "tanggal_key"),
]
for fact_tbl, fk_col, dim_tbl, dim_pk in FK_CHECKS:
    val, status = check(
        f"{fact_tbl}.{fk_col}",
        f"""SELECT COUNT(*) FROM analytics_marts.{fact_tbl} f
            LEFT JOIN analytics_marts.{dim_tbl} d ON f.{fk_col} = d.{dim_pk}
            WHERE d.{dim_pk} IS NULL"""
    )
    print(f"  {fact_tbl}.{fk_col:<28} -> {dim_tbl}: orphans={val:>6,}  [{status}]")

# ─────────────────────────────────────────
# [5] VALIDITY
# ─────────────────────────────────────────
print("\n[5] VALIDITY — Range dan nilai valid")

val, status = check(
    "dim_tanggal.bulan in [1-12]",
    "SELECT COUNT(*) FROM analytics_marts.dim_tanggal WHERE tanggal IS NOT NULL AND (bulan < 1 OR bulan > 12)"
)
print(f"  dim_tanggal.bulan di luar [1-12]         : {val:>8,}  [{status}]")

val, status = check(
    "dim_tanggal.tahun in [2020-2027]",
    "SELECT COUNT(*) FROM analytics_marts.dim_tanggal WHERE tanggal IS NOT NULL AND (tahun < 2020 OR tahun > 2027)"
)
print(f"  dim_tanggal.tahun di luar [2020-2027]    : {val:>8,}  [{status}]")

val, status = check(
    "fct_kunjungan.durasi_kunjungan_jam >= 0",
    "SELECT COUNT(*) FROM analytics_marts.fct_kunjungan WHERE durasi_kunjungan_jam IS NOT NULL AND durasi_kunjungan_jam < 0"
)
print(f"  fct_kunjungan.durasi_kunjungan_jam < 0   : {val:>8,}  [{status}]")

val, status = check(
    "fct_diagnosis.total_biaya_diagnosis >= 0",
    "SELECT COUNT(*) FROM analytics_marts.fct_diagnosis WHERE total_biaya_diagnosis IS NOT NULL AND total_biaya_diagnosis < 0"
)
print(f"  fct_diagnosis.total_biaya_diagnosis < 0  : {val:>8,}  [{status}]")

val, status = check(
    "fct_tindakan.total_biaya_tindakan >= 0",
    "SELECT COUNT(*) FROM analytics_marts.fct_tindakan_medis WHERE total_biaya_tindakan IS NOT NULL AND total_biaya_tindakan < 0"
)
print(f"  fct_tindakan.total_biaya_tindakan < 0    : {val:>8,}  [{status}]")

# ─────────────────────────────────────────
# [6] CONSISTENCY — unknown member
# WARN bukan FAIL jika tidak ada -1 (data asli mungkin sudah clean)
# ─────────────────────────────────────────
print("\n[6] CONSISTENCY — Unknown member (-1) di dimensi [WARN jika tidak ada]")
UNKNOWN_CHECKS = [
    ("dim_tanggal",    "tanggal_key"),
    ("dim_pasien",     "pasien_key"),
    ("dim_dokter",     "dokter_key"),
    ("dim_departemen", "departemen_key"),
    ("dim_ruangan",    "ruangan_key"),
]
for tbl, col in UNKNOWN_CHECKS:
    n = con.execute(f"SELECT COUNT(*) FROM analytics_marts.{tbl} WHERE {col} = -1").fetchone()[0]
    status = PASS if n >= 1 else WARN
    results.append((f"{tbl} punya unknown member", n, status))
    print(f"  {tbl}.{col} = -1          : count={n:>6,}  [{status}]")

# ─────────────────────────────────────────
# [7] DISTRIBUSI
# ─────────────────────────────────────────
print("\n[7] DISTRIBUSI — Variasi data")
n_inap  = con.execute("SELECT COUNT(*) FROM analytics_marts.fct_kunjungan WHERE is_rawat_inap = TRUE").fetchone()[0]
n_jalan = con.execute("SELECT COUNT(*) FROM analytics_marts.fct_kunjungan WHERE is_rawat_inap = FALSE").fetchone()[0]
print(f"  fct_kunjungan rawat_inap=TRUE            : {n_inap:>8,}  [{'PASS' if n_inap > 0 else 'FAIL'}]")
print(f"  fct_kunjungan rawat_inap=FALSE           : {n_jalan:>8,}  [{'PASS' if n_jalan > 0 else 'FAIL'}]")

n_confirmed = con.execute("SELECT COUNT(*) FROM analytics_marts.fct_diagnosis WHERE is_confirmed = TRUE").fetchone()[0]
print(f"  fct_diagnosis is_confirmed=TRUE          : {n_confirmed:>8,}  [{'PASS' if n_confirmed > 0 else 'FAIL'}]")

n_selesai = con.execute("SELECT COUNT(*) FROM analytics_marts.fct_tindakan_medis WHERE is_selesai = TRUE").fetchone()[0]
print(f"  fct_tindakan is_selesai=TRUE             : {n_selesai:>8,}  [{'PASS' if n_selesai > 0 else 'FAIL'}]")

n_appt = con.execute("SELECT COUNT(*) FROM analytics_marts.fct_kunjungan WHERE is_dari_appointment = TRUE").fetchone()[0]
print(f"  fct_kunjungan dari_appointment=TRUE      : {n_appt:>8,}  [{'PASS' if n_appt > 0 else 'FAIL'}]")

# ─────────────────────────────────────────
# RINGKASAN
# ─────────────────────────────────────────
total  = len(results)
n_pass = sum(1 for _, _, s in results if s == PASS)
n_warn = sum(1 for _, _, s in results if s == WARN)
n_fail = sum(1 for _, _, s in results if s == FAIL)

print("\n" + "=" * 75)
print(f"RINGKASAN: {total} checks — {n_pass} PASS | {n_warn} WARN | {n_fail} FAIL")
if n_fail > 0:
    print("STATUS AKHIR: PERLU PERHATIAN")
elif n_warn > 0:
    print("STATUS AKHIR: LULUS DENGAN PERINGATAN (normal untuk unknown member check)")
else:
    print("STATUS AKHIR: SEMUA CHECKS LULUS")
print("=" * 75)

con.close()
