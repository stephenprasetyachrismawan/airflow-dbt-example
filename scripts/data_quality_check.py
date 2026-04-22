"""
Tahap 6B — Data Quality Check Komprehensif
Memeriksa: duplikat, null tak terduga, outlier, inkonsistensi.
"""
import duckdb
import sys
from datetime import datetime

DUCKDB_PATH = '/opt/airflow/duckdb/healthcare.duckdb'
con = duckdb.connect(DUCKDB_PATH, read_only=True)

passed = 0
failed = 0
warnings = 0


def check(description, sql, expect_zero=True, warn_only=False):
    global passed, failed, warnings
    try:
        result = con.execute(sql).fetchone()[0]
        ok = (result == 0) if expect_zero else (result > 0)

        if ok:
            print(f"  ✓ PASS  {description}")
            passed += 1
        elif warn_only:
            print(f"  ⚠ WARN  {description} → nilai: {result:,}")
            warnings += 1
        else:
            print(f"  ✗ FAIL  {description} → nilai: {result:,}")
            failed += 1
    except Exception as e:
        print(f"  ✗ ERROR {description} → {e}")
        failed += 1


print("=" * 70)
print("HEALTHCARE DW — DATA QUALITY CHECK")
print(f"Dijalankan: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# ── 1. COMPLETENESS: Semua tabel terisi ─────────────────────────────────────
print("\n[1] COMPLETENESS — Tabel tidak boleh kosong")
for t in ['dim_date', 'dim_patient', 'dim_doctor', 'dim_department',
          'dim_room', 'fct_visit', 'fct_billing']:
    check(
        f"analytics_marts.{t} > 0 rows",
        f"SELECT COUNT(*) FROM analytics_marts.{t}",
        expect_zero=False
    )

# ── 2. UNIQUENESS: Surrogate keys harus unik ────────────────────────────────
print("\n[2] UNIQUENESS — Surrogate keys harus unik")
sk_checks = [
    ('dim_date', 'date_key'),
    ('dim_patient', 'patient_key'),
    ('dim_doctor', 'doctor_key'),
    ('dim_department', 'department_key'),
    ('dim_room', 'room_key'),
    ('fct_visit', 'visit_key'),
    ('fct_billing', 'billing_key'),
]
for tbl, pk in sk_checks:
    check(
        f"{tbl}.{pk} tidak ada duplikat",
        f"SELECT COUNT(*) - COUNT(DISTINCT {pk}) FROM analytics_marts.{tbl}"
    )

# ── 3. NOT NULL: Primary keys tidak boleh null ──────────────────────────────
print("\n[3] NOT NULL — Primary keys tidak boleh null")
for tbl, pk in sk_checks:
    check(
        f"{tbl}.{pk} tidak ada null",
        f"SELECT COUNT(*) FROM analytics_marts.{tbl} WHERE {pk} IS NULL"
    )

# ── 4. REFERENTIAL INTEGRITY ────────────────────────────────────────────────
print("\n[4] REFERENTIAL INTEGRITY — FK harus punya pasangan di dim")
ri_checks = [
    ('fct_visit', 'patient_key', 'dim_patient', 'patient_key'),
    ('fct_visit', 'doctor_key', 'dim_doctor', 'doctor_key'),
    ('fct_visit', 'room_key', 'dim_room', 'room_key'),
    ('fct_visit', 'department_key', 'dim_department', 'department_key'),
    ('fct_visit', 'visit_date_key', 'dim_date', 'date_key'),
    ('fct_billing', 'patient_key', 'dim_patient', 'patient_key'),
    ('fct_billing', 'billing_date_key', 'dim_date', 'date_key'),
]
for fact, fk, dim, pk in ri_checks:
    check(
        f"{fact}.{fk} → {dim}.{pk}: 0 orphans",
        f"""
        SELECT COUNT(*) FROM analytics_marts.{fact} f
        LEFT JOIN analytics_marts.{dim} d ON f.{fk} = d.{pk}
        WHERE d.{pk} IS NULL AND f.{fk} != -1
        """
    )

# ── 5. VALIDITY: Range checks ───────────────────────────────────────────────
print("\n[5] VALIDITY — Range & business rule checks")
check(
    "dim_date: tahun dalam range 2020–2026",
    "SELECT COUNT(*) FROM analytics_marts.dim_date WHERE calendar_date IS NOT NULL AND (year < 2020 OR year > 2026)"
)
check(
    "dim_date: bulan dalam range 1–12",
    "SELECT COUNT(*) FROM analytics_marts.dim_date WHERE calendar_date IS NOT NULL AND (month < 1 OR month > 12)"
)
check(
    "fct_visit: length_of_stay_hours tidak negatif",
    "SELECT COUNT(*) FROM analytics_marts.fct_visit WHERE length_of_stay_hours IS NOT NULL AND length_of_stay_hours < 0"
)
check(
    "fct_visit: is_inpatient tidak null",
    "SELECT COUNT(*) FROM analytics_marts.fct_visit WHERE is_inpatient IS NULL"
)
check(
    "fct_billing: billing_amount tidak negatif",
    "SELECT COUNT(*) FROM analytics_marts.fct_billing WHERE billing_amount IS NOT NULL AND billing_amount < 0"
)
check(
    "fct_billing: outstanding_amount tidak negatif",
    "SELECT COUNT(*) FROM analytics_marts.fct_billing WHERE outstanding_amount IS NOT NULL AND outstanding_amount < 0"
)
check(
    "dim_doctor: minimal 1 dokter aktif",
    "SELECT COUNT(*) FROM analytics_marts.dim_doctor WHERE status_code = 'A' AND doctor_key != -1",
    expect_zero=False
)

# ── 6. CONSISTENCY: Unknown member (-1) ─────────────────────────────────────
print("\n[6] CONSISTENCY — Unknown member (-1) di dimensi (best practice, warning saja)")
for tbl, pk in [('dim_patient', 'patient_key'), ('dim_doctor', 'doctor_key'),
                ('dim_department', 'department_key'), ('dim_room', 'room_key'),
                ('dim_date', 'date_key')]:
    check(
        f"{tbl}: unknown member (key=-1) ada",
        f"SELECT COUNT(*) FROM analytics_marts.{tbl} WHERE {pk} = -1",
        expect_zero=False, warn_only=True
    )

# ── 7. DISTRIBUSI (warning saja) ────────────────────────────────────────────
print("\n[7] DISTRIBUSI — Checks informatif (warning jika anomali)")
check(
    "fct_visit: ada kunjungan rawat inap",
    "SELECT COUNT(*) FROM analytics_marts.fct_visit WHERE is_inpatient = TRUE",
    expect_zero=False, warn_only=True
)
check(
    "fct_visit: ada kunjungan rawat jalan",
    "SELECT COUNT(*) FROM analytics_marts.fct_visit WHERE is_inpatient = FALSE",
    expect_zero=False, warn_only=True
)
check(
    "fct_billing: ada billing berstatus Paid",
    "SELECT COUNT(*) FROM analytics_marts.fct_billing WHERE payment_status = 'Paid'",
    expect_zero=False, warn_only=True
)

# ── RINGKASAN ────────────────────────────────────────────────────────────────
total = passed + failed + warnings
print("\n" + "=" * 70)
print("RINGKASAN DATA QUALITY CHECK")
print("=" * 70)
print(f"  Total checks : {total}")
print(f"  ✓ Passed     : {passed}")
print(f"  ✗ Failed     : {failed}")
print(f"  ⚠ Warnings   : {warnings}")
print("=" * 70)

if failed > 0:
    print(f"\n⚠ PERHATIAN: {failed} check gagal. Review dan perbaiki sebelum submit.")
    sys.exit(1)
else:
    print("\n✓ Semua critical checks PASS. Data mart siap untuk BI.")

con.close()
