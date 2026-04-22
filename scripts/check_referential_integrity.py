"""
Tahap 5D — Validasi Konsistensi Referensial Data Mart
Memastikan semua FK di fact tables punya pasangan valid di dimension tables.
"""
import duckdb
import sys

DUCKDB_PATH = '/opt/airflow/duckdb/healthcare.duckdb'


def check_referential_integrity():
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    issues = []

    checks = [
        # (fact_table, fk_col, dim_table, pk_col, description)
        ('fct_visit', 'patient_key', 'dim_patient', 'patient_key', 'Kunjungan → Pasien'),
        ('fct_visit', 'doctor_key', 'dim_doctor', 'doctor_key', 'Kunjungan → Dokter'),
        ('fct_visit', 'room_key', 'dim_room', 'room_key', 'Kunjungan → Ruangan'),
        ('fct_visit', 'department_key', 'dim_department', 'department_key', 'Kunjungan → Departemen'),
        ('fct_visit', 'visit_date_key', 'dim_date', 'date_key', 'Kunjungan → Tanggal'),
        ('fct_billing', 'patient_key', 'dim_patient', 'patient_key', 'Billing → Pasien'),
        ('fct_billing', 'billing_date_key', 'dim_date', 'date_key', 'Billing → Tanggal'),
    ]

    print("=" * 70)
    print("HEALTHCARE DW — VALIDASI KONSISTENSI REFERENSIAL")
    print("=" * 70)
    print(f"{'Relasi':<40} {'Orphans':>10} {'Status':>10}")
    print("-" * 70)

    for fact_tbl, fk_col, dim_tbl, pk_col, desc in checks:
        q = f"""
            SELECT COUNT(*) as orphans
            FROM analytics_marts.{fact_tbl} f
            LEFT JOIN analytics_marts.{dim_tbl} d ON f.{fk_col} = d.{pk_col}
            WHERE d.{pk_col} IS NULL
              AND f.{fk_col} <> -1
        """
        orphans = con.execute(q).fetchone()[0]
        status = "✓ PASS" if orphans == 0 else "✗ FAIL"
        if orphans > 0:
            issues.append(f"{desc}: {orphans} orphan rows")
        print(f"{desc:<40} {orphans:>10,} {status:>10}")

    print("=" * 70)
    if issues:
        print(f"\n⚠ DITEMUKAN {len(issues)} MASALAH REFERENSIAL:")
        for issue in issues:
            print(f"  - {issue}")
        sys.exit(1)
    else:
        print("\n✓ Semua relasi FK valid. Data mart konsisten secara referensial.")

    con.close()


if __name__ == '__main__':
    check_referential_integrity()
