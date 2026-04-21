# HASIL IMPLEMENTASI — Healthcare Data Warehouse
**Tanggal:** 21 April 2026  
**Dataset:** Kaggle EHP Healthcare Dataset  
**Stack:** Apache Airflow 2.8 + dbt 1.11 + DuckDB

---

## 9.1 Row Counts Setelah ETL Selesai

### Raw Layer (hasil ingest dari CSV)
| Tabel | Baris |
|-------|------:|
| raw.stg_ehp__patn | 351,765 |
| raw.stg_ehp__vist | 917,331 |
| raw.stg_ehp__appt | 974,032 |
| raw.stg_ehp__diag | **1,602,174** (DIAG_1: 950,000 + DIAG_2: 652,174) |
| raw.stg_ehp__trtm | 917,191 |
| raw.stg_ehp__stff | 5,496 |
| raw.stg_ehp__dpmt | 30 |
| raw.stg_ehp__roms | 24,302 |
| raw.stg_ehp__medt | 312,870 |
| raw.stg_ehp__insr | 457,072 |
| **TOTAL RAW** | **5,561,263** |

### Marts Layer (hasil transformasi dbt)
| Tabel | Baris | Keterangan |
|-------|------:|------------|
| dim_tanggal | 2,923 | 2020-2027 + 1 unknown row |
| dim_pasien | 351,766 | 351,765 pasien + 1 unknown |
| dim_dokter | 385 | 384 dokter + 1 unknown |
| dim_departemen | 31 | 30 dept + 1 unknown |
| dim_ruangan | 24,303 | 24,302 ruangan + 1 unknown |
| fct_kunjungan | 917,516 | grain: 1 baris/kunjungan |
| fct_diagnosis | 1,647,067 | grain: 1 baris/diagnosis/kunjungan |
| fct_tindakan_medis | 942,402 | grain: 1 baris/tindakan/kunjungan |

---

## 9.2 Sample Output Business Queries

### BQ1 — Tren Kunjungan per Bulan (12 bulan terakhir)
| Tahun | Bulan | Nama Bulan | Total Kunjungan | Rawat Inap | Rawat Jalan |
|-------|-------|-----------|----------------:|-----------:|------------:|
| 2025 | 4 | April | 14,419 | 3,462 | 10,957 |
| 2025 | 5 | Mei | 44,889 | 15,331 | 29,558 |
| 2025 | 6 | Juni | 43,605 | 21,224 | 22,381 |
| 2025 | 7 | Juli | 44,546 | 28,433 | 16,113 |
| 2025 | 8 | Agustus | 45,043 | 35,690 | 9,353 |
| 2025 | 9 | September | 38,354 | 35,952 | 2,402 |

### BQ2 — Top 5 Departemen Terbanyak Kunjungan
| Departemen | Total Kunjungan |
|-----------|----------------:|
| Radiology | 24,242 |
| Cardiology | 24,121 |
| Endocrinology | 23,650 |
| Hematology | 23,352 |
| Psychiatry | 23,323 |

---

## 9.3 dbt Test Results
Semua test dbt PASS. Test yang dijalankan:
- `unique` + `not_null` untuk semua surrogate key
- `unique` untuk natural key (tanggal, dep_id, rom_id)
- `relationships` FK untuk semua FK di fact tables ke dimension tables

---

## 9.4 Catatan Data Issues

### 1. STFF ROLE_CD bukan string 'DOC'
ROLE_CD menggunakan kode numerik. Filter dokter menggunakan `ROLE_DES = 'Doctor'` (ROLE_CD = '0'). Total: 384 dokter.

### 2. APPT REFR_NO berbeda format dari VIST
- APPT.REFR_NO: numerik (6953591)
- VIST.REFR_NO: alphanumeric W-prefix (W4723062)
- **Solusi:** Join appointment ke kunjungan via `PAT_ID + proximity tanggal (≤30 hari)`, bukan REFR_NO

### 3. VTYPE tidak menggunakan Inpatient/Outpatient
VTYPE_DES hanya: 'Diagnosis' dan 'Treatment'. `is_rawat_inap` menggunakan `VSTAT_DES = 'Admitted'`.

### 4. DIAG DSTAT_DES tidak menggunakan 'CNF'/'TEN'
Nilai aktual DSTAT_DES: 'Recurrent', 'Confirmed', 'Requires Surgery', dll. `is_confirmed` menggunakan `DSTAT_DES LIKE '%Confirmed%' OR DSTAT_DES = 'Recurrent'`.

### 5. TRTM_TOT nilai kecil
Nilai TRTM_TOT berupa angka 1-10 (kemungkinan kode/unit, bukan nominal rupiah).

### 6. Schema prefix dbt
dbt menggunakan profile target `analytics`, sehingga schema di DuckDB menjadi `analytics_staging`, `analytics_intermediate`, `analytics_marts` (bukan `staging`, `intermediate`, `marts`).

---

## Struktur File yang Dibuat/Diubah

```
airflow/dags/healthcare_pipeline.py        ← Updated (path archive + ingest DIAG UNION)
dbt/models/staging/sources.yml             ← Updated (hanya in-scope tables)
dbt/models/staging/stg_pasien.sql          ← NEW
dbt/models/staging/stg_kunjungan.sql       ← NEW
dbt/models/staging/stg_appointment.sql     ← NEW
dbt/models/staging/stg_diagnosis.sql       ← NEW
dbt/models/staging/stg_tindakan_medis.sql  ← NEW
dbt/models/staging/stg_staff_dokter.sql    ← NEW (filter Doctor only)
dbt/models/staging/stg_departemen.sql      ← NEW
dbt/models/staging/stg_ruangan.sql         ← NEW
dbt/models/staging/stg_tim_medis.sql       ← NEW (MEDT bridge)
dbt/models/staging/stg_asuransi.sql        ← NEW
dbt/models/intermediate/int_pasien_dengan_asuransi.sql  ← NEW
dbt/models/intermediate/int_dokter_per_kunjungan.sql    ← NEW
dbt/models/intermediate/int_kunjungan_enriched.sql      ← NEW
dbt/models/marts/dim_tanggal.sql           ← NEW (nama Indonesia + SCD0)
dbt/models/marts/dim_pasien.sql            ← NEW (SCD2)
dbt/models/marts/dim_dokter.sql            ← NEW (SCD2, Doctor only)
dbt/models/marts/dim_departemen.sql        ← NEW (SCD1)
dbt/models/marts/dim_ruangan.sql           ← NEW (SCD1)
dbt/models/marts/fct_kunjungan.sql         ← NEW
dbt/models/marts/fct_diagnosis.sql         ← NEW
dbt/models/marts/fct_tindakan_medis.sql    ← NEW
dbt/models/marts/schema.yml                ← Updated total
analytics/business_queries.sql             ← NEW (5 BQ)
HASIL_IMPLEMENTASI.md                      ← NEW (file ini)
```
