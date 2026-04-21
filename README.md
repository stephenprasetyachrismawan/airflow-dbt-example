# Healthcare Data Warehouse

**Data Mart berbasis Kimball Dimensional Model** untuk dataset rekam medis rumah sakit.  
Dibangun menggunakan Apache Airflow 2.8, dbt 1.11, dan DuckDB sebagai mesin analitik.

---

## Daftar Isi

1. [Gambaran Umum](#1-gambaran-umum)
2. [Arsitektur Sistem](#2-arsitektur-sistem)
3. [Struktur Folder](#3-struktur-folder)
4. [Prasyarat](#4-prasyarat)
5. [Cara Menjalankan](#5-cara-menjalankan)
6. [Pipeline DAG Airflow](#6-pipeline-dag-airflow)
7. [Model Dimensi & Fakta](#7-model-dimensi--fakta)
8. [Business Queries](#8-business-queries)
9. [Akses Web UI](#9-akses-web-ui)
10. [Catatan Data Issues](#10-catatan-data-issues)
11. [Hasil Implementasi](#11-hasil-implementasi)

---

## 1. Gambaran Umum

| Item | Detail |
|------|--------|
| **Dataset** | Kaggle — `moid1234/health-care-data-set-20-tables` (EHP Healthcare) |
| **Tabel sumber** | 10 tabel in-scope (9 root CSV + 2 file DIAG subfolder) |
| **Total baris raw** | ~5,5 juta baris |
| **Metodologi** | Kimball Dimensional Modeling |
| **Orchestrator** | Apache Airflow 2.8.0 |
| **Transformasi** | dbt 1.11.8 + dbt-duckdb |
| **Data Warehouse** | DuckDB (file-based, embedded) |
| **Deployment** | Docker Compose (multi-service) |

---

## 2. Arsitektur Sistem

```
┌─────────────────────────────────────────────────────────────┐
│                     APACHE AIRFLOW DAG                      │
│                                                             │
│  [validate_source_files]                                    │
│          ↓                                                  │
│  [ingest_csv_to_duckdb]  ← read_csv_auto() dari /archive   │
│          ↓                                                  │
│  [validate_row_counts]                                      │
│          ↓                                                  │
│  [dbt_run_staging]    → analytics_staging.*                 │
│          ↓                                                  │
│  [dbt_run_intermediate] → analytics_intermediate.*          │
│          ↓                                                  │
│  [dbt_run_marts]      → analytics_marts.*                   │
│          ↓                                                  │
│  [dbt_test_all]       → unique / not_null / relationships   │
│          ↓                                                  │
│  [dbt_docs_generate]  → katalog dokumentasi dbt             │
└─────────────────────────────────────────────────────────────┘
           ↓
    DuckDB  (duckdb/healthcare.duckdb)
           ↓
    analytics_marts.dim_*  /  fct_*
           ↓
    analytics/business_queries.sql
```

### Layer dbt

| Layer | Schema DuckDB | Materialisasi | Keterangan |
|-------|--------------|---------------|------------|
| Staging | `analytics_staging` | View | Cast tipe data, rename kolom ke Bahasa Indonesia |
| Intermediate | `analytics_intermediate` | View | Join lintas entitas, enrich data |
| Marts | `analytics_marts` | Table | Dimensi + Fakta siap query |

---

## 3. Struktur Folder

```
healthcare_dw/
├── airflow/
│   └── dags/
│       └── healthcare_pipeline.py      # DAG utama Airflow
├── analytics/
│   └── business_queries.sql            # 5 Business Query analitik
├── data/
│   └── archive/
│       └── STG_EHP_DATASET/            # CSV sumber (dari Kaggle)
│           ├── STG_EHP__PATN.csv
│           ├── STG_EHP__VIST.csv
│           ├── STG_EHP__APPT.csv
│           ├── STG_EHP__TRTM.csv
│           ├── STG_EHP__STFF.csv
│           ├── STG_EHP__DPMT.csv
│           ├── STG_EHP__ROMS.csv
│           ├── STG_EHP__MEDT.csv
│           ├── STG_EHP__INSR.csv
│           └── STG_EHP__DIAG/
│               ├── STG_EHP__DIAG_1.csv
│               └── STG_EHP__DIAG_2.csv
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       │   ├── sources.yml
│       │   ├── stg_pasien.sql
│       │   ├── stg_kunjungan.sql
│       │   ├── stg_appointment.sql
│       │   ├── stg_diagnosis.sql
│       │   ├── stg_tindakan_medis.sql
│       │   ├── stg_staff_dokter.sql
│       │   ├── stg_departemen.sql
│       │   ├── stg_ruangan.sql
│       │   ├── stg_tim_medis.sql
│       │   └── stg_asuransi.sql
│       ├── intermediate/
│       │   ├── int_pasien_dengan_asuransi.sql
│       │   ├── int_dokter_per_kunjungan.sql
│       │   └── int_kunjungan_enriched.sql
│       └── marts/
│           ├── dim_tanggal.sql
│           ├── dim_pasien.sql
│           ├── dim_dokter.sql
│           ├── dim_departemen.sql
│           ├── dim_ruangan.sql
│           ├── fct_kunjungan.sql
│           ├── fct_diagnosis.sql
│           ├── fct_tindakan_medis.sql
│           └── schema.yml
├── duckdb/
│   └── healthcare.duckdb               # File DuckDB (auto-generated saat pipeline run)
├── scripts/
│   └── sync_kaggle_dataset.py          # Helper opsional untuk download dataset
├── docker-compose.yml
├── HASIL_IMPLEMENTASI.md               # Laporan hasil: row count, sample query, catatan
└── README.md
```

---

## 4. Prasyarat

| Kebutuhan | Versi Minimum |
|-----------|--------------|
| Docker Desktop | 24.x |
| Docker Compose | 2.x |
| RAM tersedia | 4 GB |
| Ruang disk | 10 GB |

> Dataset Kaggle harus sudah diunduh dan diekstrak ke `data/archive/STG_EHP_DATASET/` sebelum pipeline dijalankan.
>
> Link dataset: https://www.kaggle.com/datasets/moid1234/health-care-data-set-20-tables

---

## 5. Cara Menjalankan

### Langkah 1 — Siapkan Dataset

Unduh dataset dari Kaggle, ekstrak, lalu pastikan strukturnya seperti ini:

```
healthcare_dw/
└── data/
    └── archive/
        └── STG_EHP_DATASET/
            ├── STG_EHP__PATN.csv
            ├── STG_EHP__VIST.csv
            ├── ...
            └── STG_EHP__DIAG/
                ├── STG_EHP__DIAG_1.csv
                └── STG_EHP__DIAG_2.csv
```

### Langkah 2 — Jalankan Docker Compose

```bash
# Dari folder healthcare_dw/
docker compose up -d
```

Tunggu semua container berstatus `healthy` (sekitar 2–3 menit):

```bash
docker compose ps
```

Output yang diharapkan:

```
NAME                           STATUS
healthcare_postgres            Up (healthy)
healthcare_airflow_webserver   Up (healthy)
healthcare_airflow_scheduler   Up
healthcare_dbt_docs            Up
```

### Langkah 3 — Trigger DAG

**Via Web UI** — buka http://localhost:8080 (admin / admin), klik DAG `healthcare_pipeline_duckdb`, lalu klik tombol **Trigger DAG**.

**Via CLI:**

```bash
docker exec healthcare_airflow_webserver \
  airflow dags trigger healthcare_pipeline_duckdb
```

### Langkah 4 — Monitor Progress

Di Airflow UI, buka tab **Graph** atau **Grid** untuk melihat status tiap task secara real-time.

Via CLI:

```bash
docker exec healthcare_airflow_webserver \
  airflow tasks states-for-dag-run healthcare_pipeline_duckdb <run_id>
```

### Langkah 5 — Jalankan Business Queries

Setelah DAG berstatus **success**, jalankan query analitik:

```bash
docker exec -it healthcare_airflow_webserver python3 << 'EOF'
import duckdb
con = duckdb.connect('/opt/airflow/duckdb/healthcare.duckdb', read_only=True)
result = con.execute("""
    SELECT t.tahun, t.bulan, t.nama_bulan, COUNT(*) AS total_kunjungan
    FROM analytics_marts.fct_kunjungan k
    JOIN analytics_marts.dim_tanggal t ON k.tanggal_masuk_key = t.tanggal_key
    WHERE t.tanggal >= (CURRENT_DATE - INTERVAL 12 MONTH)
    GROUP BY t.tahun, t.bulan, t.nama_bulan
    ORDER BY t.tahun, t.bulan
""").fetchdf()
print(result)
con.close()
EOF
```

Atau salin query lengkap dari `analytics/business_queries.sql`.

---

## 6. Pipeline DAG Airflow

**DAG ID:** `healthcare_pipeline_duckdb`  
**Jadwal:** Setiap hari pukul 01.00 UTC (`0 1 * * *`)

```
validate_source_files
        ↓
ingest_csv_to_duckdb
        ↓
validate_row_counts
        ↓
dbt_run_staging
        ↓
dbt_run_intermediate
        ↓
dbt_run_marts
        ↓
dbt_test_all
        ↓
dbt_docs_generate
```

| Task | Waktu Eksekusi | Keterangan |
|------|---------------:|------------|
| `validate_source_files` | ~1 detik | Cek keberadaan 11 file CSV |
| `ingest_csv_to_duckdb` | ~15–20 detik | Load 5,5 juta baris ke DuckDB raw schema |
| `validate_row_counts` | ~1 detik | Pastikan semua tabel raw tidak kosong |
| `dbt_run_staging` | ~9 detik | 10 staging view |
| `dbt_run_intermediate` | ~8 detik | 3 intermediate view |
| `dbt_run_marts` | ~35–40 detik | 5 dimensi + 3 fakta (materialisasi table) |
| `dbt_test_all` | ~11 detik | Semua test unique / not_null / FK |
| `dbt_docs_generate` | ~10 detik | Generate katalog JSON |

**Total waktu pipeline:** ~90–100 detik

---

## 7. Model Dimensi & Fakta

### Star Schema

```
                    dim_tanggal
                        │
         dim_pasien ────┤
                        │
         dim_dokter ────┼──── fct_kunjungan
                        │         │
      dim_departemen ───┤         ├── fct_diagnosis
                        │         └── fct_tindakan_medis
        dim_ruangan ────┘
```

### Tabel Dimensi

| Tabel | SCD | Baris | Keterangan |
|-------|-----|------:|------------|
| `dim_tanggal` | Type 0 | 2,923 | Kalender 2020–2027, atribut nama bulan/hari Indonesia |
| `dim_pasien` | Type 2 | 351,766 | Historisasi pasien; `dw_is_current = TRUE` untuk baris aktif |
| `dim_dokter` | Type 2 | 385 | Hanya dokter (`ROLE_DES = 'Doctor'`), denormalisasi nama departemen |
| `dim_departemen` | Type 1 | 31 | Update in-place; tidak ada histori |
| `dim_ruangan` | Type 1 | 24,303 | Termasuk `departemen_id` sebagai FK ke departemen |

> Setiap dimensi memiliki **unknown member** (surrogate key = `-1`) untuk menjaga integritas referensial bila join tidak cocok.

### Tabel Fakta

| Tabel | Grain | Baris | Measures Utama |
|-------|-------|------:|----------------|
| `fct_kunjungan` | 1 baris per kunjungan | 917,516 | `durasi_kunjungan_jam`, `is_rawat_inap`, `is_dari_appointment`, `is_appointment_converted` |
| `fct_diagnosis` | 1 baris per diagnosis per kunjungan | 1,647,067 | `total_biaya_diagnosis`, `is_confirmed`, `is_diagnosis_utama`, `jumlah_diagnosis` |
| `fct_tindakan_medis` | 1 baris per tindakan per kunjungan | 942,402 | `total_biaya_tindakan`, `is_selesai`, `is_operasi`, `is_obat`, `jumlah_tindakan` |

### dbt Tests

Semua model marts dilengkapi test otomatis:

- `unique` + `not_null` — untuk semua surrogate key
- `unique` — untuk natural key (tanggal, dep_id, rom_id)
- `relationships` — FK dari setiap kolom `*_key` di fact tables ke tabel dimensi yang bersesuaian

---

## 8. Business Queries

File: [`analytics/business_queries.sql`](analytics/business_queries.sql)

| Query | Pertanyaan Bisnis |
|-------|------------------|
| **BQ1** | Berapa kunjungan per bulan dalam 12 bulan terakhir? (rawat inap vs rawat jalan) |
| **BQ2** | Departemen mana yang paling banyak dan paling sedikit dikunjungi? |
| **BQ3** | Berapa rata-rata Length of Stay (LOS) pasien rawat inap per departemen? |
| **BQ4** | Siapa 5 dokter dengan beban kerja tertinggi setiap bulannya? |
| **BQ5** | Berapa persen appointment yang berhasil dikonversi menjadi kunjungan aktual? |

Semua query menggunakan schema `analytics_marts` (prefix dari target dbt `analytics`).

---

## 9. Akses Web UI

| Layanan | URL | Login |
|---------|-----|-------|
| Airflow UI | http://localhost:8080 | admin / admin |
| dbt Docs | http://localhost:8081 | — |

Di **dbt Docs** (port 8081) tersedia:
- Lineage graph dari semua model (raw → staging → intermediate → marts)
- Dokumentasi kolom dan deskripsi tiap tabel
- Hasil dbt test per model

---

## 10. Catatan Data Issues

Penyesuaian yang ditemukan saat eksplorasi data aktual Kaggle:

| # | Issue | Solusi yang Diterapkan |
|---|-------|------------------------|
| 1 | `STFF.ROLE_CD` menggunakan kode numerik, bukan string `'DOC'` | Filter dokter pakai `ROLE_DES = 'Doctor'` (ROLE_CD = `'0'`) |
| 2 | `APPT.REFR_NO` (numerik) berbeda format dari `VIST.REFR_NO` (W-prefix) | Join appointment ke kunjungan via `PAT_ID + tanggal proximity ≤30 hari` |
| 3 | `VTYPE_DES` hanya berisi `'Diagnosis'` dan `'Treatment'` (bukan Inpatient/Outpatient) | `is_rawat_inap` menggunakan `VSTAT_DES = 'Admitted'` |
| 4 | `DSTAT_DES` tidak menggunakan kode `'CNF'`/`'TEN'` | `is_confirmed` pakai `DSTAT_DES LIKE '%Confirmed%' OR = 'Recurrent'` |
| 5 | `TRTM_TOT` berisi nilai 1–10 (bukan nominal rupiah) | Diperlakukan sebagai measures unit/kuantitas |
| 6 | dbt profile target `analytics` menambahkan prefix ke nama schema | Schema aktual di DuckDB: `analytics_staging`, `analytics_intermediate`, `analytics_marts` |

---

## 11. Hasil Implementasi

Detail lengkap tersedia di [`HASIL_IMPLEMENTASI.md`](HASIL_IMPLEMENTASI.md).

### Ringkasan Row Count

| Layer | Tabel | Baris |
|-------|-------|------:|
| Raw | `stg_ehp__patn` | 351,765 |
| Raw | `stg_ehp__vist` | 917,331 |
| Raw | `stg_ehp__appt` | 974,032 |
| Raw | `stg_ehp__diag` | 1,602,174 |
| Raw | `stg_ehp__trtm` | 917,191 |
| Raw | `stg_ehp__stff` | 5,496 |
| Raw | `stg_ehp__dpmt` | 30 |
| Raw | `stg_ehp__roms` | 24,302 |
| Raw | `stg_ehp__medt` | 312,870 |
| Raw | `stg_ehp__insr` | 457,072 |
| **Raw TOTAL** | 10 tabel | **5,561,263** |
| Marts | `dim_tanggal` | 2,923 |
| Marts | `dim_pasien` | 351,766 |
| Marts | `dim_dokter` | 385 |
| Marts | `dim_departemen` | 31 |
| Marts | `dim_ruangan` | 24,303 |
| Marts | `fct_kunjungan` | 917,516 |
| Marts | `fct_diagnosis` | 1,647,067 |
| Marts | `fct_tindakan_medis` | 942,402 |
| **Marts TOTAL** | 8 tabel | **3,886,393** |

### Sample Output BQ1 — Tren Kunjungan (6 Bulan)

| Tahun | Bulan | Nama Bulan | Total Kunjungan | Rawat Inap | Rawat Jalan |
|-------|-------|-----------|----------------:|-----------:|------------:|
| 2025 | 4 | April | 14,419 | 3,462 | 10,957 |
| 2025 | 5 | Mei | 44,889 | 15,331 | 29,558 |
| 2025 | 6 | Juni | 43,605 | 21,224 | 22,381 |
| 2025 | 7 | Juli | 44,546 | 28,433 | 16,113 |
| 2025 | 8 | Agustus | 45,043 | 35,690 | 9,353 |
| 2025 | 9 | September | 38,354 | 35,952 | 2,402 |

### Sample Output BQ2 — Top 5 Departemen

| Departemen | Total Kunjungan |
|-----------|----------------:|
| Radiology | 24,242 |
| Cardiology | 24,121 |
| Endocrinology | 23,650 |
| Hematology | 23,352 |
| Psychiatry | 23,323 |

---

## Troubleshooting

**Container tidak mau start:**
```bash
docker compose down -v && docker compose up -d
```

**DAG tidak muncul di Airflow UI:**
```bash
# Cek error parse DAG
docker exec healthcare_airflow_scheduler airflow dags list-import-errors
```

**dbt run gagal dengan "table not found":**
```bash
# Pastikan ingest sudah selesai dulua, lalu cek schema
docker exec healthcare_airflow_webserver python3 -c "
import duckdb
con = duckdb.connect('/opt/airflow/duckdb/healthcare.duckdb')
print(con.execute('SHOW ALL TABLES').fetchdf())
"
```

**File CSV tidak ditemukan:**
- Pastikan `data/archive/STG_EHP_DATASET/` ada dan berisi semua file yang diperlukan
- Cek volume mount di `docker-compose.yml`: `./data:/opt/airflow/data`

---

*Dibuat untuk keperluan tugas akhir S2 — Data Warehouse & Inteligensia Bisnis, Universitas Gadjah Mada.*
