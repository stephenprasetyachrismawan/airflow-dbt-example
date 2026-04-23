# Healthcare Data Warehouse

**Data Mart berbasis Kimball Dimensional Model** untuk dataset rekam medis rumah sakit.  
Dibangun menggunakan Apache Airflow 2.8, dbt 1.11, DuckDB, dan Apache Superset.

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
9. [Visualisasi di Apache Superset](#9-visualisasi-di-apache-superset)
10. [Akses Web UI](#10-akses-web-ui)
11. [Catatan Data Issues](#11-catatan-data-issues)
12. [Hasil Implementasi](#12-hasil-implementasi)
13. [Troubleshooting](#13-troubleshooting)

---

## 1. Gambaran Umum

| Item | Detail |
|------|--------|
| **Dataset** | Kaggle — `moid1234/health-care-data-set-20-tables` (EHP Healthcare) |
| **Tabel sumber** | 10 tabel in-scope (9 root CSV + 2 file DIAG subfolder) |
| **Total baris raw** | ~5,5 juta baris |
| **Metodologi** | Kimball Dimensional Modeling (Star Schema) |
| **Orchestrator** | Apache Airflow 2.8.0 |
| **Transformasi** | dbt 1.11.8 + dbt-duckdb |
| **Data Warehouse** | DuckDB (file-based, embedded) |
| **BI / Visualisasi** | Apache Superset 4.x |
| **Deployment** | Docker Compose (multi-service) |

---

## 2. Arsitektur Sistem

```
┌──────────────────────────────────────────────────────────────────┐
│                        APACHE AIRFLOW DAG                        │
│                                                                  │
│   [validate_source_files]  ← cek 11 file CSV sumber             │
│             ↓                                                    │
│   [ingest_csv_to_duckdb]   ← read_csv_auto() ke schema raw      │
│             ↓                                                    │
│   [validate_row_counts]    ← pastikan semua tabel tidak kosong   │
│             ↓                                                    │
│   [dbt_run_staging]        → analytics_staging.*                 │
│             ↓                                                    │
│   [dbt_run_intermediate]   → analytics_intermediate.*            │
│             ↓                                                    │
│   [dbt_run_marts]          → analytics_marts.*                   │
│             ↓                                                    │
│   [dbt_test_all]           → unique / not_null / relationships   │
│             ↓                                                    │
│   [dbt_docs_generate]      → katalog dokumentasi dbt             │
└──────────────────────────────────────────────────────────────────┘
                              ↓
              DuckDB  (duckdb/healthcare.duckdb)
                              ↓
          analytics_marts.dim_*  /  analytics_marts.fct_*
                              ↓
               ┌──────────────┴──────────────┐
               │                             │
        SQL Analytics               Apache Superset
   (business_queries.sql)        (Dashboard & Charts)
```

### Layer dbt

| Layer | Schema DuckDB | Materialisasi | Keterangan |
|-------|--------------|---------------|------------|
| Staging | `analytics_staging` | View | Cast tipe data, rename kolom ke Bahasa Indonesia |
| Intermediate | `analytics_intermediate` | View | Join lintas entitas, enrich data |
| Marts | `analytics_marts` | Table | Dimensi + Fakta siap query dan visualisasi |

---

## 3. Struktur Folder

```
healthcare_dw/
├── airflow/
│   └── dags/
│       └── healthcare_pipeline.py          # DAG utama Airflow (8 task)
├── analytics/
│   ├── business_queries.sql                # 5 Business Query (BQ1–BQ5)
│   └── run_business_queries.py             # Eksekusi BQ via Python
├── data/
│   └── archive/
│       └── STG_EHP_DATASET/                # CSV sumber (dari Kaggle)
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
│   └── healthcare.duckdb                   # File DuckDB (auto-generated saat pipeline run)
├── scripts/
│   ├── check_referential_integrity.py      # 16 FK checks
│   ├── data_quality_check.py               # 24 data quality checks
│   ├── query_performance.py                # Benchmark before/after indexing
│   └── generate_final_report.py            # Laporan validasi akhir
├── view_datamart.py                        # Interactive viewer 8 tabel mart
├── docker-compose.yml
├── CONNECTION_INFO.md                      # Info koneksi & skema tabel
├── HASIL_IMPLEMENTASI.md                   # Laporan hasil: row count, sample query
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

> **Dataset Kaggle** harus sudah diunduh dan diekstrak ke `data/archive/STG_EHP_DATASET/` sebelum pipeline dijalankan.
>
> Link dataset: https://www.kaggle.com/datasets/moid1234/health-care-data-set-20-tables

---

## 5. Cara Menjalankan

### Langkah 1 — Siapkan Dataset

Unduh dataset dari Kaggle, ekstrak, lalu pastikan strukturnya seperti ini:

```
healthcare_dw/data/archive/STG_EHP_DATASET/
├── STG_EHP__PATN.csv
├── STG_EHP__VIST.csv
├── STG_EHP__APPT.csv
├── STG_EHP__TRTM.csv
├── STG_EHP__STFF.csv
├── STG_EHP__DPMT.csv
├── STG_EHP__ROMS.csv
├── STG_EHP__MEDT.csv
├── STG_EHP__INSR.csv
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
healthcare_superset            Up (healthy)
```

### Langkah 3 — Trigger DAG

**Via Web UI** — buka http://localhost:8080 (admin / admin), klik DAG `healthcare_pipeline_duckdb`, lalu klik tombol **Trigger DAG ▶**.

**Via CLI:**

```bash
docker exec healthcare_airflow_webserver \
  airflow dags trigger healthcare_pipeline_duckdb
```

### Langkah 4 — Monitor Progress

Di Airflow UI, buka tab **Graph** atau **Grid** untuk melihat status tiap task secara real-time. Semua task harus berwarna hijau (success) dari atas ke bawah.

### Langkah 5 — Buka Superset

Setelah DAG berhasil, buka Apache Superset di http://localhost:8088 (admin / admin) untuk eksplorasi data dan pembuatan dashboard.

---

## 6. Pipeline DAG Airflow

**DAG ID:** `healthcare_pipeline_duckdb`  
**Jadwal:** Setiap hari pukul 01.00 UTC (`0 1 * * *`)  
**Catchup:** Tidak aktif

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
| `ingest_csv_to_duckdb` | ~15–20 detik | Load 5,5 juta baris ke DuckDB schema `raw` |
| `validate_row_counts` | ~1 detik | Pastikan semua tabel raw tidak kosong |
| `dbt_run_staging` | ~9 detik | 10 staging view |
| `dbt_run_intermediate` | ~8 detik | 3 intermediate view |
| `dbt_run_marts` | ~35–40 detik | 5 dimensi + 3 fakta (thread 1, sequential) |
| `dbt_test_all` | ~11 detik | Semua test unique / not_null / relationships |
| `dbt_docs_generate` | ~10 detik | Generate katalog JSON |

**Total waktu pipeline:** ~90–100 detik

> **Catatan:** `dbt_run_marts` dijalankan dengan `--threads 1` untuk mencegah OOM kill saat 3 fact table besar dijalankan paralel.

---

## 7. Model Dimensi & Fakta

### Star Schema

```
                     dim_tanggal (SCD0)
                          │
          dim_pasien ─────┤ (SCD2)
                          │
          dim_dokter ─────┼──── fct_kunjungan ────► fct_diagnosis
                          │         │
       dim_departemen ────┤         └────────────► fct_tindakan_medis
                          │
         dim_ruangan ─────┘ (SCD1)
```

### Tabel Dimensi

| Tabel | SCD Type | Baris | Keterangan |
|-------|----------|------:|------------|
| `dim_tanggal` | Type 0 | 2,923 | Kalender 2020–2027, nama bulan/hari Bahasa Indonesia |
| `dim_pasien` | Type 2 | 351,766 | Historisasi; `dw_is_current = TRUE` untuk baris aktif |
| `dim_dokter` | Type 2 | 385 | Hanya dokter (`ROLE_DES = 'Doctor'`), 384 + 1 unknown |
| `dim_departemen` | Type 1 | 31 | Update in-place, 30 dept + 1 unknown |
| `dim_ruangan` | Type 1 | 24,303 | Denormalisasi `departemen_id`, 24,302 + 1 unknown |

> Setiap dimensi memiliki **unknown member** dengan surrogate key = `-1` untuk menjaga integritas referensial.

### Tabel Fakta

| Tabel | Grain | Baris | Measures Utama |
|-------|-------|------:|----------------|
| `fct_kunjungan` | 1 baris per kunjungan | 917,516 | `durasi_kunjungan_jam`, `is_rawat_inap`, `is_dari_appointment`, `is_appointment_converted` |
| `fct_diagnosis` | 1 baris per diagnosis per kunjungan | 1,647,067 | `total_biaya_diagnosis`, `is_confirmed`, `is_diagnosis_utama`, `jumlah_diagnosis` |
| `fct_tindakan_medis` | 1 baris per tindakan per kunjungan | 942,402 | `total_biaya_tindakan`, `is_selesai`, `is_operasi`, `is_obat`, `jumlah_tindakan` |

### dbt Tests

Semua model marts dilengkapi test otomatis yang berjalan setiap kali DAG dieksekusi:

| Jenis Test | Jumlah | Target |
|------------|-------:|--------|
| `unique` | 8 | Semua surrogate key + natural key |
| `not_null` | 8 | Semua surrogate key |
| `relationships` | 16 | Semua FK di 3 fact tables ke dimensi |

---

## 8. Business Queries

File lengkap: [`analytics/business_queries.sql`](analytics/business_queries.sql)

Semua query menggunakan prefix schema `analytics_marts` (hasil dari target dbt `analytics`).

---

### BQ1 — Tren Kunjungan per Bulan (12 Bulan Terakhir)

**Pertanyaan:** Berapa total kunjungan per bulan dalam 12 bulan terakhir, dan bagaimana proporsi rawat inap vs rawat jalan?

```sql
SELECT
    t.tahun,
    t.bulan,
    t.nama_bulan,
    CAST(t.tahun AS VARCHAR) || '-' || LPAD(CAST(t.bulan AS VARCHAR), 2, '0') AS periode,
    COUNT(*)                                                    AS total_kunjungan,
    SUM(CASE WHEN k.is_rawat_inap = TRUE  THEN 1 ELSE 0 END)  AS rawat_inap,
    SUM(CASE WHEN k.is_rawat_inap = FALSE THEN 1 ELSE 0 END)  AS rawat_jalan
FROM analytics_marts.fct_kunjungan k
JOIN analytics_marts.dim_tanggal t ON k.tanggal_masuk_key = t.tanggal_key
WHERE
    t.tanggal IS NOT NULL
    AND t.tahun * 100 + t.bulan >= (
        SELECT (MAX(t2.tahun) * 100 + MAX(t2.bulan)) - 11
        FROM analytics_marts.fct_kunjungan k2
        JOIN analytics_marts.dim_tanggal t2 ON k2.tanggal_masuk_key = t2.tanggal_key
        WHERE t2.tanggal IS NOT NULL
    )
GROUP BY t.tahun, t.bulan, t.nama_bulan
ORDER BY t.tahun, t.bulan
```

**Contoh hasil:**

| periode | total_kunjungan | rawat_inap | rawat_jalan |
|---------|----------------:|-----------:|------------:|
| 2025-04 | 14,419 | 3,462 | 10,957 |
| 2025-05 | 44,889 | 15,331 | 29,558 |
| 2025-06 | 43,605 | 21,224 | 22,381 |

---

### BQ2 — Departemen dengan Kunjungan Terbanyak dan Tersedikit

**Pertanyaan:** Departemen mana yang paling banyak dan paling sedikit dikunjungi, beserta persentase dari total?

```sql
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
    ROUND(dc.total_kunjungan * 100.0 / t.grand_total, 2) AS persentase_dari_total,
    RANK() OVER (ORDER BY dc.total_kunjungan DESC)        AS rank_tertinggi
FROM dept_counts dc
CROSS JOIN total t
ORDER BY dc.total_kunjungan DESC
```

**Contoh hasil (Top 5):**

| nama_departemen | total_kunjungan | persentase | rank |
|----------------|----------------:|-----------:|-----:|
| Radiology | 24,242 | 2.64% | 1 |
| Cardiology | 24,121 | 2.63% | 2 |
| Endocrinology | 23,650 | 2.58% | 3 |

---

### BQ3 — Rata-rata Length of Stay (LOS) per Departemen

**Pertanyaan:** Berapa rata-rata, minimum, dan maksimum durasi kunjungan pasien per departemen?

```sql
SELECT
    d.nama_departemen,
    COUNT(*)                                        AS total_kunjungan,
    ROUND(AVG(k.durasi_kunjungan_jam), 2)           AS avg_los_jam,
    ROUND(MIN(k.durasi_kunjungan_jam), 2)           AS min_los_jam,
    ROUND(MAX(k.durasi_kunjungan_jam), 2)           AS max_los_jam,
    SUM(CASE WHEN k.is_rawat_inap THEN 1 ELSE 0 END)     AS total_rawat_inap,
    SUM(CASE WHEN NOT k.is_rawat_inap THEN 1 ELSE 0 END) AS total_rawat_jalan
FROM analytics_marts.fct_kunjungan k
JOIN analytics_marts.dim_departemen d ON k.departemen_key = d.departemen_key
WHERE
    k.durasi_kunjungan_jam IS NOT NULL
    AND d.dep_id != 'UNKNOWN'
GROUP BY d.nama_departemen
ORDER BY avg_los_jam DESC
```

> **Catatan:** Filter `is_rawat_inap = TRUE` dihapus agar mencakup semua kunjungan (bukan hanya 16% yang Admitted). Ini menghasilkan data yang lebih representatif.

---

### BQ4 — Beban Kerja Tertinggi per Departemen per Bulan

**Pertanyaan:** Departemen mana yang memiliki beban kunjungan tertinggi setiap bulannya?

> **Catatan:** BQ4 menggunakan **departemen** sebagai unit analisis (bukan dokter) karena 99.8% kunjungan di dataset tidak memiliki data dokter yang ter-assign (`dokter_key = -1`), sehingga analisis per departemen lebih representatif.

```sql
SELECT
    t.tahun,
    t.bulan,
    t.nama_bulan,
    d.nama_departemen,
    COUNT(*)                                              AS total_kunjungan,
    SUM(CASE WHEN k.is_rawat_inap THEN 1 ELSE 0 END)     AS rawat_inap,
    SUM(CASE WHEN NOT k.is_rawat_inap THEN 1 ELSE 0 END) AS rawat_jalan,
    RANK() OVER (
        PARTITION BY t.tahun, t.bulan
        ORDER BY COUNT(*) DESC
    )                                                     AS rank_departemen
FROM analytics_marts.fct_kunjungan k
JOIN analytics_marts.dim_departemen d ON k.departemen_key  = d.departemen_key
JOIN analytics_marts.dim_tanggal    t ON k.tanggal_masuk_key = t.tanggal_key
WHERE
    d.dep_id  != 'UNKNOWN'
    AND t.tanggal IS NOT NULL
GROUP BY t.tahun, t.bulan, t.nama_bulan, d.nama_departemen
ORDER BY t.tahun, t.bulan, total_kunjungan DESC
```

---

### BQ5 — Konversi Appointment ke Kunjungan Aktual

**Pertanyaan:** Berapa persen appointment yang berhasil dikonversi menjadi kunjungan aktual setiap bulannya?

```sql
SELECT
    t.tahun,
    t.bulan,
    t.nama_bulan,
    CAST(t.tahun AS VARCHAR) || '-' || LPAD(CAST(t.bulan AS VARCHAR), 2, '0') AS periode,
    COUNT(*)                                                        AS total_dari_appointment,
    SUM(CASE WHEN k.is_appointment_converted THEN 1 ELSE 0 END)    AS total_converted,
    COUNT(*) - SUM(CASE WHEN k.is_appointment_converted THEN 1 ELSE 0 END) AS total_tidak_converted,
    ROUND(
        SUM(CASE WHEN k.is_appointment_converted THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*), 2
    )                                                               AS conversion_rate_persen
FROM analytics_marts.fct_kunjungan k
JOIN analytics_marts.dim_tanggal t ON k.tanggal_masuk_key = t.tanggal_key
WHERE
    k.is_dari_appointment = TRUE
    AND t.tanggal IS NOT NULL
GROUP BY t.tahun, t.bulan, t.nama_bulan
ORDER BY t.tahun ASC, t.bulan ASC
```

**Contoh hasil:**

| periode | total_appt | converted | rate_% |
|---------|-----------:|----------:|-------:|
| 2024-01 | 13,737 | 12,526 | 91.18% |
| 2024-02 | 14,688 | 12,548 | 85.43% |
| 2024-03 | 15,373 | 13,191 | 85.81% |

**Insight:** Conversion rate stabil di ~85–86% sepanjang tahun, menunjukkan sistem appointment berjalan efektif.

---

## 9. Visualisasi di Apache Superset

Apache Superset tersedia di **http://localhost:8088** (admin / admin).  
Superset terhubung ke DuckDB melalui database connection bernama **"Healthcare DuckDB"** dengan schema `analytics_marts`.

### Cara Kerja Koneksi Superset ↔ DuckDB

```
Airflow DAG selesai → menulis ke healthcare.duckdb
                                  ↓
Superset membaca dari file yang sama (duckdb-engine)
                                  ↓
Setiap kali Anda query/refresh chart → data selalu terbaru
```

> Data di Superset **tidak otomatis refresh**. Superset hanya membaca data terbaru saat Anda membuka chart atau mengklik refresh. Tidak ada delay — begitu DAG selesai, Superset langsung bisa membaca hasil terbarunya.

---

### Cara Membuat Chart BQ1 — Tren Kunjungan per Bulan

**Tipe Chart yang digunakan:** Line Chart + Bar Chart Stacked

**Langkah:**

1. Buka **SQL Lab** → paste SQL BQ1 → klik **Run**
2. Klik **Save** → pilih **"Save as Dataset"** → beri nama `BQ1 Tren Kunjungan Bulanan`
3. Buka menu **Charts** → klik **+ Chart**
4. Pilih dataset `BQ1 Tren Kunjungan Bulanan`

**Chart 1 — Line Chart (Tren Total Kunjungan):**

| Setting | Nilai |
|---------|-------|
| Chart Type | Line Chart |
| X-axis | `periode` |
| Metrics | `SUM(total_kunjungan)` |
| Show Markers | ✅ Yes |

5. Klik **Save** → nama: `BQ1 - Tren Total Kunjungan per Bulan`

**Chart 2 — Stacked Bar (Rawat Inap vs Rawat Jalan):**

| Setting | Nilai |
|---------|-------|
| Chart Type | Bar Chart |
| X-axis | `periode` |
| Metrics | `SUM(rawat_inap)`, `SUM(rawat_jalan)` |
| Stacked | ✅ Yes |

6. Klik **Save** → nama: `BQ1 - Rawat Inap vs Rawat Jalan per Bulan`

---

### Cara Membuat Chart BQ2 — Top Departemen

**Tipe Chart yang digunakan:** Bar Chart Horizontal + Pie/Donut Chart

**Langkah:**

1. Buka **SQL Lab** → paste SQL BQ2 → klik **Run**
2. **Save as Dataset** → nama: `BQ2 Top Departemen`
3. Buka **Charts** → **+ Chart** → pilih dataset `BQ2 Top Departemen`

**Chart 1 — Bar Chart Horizontal (Ranking):**

| Setting | Nilai |
|---------|-------|
| Chart Type | Bar Chart (aktifkan Horizontal) |
| X-axis | `nama_departemen` |
| Metrics | `SUM(total_kunjungan)` |
| Sort By | `SUM(total_kunjungan)` Descending |
| Row Limit | 10 |
| Show Values | ✅ Yes |

4. Klik **Save** → nama: `BQ2 - Ranking Kunjungan per Departemen`

**Chart 2 — Donut Chart (Proporsi):**

| Setting | Nilai |
|---------|-------|
| Chart Type | Donut Chart |
| Dimension | `nama_departemen` |
| Metric | `SUM(total_kunjungan)` |
| Row Limit | 10 |
| Show Labels | ✅ Yes |

5. Klik **Save** → nama: `BQ2 - Proporsi Kunjungan per Departemen`

---

### Cara Membuat Chart BQ3 — Length of Stay per Departemen

**Tipe Chart yang digunakan:** Bar Chart Horizontal + Table Chart

**Langkah:**

1. Buka **SQL Lab** → paste SQL BQ3 → klik **Run**
2. **Save as Dataset** → nama: `BQ3 Length of Stay`
3. Buka **Charts** → **+ Chart** → pilih dataset `BQ3 Length of Stay`

**Chart 1 — Bar Chart Horizontal (Rata-rata LOS):**

| Setting | Nilai |
|---------|-------|
| Chart Type | Bar Chart (Horizontal) |
| X-axis | `nama_departemen` |
| Metrics | `SUM(avg_los_jam)` |
| Sort By | `SUM(avg_los_jam)` Descending |
| Show Values | ✅ Yes |

4. Klik **Save** → nama: `BQ3 - Rata-rata LOS per Departemen`

**Chart 2 — Table Chart (Detail Lengkap):**

| Setting | Nilai |
|---------|-------|
| Chart Type | Table |
| Columns | `nama_departemen`, `SUM(total_kunjungan)`, `SUM(avg_los_jam)`, `SUM(min_los_jam)`, `SUM(max_los_jam)` |
| Sort By | `SUM(avg_los_jam)` Descending |
| Page Size | 30 |

5. Klik **Save** → nama: `BQ3 - Detail LOS per Departemen`

---

### Cara Membuat Chart BQ4 — Beban Kerja Departemen per Bulan

**Tipe Chart yang digunakan:** Table Chart + Heatmap

**Langkah:**

1. Buka **SQL Lab** → paste SQL BQ4 → klik **Run**
2. **Save as Dataset** → nama: `BQ4 Beban Kerja Departemen`
3. Buka **Charts** → **+ Chart** → pilih dataset `BQ4 Beban Kerja Departemen`

**Chart 1 — Table Chart (Detail per Bulan):**

| Setting | Nilai |
|---------|-------|
| Chart Type | Table |
| Columns | `tahun`, `nama_bulan`, `nama_departemen`, `SUM(total_kunjungan)`, `SUM(rawat_inap)`, `SUM(rawat_jalan)` |
| Sort By | `tahun` ASC, `bulan` ASC, `SUM(total_kunjungan)` DESC |
| Page Size | 20 |

4. Klik **Save** → nama: `BQ4 - Beban Kerja Departemen per Bulan`

**Chart 2 — Heatmap (Intensitas Beban):**

| Setting | Nilai |
|---------|-------|
| Chart Type | Heatmap |
| X-axis | `nama_bulan` |
| Y-axis | `nama_departemen` |
| Metric | `SUM(total_kunjungan)` |
| Color Scheme | Blue-Red (merah = beban tertinggi) |

5. Klik **Save** → nama: `BQ4 - Heatmap Beban Kerja Departemen`

---

### Cara Membuat Chart BQ5 — Konversi Appointment

**Tipe Chart yang digunakan:** Line Chart + Bar Chart Grouped + Big Number

**Langkah:**

1. Buka **SQL Lab** → paste SQL BQ5 → klik **Run**
2. **Save as Dataset** → nama: `BQ5 Konversi Appointment`
3. Buka **Charts** → **+ Chart** → pilih dataset `BQ5 Konversi Appointment`

**Chart 1 — Line Chart (Tren Conversion Rate):**

| Setting | Nilai |
|---------|-------|
| Chart Type | Line Chart |
| X-axis | `periode` |
| Metrics | `SUM(conversion_rate_persen)` |
| Y-axis Label | `Conversion Rate (%)` |
| Show Markers | ✅ Yes |

4. Klik **Save** → nama: `BQ5 - Tren Conversion Rate Appointment`

**Chart 2 — Bar Chart Grouped (Converted vs Tidak):**

| Setting | Nilai |
|---------|-------|
| Chart Type | Bar Chart |
| X-axis | `periode` |
| Metrics | `SUM(total_converted)`, `SUM(total_tidak_converted)` |
| Stacked | ❌ No (Grouped) |
| Show Values | ✅ Yes |

5. Klik **Save** → nama: `BQ5 - Converted vs Tidak per Bulan`

**Chart 3 — Big Number (Overall Rate):**

Buat dataset baru dari SQL berikut di SQL Lab:

```sql
SELECT
    ROUND(
        SUM(CASE WHEN is_appointment_converted THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*), 2
    ) AS avg_conversion_rate
FROM analytics_marts.fct_kunjungan
WHERE is_dari_appointment = TRUE
```

| Setting | Nilai |
|---------|-------|
| Chart Type | Big Number with Trendline |
| Metric | `SUM(avg_conversion_rate)` |
| Subheader | `Overall Appointment Conversion Rate` |

6. Klik **Save** → nama: `BQ5 - Overall Conversion Rate`

---

### Merangkai Dashboard

Setelah semua chart dibuat:

1. Buka menu **Dashboards** → klik **+ Dashboard**
2. Beri nama: `Healthcare Data Mart — Overview`
3. Klik **Edit Dashboard** → drag chart-chart ke kanvas
4. Susun layout yang direkomendasikan:

```
┌─────────────────────────────┬──────────────────┐
│  BQ5 - Big Number (Rate)    │  BQ2 - Donut     │
├─────────────────────────────┴──────────────────┤
│         BQ1 - Line Chart (Tren Kunjungan)       │
├─────────────────────────────┬──────────────────┤
│  BQ1 - Stacked Bar          │  BQ2 - Bar Horiz │
├─────────────────────────────┴──────────────────┤
│         BQ5 - Line Chart (Conversion Rate)      │
├─────────────────────────────┬──────────────────┤
│  BQ3 - Bar LOS              │  BQ4 - Heatmap   │
└─────────────────────────────┴──────────────────┘
```

5. Klik **Save** → Dashboard siap digunakan

---

## 10. Akses Web UI

| Layanan | URL | Login | Keterangan |
|---------|-----|-------|------------|
| Airflow UI | http://localhost:8080 | admin / admin | Monitor dan trigger DAG |
| dbt Docs | http://localhost:8081 | — | Lineage graph & dokumentasi model |
| Apache Superset | http://localhost:8088 | admin / admin | Dashboard & visualisasi data mart |

**Di dbt Docs** (port 8081) tersedia:
- Lineage graph dari semua model (raw → staging → intermediate → marts)
- Dokumentasi kolom dan deskripsi tiap tabel
- Hasil dbt test per model

---

## 11. Catatan Data Issues

Penyesuaian yang ditemukan saat eksplorasi data aktual Kaggle:

| # | Issue | Solusi yang Diterapkan |
|---|-------|------------------------|
| 1 | `STFF.ROLE_CD` menggunakan kode numerik, bukan string `'DOC'` | Filter dokter pakai `ROLE_DES = 'Doctor'` (ROLE_CD = `'0'`) |
| 2 | `APPT.REFR_NO` (numerik) berbeda format dari `VIST.REFR_NO` (W-prefix) | Join appointment ke kunjungan via `PAT_ID + tanggal proximity ≤30 hari` |
| 3 | `VTYPE_DES` hanya berisi `'Diagnosis'` dan `'Treatment'` (bukan Inpatient/Outpatient) | `is_rawat_inap` menggunakan `VSTAT_DES = 'Admitted'` |
| 4 | `DSTAT_DES` tidak menggunakan kode `'CNF'`/`'TEN'` | `is_confirmed` pakai `DSTAT_DES LIKE '%Confirmed%' OR = 'Recurrent'` |
| 5 | `TRTM_TOT` berisi nilai 1–10 (bukan nominal rupiah) | Diperlakukan sebagai measures unit/kuantitas |
| 6 | dbt profile target `analytics` menambahkan prefix ke nama schema | Schema aktual di DuckDB: `analytics_staging`, `analytics_intermediate`, `analytics_marts` |
| 7 | 99.8% kunjungan tidak memiliki `dokter_key` valid (= -1) | BQ4 dialihkan ke analisis per departemen yang datanya 100% lengkap |
| 8 | `dim_dokter` menggunakan kolom `nama_lengkap` dan `deskripsi_peran` | Query yang merujuk dokter harus pakai kolom `nama_lengkap` (bukan `nama_dokter`) |

---

## 12. Hasil Implementasi

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

---

## 13. Troubleshooting

**Container tidak mau start:**
```bash
docker compose down && docker compose up -d
```

**DAG tidak muncul di Airflow UI:**
```bash
docker exec healthcare_airflow_scheduler airflow dags list-import-errors
```

**dbt run gagal dengan "table not found":**
```bash
# Pastikan ingest sudah selesai, lalu cek schema
docker exec healthcare_airflow_webserver python3 -c "
import duckdb
con = duckdb.connect('/opt/airflow/duckdb/healthcare.duckdb')
print(con.execute('SHOW ALL TABLES').fetchdf())
"
```

**dbt_run_marts gagal dengan exit code 137 (OOM):**
```bash
# Pastikan --threads 1 sudah ada di healthcare_pipeline.py
# Baris yang harus ada:
# bash_command=f'... dbt run --select marts --threads 1 --profiles-dir . 2>&1'
```

**Superset tidak bisa connect ke DuckDB:**
```bash
# Cek duckdb-engine terinstall di container superset
docker exec healthcare_superset pip show duckdb-engine
```

**File CSV tidak ditemukan saat DAG jalan:**
- Pastikan `data/archive/STG_EHP_DATASET/` ada dan berisi semua file
- Cek volume mount di `docker-compose.yml`: `./data:/opt/airflow/data`
- Pastikan subfolder `STG_EHP__DIAG/` dengan 2 file CSV juga ada

**Query di Superset error "column not found":**
- Cek nama kolom aktual dengan query:
  ```sql
  SELECT column_name FROM information_schema.columns
  WHERE table_schema = 'analytics_marts' AND table_name = 'nama_tabel'
  ORDER BY ordinal_position
  ```

---

*Dibuat untuk keperluan tugas akhir S2 — Data Warehouse & Inteligensia Bisnis, Universitas Gadjah Mada.*
