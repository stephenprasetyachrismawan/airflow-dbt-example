# Deskripsi Dataset — EHP Healthcare Data Set

---

## Informasi Umum

| Item | Detail |
|------|--------|
| **Nama Dataset** | Health Care Data Set (20 Tables) |
| **Kode Sistem** | STG_EHP (Electronic Health Platform) |
| **Sumber** | Kaggle — [moid1234/health-care-data-set-20-tables](https://www.kaggle.com/datasets/moid1234/health-care-data-set-20-tables) |
| **Format** | CSV (Comma-Separated Values) |
| **Total File** | 11 file CSV (9 root + 2 subfolder DIAG) |
| **Total Baris** | ±5,561,263 baris (seluruh tabel) |
| **Cakupan Waktu** | 2020 – 2027 (data kunjungan pasien) |
| **Domain** | Healthcare / Rekam Medis Rumah Sakit |
| **Lisensi** | Kaggle Dataset License |

---

## Konteks Dataset

Dataset ini merepresentasikan data operasional sebuah sistem informasi rumah sakit yang disebut **EHP (Electronic Health Platform)**. Data mencakup seluruh siklus layanan medis mulai dari pendaftaran pasien, penjadwalan appointment, kunjungan, diagnosis, tindakan medis, tim medis, hingga asuransi.

Dataset ini dirancang sebagai **staging layer** (prefix `STG_`) yang mencerminkan data mentah dari sistem sumber sebelum diproses lebih lanjut ke dalam data warehouse.

---

## Tabel-Tabel Dataset

### Gambaran Umum 10 Tabel In-Scope

| No | Nama File | Nama Tabel | Baris | Keterangan |
|----|-----------|-----------|------:|------------|
| 1 | `STG_EHP__PATN.csv` | Patient | 351,765 | Data demografis pasien |
| 2 | `STG_EHP__VIST.csv` | Visit | 917,331 | Data kunjungan pasien |
| 3 | `STG_EHP__APPT.csv` | Appointment | 974,032 | Data penjadwalan appointment |
| 4 | `STG_EHP__TRTM.csv` | Treatment | 917,191 | Data tindakan medis |
| 5 | `STG_EHP__STFF.csv` | Staff | 5,496 | Data staf rumah sakit |
| 6 | `STG_EHP__DPMT.csv` | Department | 30 | Data departemen |
| 7 | `STG_EHP__ROMS.csv` | Rooms | 24,302 | Data ruangan |
| 8 | `STG_EHP__MEDT.csv` | Medical Team | 312,870 | Data tim medis |
| 9 | `STG_EHP__INSR.csv` | Insurance | 457,072 | Data asuransi pasien |
| 10 | `STG_EHP__DIAG/STG_EHP__DIAG_1.csv` + `_2.csv` | Diagnosis | 1,602,174 | Data diagnosis (UNION ALL 2 file) |
| | **TOTAL** | | **5,561,263** | |

---

## Skema Kolom per Tabel

### 1. STG_EHP__PATN — Data Pasien (351,765 baris)

Menyimpan informasi demografis dan identitas setiap pasien.

| Kolom Asli | Tipe Data | Deskripsi | Nama di Data Mart |
|-----------|-----------|-----------|------------------|
| `PAT_ID` | VARCHAR | ID unik pasien (natural key) | `pat_id` |
| `F_NAME` | VARCHAR | Nama depan pasien | `nama_depan` |
| `L_NAME` | VARCHAR | Nama belakang pasien | `nama_belakang` |
| `M_NAME` | VARCHAR | Nama tengah pasien | `nama_tengah` |
| `GEN_CD` | BIGINT | Kode jenis kelamin (0/1) | `kode_jenis_kelamin` |
| `GEN_DES` | VARCHAR | Deskripsi jenis kelamin (Male/Female) | `jenis_kelamin` |
| `DT_BRT` | DATE | Tanggal lahir | `tanggal_lahir` |
| `CON_NO` | VARCHAR | Nomor kontak pasien | `nomor_kontak` |
| `EM_ADD` | VARCHAR | Alamat email pasien | `email` |
| `EC_F_NAME` | VARCHAR | Nama depan kontak darurat | — |
| `EC_L_NAME` | VARCHAR | Nama belakang kontak darurat | — |
| `EC_M_NAME` | VARCHAR | Nama tengah kontak darurat | — |
| `EC_CON_NO` | VARCHAR | Nomor kontak darurat | — |
| `B_TYPE` | VARCHAR | Golongan darah (A/B/AB/O) | `golongan_darah` |
| `MAR_ST` | VARCHAR | Status pernikahan | `status_pernikahan` |
| `NAT_ID_S` | VARCHAR | Seri ID nasional | — |
| `NAT_ID_T` | VARCHAR | Tipe ID nasional | — |
| `NAT_ID_N` | VARCHAR | Nomor ID nasional | — |
| `NAT_ID_E` | DATE | Tanggal kadaluarsa ID nasional | — |
| `INGST_TMSTMP` | VARCHAR | Timestamp ingest data | — |

---

### 2. STG_EHP__VIST — Data Kunjungan (917,331 baris)

Menyimpan setiap kunjungan pasien ke rumah sakit. Ini adalah **tabel fakta utama** dari sistem sumber.

| Kolom Asli | Tipe Data | Deskripsi | Nama di Data Mart |
|-----------|-----------|-----------|------------------|
| `REFR_NO` | VARCHAR | Nomor referensi kunjungan (degenerate dim) | `refr_no` |
| `PAT_ID` | VARCHAR | ID pasien (FK ke PATN) | `pasien_key` |
| `MEDT_ID` | VARCHAR | ID tim medis yang menangani | `medt_id` |
| `VIS_EN` | TIMESTAMP | Waktu masuk kunjungan | `waktu_masuk` |
| `VIS_EX` | TIMESTAMP | Waktu keluar kunjungan | `waktu_keluar` |
| `VSTAT_CD` | BIGINT | Kode status kunjungan (0/1/2) | `kode_status_kunjungan` |
| `VSTAT_DES` | VARCHAR | Deskripsi status: Discharged / Admitted / Need Follow-Up | `deskripsi_status_kunjungan` |
| `VTYPE_CD` | BIGINT | Kode tipe kunjungan | `kode_tipe_kunjungan` |
| `VTYPE_DES` | VARCHAR | Deskripsi tipe: Diagnosis / Treatment | `deskripsi_tipe_kunjungan` |
| `ROM_ID` | VARCHAR | ID ruangan tempat kunjungan (FK ke ROMS) | `ruangan_key` |

**Catatan penting:**
- `VSTAT_DES = 'Admitted'` → digunakan sebagai flag `is_rawat_inap = TRUE`
- `VTYPE_DES` hanya berisi `'Diagnosis'` dan `'Treatment'` — bukan Inpatient/Outpatient
- Distribusi status: Discharged (385,359) | Admitted (146,408) | Need Follow-Up (385,749)

---

### 3. STG_EHP__APPT — Data Appointment (974,032 baris)

Menyimpan data penjadwalan appointment pasien sebelum kunjungan aktual.

| Kolom Asli | Tipe Data | Deskripsi | Nama di Data Mart |
|-----------|-----------|-----------|------------------|
| `REFR_NO` | BIGINT | Nomor referensi appointment | `appt_refr_no` |
| `PAT_ID` | VARCHAR | ID pasien (FK ke PATN) | `pasien_key` |
| `APT_TIME` | TIMESTAMP | Waktu appointment dijadwalkan | `waktu_appointment` |
| `CTD_ID` | VARCHAR | ID yang membuat appointment | — |
| `DOC_ID` | VARCHAR | ID dokter yang dituju | — |
| `ASTAT_CD` | BIGINT | Kode status appointment | `kode_status_appointment` |
| `ASTAT_DES` | VARCHAR | Deskripsi status appointment | `deskripsi_status_appointment` |
| `ATYPE_CD` | BIGINT | Kode tipe appointment | `kode_tipe_appointment` |
| `ATYPE_DES` | VARCHAR | Deskripsi tipe appointment | — |

**Catatan penting:**
- `REFR_NO` di APPT bertipe **BIGINT** (numerik), sedangkan di VIST bertipe **VARCHAR** (W-prefix)
- Join APPT → VIST tidak bisa via REFR_NO langsung; dilakukan via `PAT_ID + tanggal proximity ≤30 hari`
- 317,030 dari 917,516 kunjungan berasal dari appointment (34.5%)

---

### 4. STG_EHP__TRTM — Data Tindakan Medis (917,191 baris)

Menyimpan setiap tindakan atau prosedur medis yang dilakukan selama kunjungan.

| Kolom Asli | Tipe Data | Deskripsi | Nama di Data Mart |
|-----------|-----------|-----------|------------------|
| `REFR_NO` | VARCHAR | Nomor referensi kunjungan terkait | `refr_no` |
| `TRTM_TOT` | BIGINT | Total nilai tindakan (skala 1–10, bukan rupiah) | `total_biaya_tindakan` |
| `TRTM_EN` | TIMESTAMP | Waktu tindakan dilakukan | `tanggal_tindakan_key` |
| `TTYPE_CD` | BIGINT | Kode tipe tindakan | `kode_tipe_tindakan` |
| `TTYPE_DES` | VARCHAR | Deskripsi tipe tindakan | `deskripsi_tipe_tindakan` |
| `TRTM_CD` | BIGINT | Kode tindakan | `kode_tindakan` |
| `TSTAT_CD` | BIGINT | Kode status tindakan | `kode_status_tindakan` |
| `TRTM_DES` | VARCHAR | Deskripsi tindakan | `deskripsi_tindakan` |
| `TSTAT_DES` | VARCHAR | Deskripsi status tindakan | `deskripsi_status_tindakan` |
| `TRTM_SEQ` | BIGINT | Urutan tindakan dalam satu kunjungan | `urutan_tindakan` |

**Catatan penting:**
- `TRTM_TOT` berisi nilai 1–10, bukan nominal rupiah — kemungkinan kode kompleksitas atau unit prosedur
- Flag `is_operasi` dan `is_obat` diturunkan dari `TTYPE_DES`

---

### 5. STG_EHP__STFF — Data Staf (5,496 baris)

Menyimpan data seluruh staf rumah sakit, termasuk dokter, perawat, dan administrasi.

| Kolom Asli | Tipe Data | Deskripsi | Nama di Data Mart |
|-----------|-----------|-----------|------------------|
| `STF_ID` | VARCHAR | ID unik staf (natural key) | `stf_id` |
| `F_NAME` | VARCHAR | Nama depan staf | `nama_depan` |
| `L_NAME` | VARCHAR | Nama belakang staf | `nama_belakang` |
| `M_NAME` | VARCHAR | Nama tengah staf | `nama_tengah` |
| `GEN_CD` | BIGINT | Kode jenis kelamin | — |
| `GEN_DES` | VARCHAR | Deskripsi jenis kelamin | `jenis_kelamin` |
| `DT_BRT` | DATE | Tanggal lahir staf | `tanggal_lahir` |
| `DEP_ID` | VARCHAR | ID departemen tempat bertugas | `nama_departemen` |
| `ROLE_CD` | BIGINT | Kode peran staf (0 = Doctor) | `kode_peran` |
| `ROLE_DES` | VARCHAR | Deskripsi peran: Doctor / Nurse / Admin / dll | `deskripsi_peran` |
| `CON_NO` | VARCHAR | Nomor kontak staf | — |
| `EM_ADD` | VARCHAR | Email staf | — |
| `STAT_CD` | BIGINT | Kode status kepegawaian | — |
| `STAT_DES` | VARCHAR | Deskripsi status kepegawaian | — |
| `HIRE_DATE` | DATE | Tanggal mulai bekerja | `tanggal_bergabung` |
| `ROLE_DATE` | DATE | Tanggal mulai peran saat ini | — |
| `SAL_YR` | BIGINT | Gaji tahunan | — |
| `SAL_MN` | DOUBLE | Gaji bulanan | — |
| `INGST_TMSTMP` | VARCHAR | Timestamp ingest data | — |

**Catatan penting:**
- `ROLE_CD = 0` → Doctor; filter dokter menggunakan `ROLE_DES = 'Doctor'`
- Total dokter: **384 orang** dari 5,496 total staf
- 99.8% kunjungan tidak memiliki data dokter yang ter-assign (keterbatasan data sumber)

---

### 6. STG_EHP__DPMT — Data Departemen (30 baris)

Menyimpan daftar departemen medis di rumah sakit.

| Kolom Asli | Tipe Data | Deskripsi | Nama di Data Mart |
|-----------|-----------|-----------|------------------|
| `DEP_ID` | VARCHAR | ID unik departemen (natural key) | `dep_id` |
| `DEP_NAME` | VARCHAR | Nama departemen | `nama_departemen` |
| `DEP_DES` | VARCHAR | Deskripsi departemen | `deskripsi_departemen` |
| `DEP_LOC` | VARCHAR | Lokasi/lantai departemen | `lokasi_departemen` |

**30 Departemen dalam dataset:**
Radiology, Cardiology, Endocrinology, Hematology, Psychiatry, Oncology, Neurology, Orthopedics, Gastroenterology, Pulmonology, Nephrology, Rheumatology, Dermatology, Ophthalmology, Urology, Gynecology, Pediatrics, Geriatrics, Surgery, Internal Medicine, Emergency, ICU, dan lainnya.

---

### 7. STG_EHP__ROMS — Data Ruangan (24,302 baris)

Menyimpan data ruangan yang tersedia di setiap departemen.

| Kolom Asli | Tipe Data | Deskripsi | Nama di Data Mart |
|-----------|-----------|-----------|------------------|
| `ROM_ID` | VARCHAR | ID unik ruangan (natural key) | `rom_id` |
| `DEP_ID` | VARCHAR | ID departemen pemilik ruangan | `departemen_id` |
| `PRPS_CD` | BIGINT | Kode tujuan ruangan | — |
| `PRPS_DES` | VARCHAR | Deskripsi tujuan ruangan | `tujuan_ruangan` |
| `RTYPE_CD` | BIGINT | Kode tipe ruangan | — |
| `RTYPE_DES` | VARCHAR | Deskripsi tipe: Ward / Clinic / Lab / dll | `tipe_ruangan` |
| `CPCT_NO` | BIGINT | Kapasitas ruangan (jumlah tempat tidur/kursi) | `kapasitas` |

---

### 8. STG_EHP__MEDT — Data Tim Medis (312,870 baris)

Tabel jembatan (bridge table) yang menghubungkan staf dengan tim medis per kunjungan.

| Kolom Asli | Tipe Data | Deskripsi | Nama di Data Mart |
|-----------|-----------|-----------|------------------|
| `MEDT_ID` | VARCHAR | ID tim medis (FK ke VIST.MEDT_ID) | `medt_id` |
| `TEAM_NO` | BIGINT | Nomor urut anggota dalam tim | `nomor_tim` |
| `STF_ID` | VARCHAR | ID staf anggota tim (FK ke STFF) | `stf_id` |
| `ROLE_CD` | BIGINT | Kode peran dalam tim | `kode_peran_tim` |
| `ROLE_DES` | VARCHAR | Deskripsi peran dalam tim | `deskripsi_peran_tim` |

**Catatan:** Tabel ini berfungsi sebagai many-to-many bridge antara kunjungan dan staf — satu kunjungan bisa ditangani oleh banyak staf, dan satu staf bisa menangani banyak kunjungan.

---

### 9. STG_EHP__INSR — Data Asuransi (457,072 baris)

Menyimpan data polis asuransi yang dimiliki pasien.

| Kolom Asli | Tipe Data | Deskripsi | Nama di Data Mart |
|-----------|-----------|-----------|------------------|
| `POL_NO` | VARCHAR | Nomor polis asuransi | `nomor_polis` |
| `PAT_ID` | VARCHAR | ID pasien pemilik polis (FK ke PATN) | `pat_id` |
| `COM_NAME` | VARCHAR | Nama perusahaan asuransi | `nama_perusahaan_asuransi` |
| `ITYPE_CD` | VARCHAR | Kode tipe asuransi | `kode_tipe_asuransi` |
| `ITYPE_DES` | VARCHAR | Deskripsi tipe asuransi | `tipe_asuransi` |
| `POL_ST` | VARCHAR | Tanggal mulai polis | `tanggal_mulai_polis` |
| `POL_ED` | VARCHAR | Tanggal berakhir polis | `tanggal_berakhir_polis` |
| `ISTAT_CD` | BIGINT | Kode status polis | `kode_status_asuransi` |
| `ISTAT_DES` | VARCHAR | Deskripsi status: Active / Expired / dll | `status_asuransi` |
| `CON_NO` | VARCHAR | Nomor kontak perusahaan asuransi | — |

---

### 10. STG_EHP__DIAG — Data Diagnosis (1,602,174 baris)

Menyimpan setiap diagnosis yang diberikan selama kunjungan. Terdiri dari **2 file** yang di-UNION ALL:
- `STG_EHP__DIAG/STG_EHP__DIAG_1.csv` → 950,000 baris
- `STG_EHP__DIAG/STG_EHP__DIAG_2.csv` → 652,174 baris

| Kolom Asli | Tipe Data | Deskripsi | Nama di Data Mart |
|-----------|-----------|-----------|------------------|
| `REFR_NO` | VARCHAR | Nomor referensi kunjungan terkait | `refr_no` |
| `DIG_TOT` | BIGINT | Total nilai diagnosis (biaya/skala) | `total_biaya_diagnosis` |
| `DIG_EN` | TIMESTAMP | Waktu diagnosis dilakukan | `tanggal_diagnosis_key` |
| `DIG_SEQ` | BIGINT | Urutan diagnosis dalam kunjungan | `urutan_diagnosis` |
| `DIG_CD` | BIGINT | Kode diagnosis | `kode_diagnosis` |
| `DIG_DES` | VARCHAR | Deskripsi/nama diagnosis | `deskripsi_diagnosis` |
| `DSTAT_CD` | BIGINT | Kode status diagnosis | `kode_status` |
| `DSTAT_DES` | VARCHAR | Deskripsi status: Confirmed / Recurrent / Requires Surgery / dll | `deskripsi_status` |
| `DTYPE_CD` | BIGINT | Kode tipe diagnosis | `kode_tipe` |
| `DTYPE_DES` | VARCHAR | Deskripsi tipe diagnosis | `deskripsi_tipe` |
| `ROM_ID` | VARCHAR | ID ruangan tempat diagnosis (FK ke ROMS) | `ruangan_key` |

**Catatan penting:**
- `DSTAT_DES` tidak menggunakan kode standar `'CNF'`/`'TEN'` — menggunakan deskripsi teks lengkap
- Flag `is_confirmed` diturunkan dari `DSTAT_DES LIKE '%Confirmed%' OR DSTAT_DES = 'Recurrent'`
- `DIG_SEQ = 1` digunakan sebagai flag `is_diagnosis_utama = TRUE`

---

## Relasi Antar Tabel

```
STG_EHP__PATN (PAT_ID)
    ├── STG_EHP__VIST     (PAT_ID)  ← kunjungan pasien
    ├── STG_EHP__APPT     (PAT_ID)  ← appointment pasien
    └── STG_EHP__INSR     (PAT_ID)  ← asuransi pasien

STG_EHP__VIST (REFR_NO, MEDT_ID, ROM_ID)
    ├── STG_EHP__DIAG     (REFR_NO) ← diagnosis per kunjungan
    ├── STG_EHP__TRTM     (REFR_NO) ← tindakan per kunjungan
    ├── STG_EHP__MEDT     (MEDT_ID) ← tim medis per kunjungan
    └── STG_EHP__ROMS     (ROM_ID)  ← ruangan kunjungan

STG_EHP__ROMS (DEP_ID)
    └── STG_EHP__DPMT     (DEP_ID)  ← departemen ruangan

STG_EHP__STFF (DEP_ID, STF_ID)
    ├── STG_EHP__DPMT     (DEP_ID)  ← departemen staf
    └── STG_EHP__MEDT     (STF_ID)  ← anggota tim medis
```

---

## Statistik Distribusi Data

### Row Count per Tabel

| Tabel | Baris | % dari Total |
|-------|------:|:------------:|
| STG_EHP__DIAG | 1,602,174 | 28.8% |
| STG_EHP__APPT | 974,032 | 17.5% |
| STG_EHP__VIST | 917,331 | 16.5% |
| STG_EHP__TRTM | 917,191 | 16.5% |
| STG_EHP__INSR | 457,072 | 8.2% |
| STG_EHP__MEDT | 312,870 | 5.6% |
| STG_EHP__PATN | 351,765 | 6.3% |
| STG_EHP__ROMS | 24,302 | 0.4% |
| STG_EHP__STFF | 5,496 | 0.1% |
| STG_EHP__DPMT | 30 | <0.1% |
| **TOTAL** | **5,561,263** | **100%** |

### Distribusi Kunjungan (VIST)

| Status | Jumlah | Persentase |
|--------|-------:|:---------:|
| Need Follow-Up | 385,749 | 42.1% |
| Discharged | 385,359 | 42.0% |
| Admitted (Rawat Inap) | 146,408 | 15.9% |

### Distribusi Appointment

| Kondisi | Jumlah | Persentase |
|---------|-------:|:---------:|
| Kunjungan dari appointment | 317,030 | 34.5% |
| Kunjungan langsung (walk-in) | 600,486 | 65.5% |
| Appointment yang dikonversi | 272,686 | ~86% dari appt |

---

## Catatan Kualitas Data

| # | Temuan | Detail |
|---|--------|--------|
| 1 | **STFF.ROLE_CD bukan string** | `ROLE_CD` menggunakan kode numerik (`0` = Doctor), bukan string `'DOC'` seperti yang didokumentasikan |
| 2 | **APPT.REFR_NO berbeda format** | APPT menggunakan BIGINT numerik (contoh: `6953591`), sedangkan VIST menggunakan VARCHAR dengan prefix W (contoh: `W4723062`) — tidak bisa di-join langsung |
| 3 | **VTYPE_DES terbatas** | Hanya berisi `'Diagnosis'` dan `'Treatment'` — tidak ada nilai Inpatient/Outpatient |
| 4 | **DSTAT_DES teks bebas** | Tidak menggunakan kode singkat `'CNF'`/`'TEN'` — menggunakan deskripsi panjang seperti `'Requires Surgery'`, `'Recurrent'`, dll |
| 5 | **TRTM_TOT skala kecil** | Nilai 1–10, bukan nominal biaya dalam mata uang |
| 6 | **99.8% kunjungan tanpa dokter** | Dari 917,516 kunjungan, hanya 2,034 yang memiliki data dokter ter-assign di `dim_dokter` |
| 7 | **DIAG dibagi 2 file** | Tabel diagnosis dibagi menjadi 2 file CSV (`_1` dan `_2`) karena ukuran yang besar, perlu UNION ALL saat ingest |
| 8 | **ROM_ID tidak unik di ROMS** | Terdapat 4 `ROM_ID` yang muncul duplikat di data sumber (data quality issue sumber) |

---

## Cara Download Dataset

1. Buka halaman Kaggle: https://www.kaggle.com/datasets/moid1234/health-care-data-set-20-tables
2. Login dengan akun Kaggle (gratis)
3. Klik tombol **Download** → pilih **Download All**
4. Ekstrak file ZIP ke folder `data/archive/STG_EHP_DATASET/`
5. Pastikan struktur folder sesuai:

```
data/archive/STG_EHP_DATASET/
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

---

## Pemetaan ke Data Mart

Berikut pemetaan dari tabel sumber ke model di data mart (schema `analytics_marts`):

| Tabel Sumber | Layer Staging | Layer Mart |
|-------------|--------------|------------|
| STG_EHP__PATN | `stg_pasien` | `dim_pasien` (SCD Type 2) |
| STG_EHP__VIST | `stg_kunjungan` | `fct_kunjungan` |
| STG_EHP__APPT | `stg_appointment` | (enrichment ke `fct_kunjungan`) |
| STG_EHP__TRTM | `stg_tindakan_medis` | `fct_tindakan_medis` |
| STG_EHP__STFF | `stg_staff_dokter` | `dim_dokter` (SCD Type 2) |
| STG_EHP__DPMT | `stg_departemen` | `dim_departemen` (SCD Type 1) |
| STG_EHP__ROMS | `stg_ruangan` | `dim_ruangan` (SCD Type 1) |
| STG_EHP__MEDT | `stg_tim_medis` | (bridge — enrichment ke `fct_kunjungan`) |
| STG_EHP__INSR | `stg_asuransi` | (enrichment ke `dim_pasien`) |
| STG_EHP__DIAG | `stg_diagnosis` | `fct_diagnosis` |
| *(generated)* | — | `dim_tanggal` (SCD Type 0) |

---

*Dokumentasi ini dibuat berdasarkan eksplorasi langsung terhadap file CSV dan data yang telah diingest ke DuckDB.*  
*Untuk informasi lebih lanjut tentang pipeline dan transformasi, lihat [README.md](README.md).*
