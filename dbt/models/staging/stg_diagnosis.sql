{{ config(materialized='view', schema='staging') }}

-- Sudah di-UNION ALL saat ingest ke raw.stg_ehp__diag
select
    REFR_NO                             as refr_no,
    DIG_SEQ                             as urutan_diagnosis,
    DIG_CD                              as kode_diagnosis,
    DIG_DES                             as deskripsi_diagnosis,
    try_cast(DIG_EN as timestamp)       as waktu_diagnosis,
    try_cast(DIG_EN as date)            as tanggal_diagnosis,
    try_cast(DIG_TOT as decimal(15,2))  as total_biaya,
    DSTAT_CD                            as kode_status,
    DSTAT_DES                           as deskripsi_status,
    DTYPE_CD                            as kode_tipe,
    DTYPE_DES                           as deskripsi_tipe,
    ROM_ID                              as rom_id
from {{ source('raw_kaggle', 'stg_ehp__diag') }}
where REFR_NO is not null
