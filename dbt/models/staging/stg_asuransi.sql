{{ config(materialized='view', schema='staging') }}

select
    POL_NO                              as pol_no,
    PAT_ID                              as pat_id,
    COM_NAME                            as nama_asuransi,
    ITYPE_CD                            as kode_tipe,
    ITYPE_DES                           as jenis_penjamin,
    try_cast(POL_ST as date)            as tanggal_mulai,
    try_cast(POL_ED as date)            as tanggal_selesai,
    ISTAT_CD                            as kode_status,
    ISTAT_DES                           as deskripsi_status,
    CON_NO                              as nomor_kontak,
    case when ISTAT_DES = 'Active' then true else false end as is_aktif
from {{ source('raw_kaggle', 'stg_ehp__insr') }}
where PAT_ID is not null
