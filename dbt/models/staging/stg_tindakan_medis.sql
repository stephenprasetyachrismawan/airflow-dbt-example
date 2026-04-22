{{ config(materialized='view', schema='staging') }}

select
    REFR_NO                                 as refr_no,
    TRTM_SEQ                                as urutan_tindakan,
    TRTM_CD                                 as kode_tindakan,
    TRTM_DES                                as deskripsi_tindakan,
    TTYPE_CD                                as kode_tipe,
    TTYPE_DES                               as deskripsi_tipe,
    TSTAT_CD                                as kode_status,
    TSTAT_DES                               as deskripsi_status,
    try_cast(TRTM_EN as timestamp)          as waktu_tindakan,
    try_cast(TRTM_EN as date)               as tanggal_tindakan,
    try_cast(TRTM_TOT as decimal(15,2))     as total_biaya,
    case when TSTAT_DES = 'Completed' then true else false end as is_selesai,
    case when TTYPE_DES = 'Surgery'   then true else false end as is_operasi,
    case when TTYPE_DES = 'Medication' then true else false end as is_obat
from {{ source('raw_kaggle', 'stg_ehp__trtm') }}
where REFR_NO is not null
