{{ config(materialized='view', schema='staging') }}

select
    PAT_ID                              as pat_id,
    trim(F_NAME)                        as nama_depan,
    trim(coalesce(M_NAME, ''))          as nama_tengah,
    trim(L_NAME)                        as nama_belakang,
    trim(coalesce(F_NAME, '') || ' ' ||
         case when M_NAME is not null and M_NAME != '' then M_NAME || ' ' else '' end ||
         coalesce(L_NAME, ''))          as nama_lengkap,
    GEN_CD                              as kode_jenis_kelamin,
    GEN_DES                             as jenis_kelamin,
    try_cast(DT_BRT as date)            as tanggal_lahir,
    CON_NO                              as nomor_kontak,
    EM_ADD                              as email,
    B_TYPE                              as golongan_darah,
    MAR_ST                              as status_pernikahan,
    INGST_TMSTMP                        as ingst_tmstmp
from {{ source('raw_kaggle', 'stg_ehp__patn') }}
where PAT_ID is not null
