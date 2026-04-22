{{ config(materialized='view', schema='staging') }}

-- Hanya filter staff dengan ROLE_DES = 'Doctor' (ROLE_CD = '0')
select
    STF_ID                              as stf_id,
    trim(F_NAME)                        as nama_depan,
    trim(coalesce(M_NAME, ''))          as nama_tengah,
    trim(L_NAME)                        as nama_belakang,
    trim(F_NAME || ' ' || L_NAME)       as nama_lengkap,
    GEN_CD                              as kode_jenis_kelamin,
    GEN_DES                             as jenis_kelamin,
    try_cast(DT_BRT as date)            as tanggal_lahir,
    DEP_ID                              as dep_id,
    ROLE_CD                             as kode_peran,
    ROLE_DES                            as deskripsi_peran,
    STAT_CD                             as kode_status,
    STAT_DES                            as status_karyawan,
    try_cast(HIRE_DATE as date)         as tanggal_masuk,
    try_cast(ROLE_DATE as date)         as tanggal_peran,
    try_cast(SAL_YR as decimal(15,2))   as gaji_tahunan,
    CON_NO                              as nomor_kontak,
    EM_ADD                              as email,
    INGST_TMSTMP                        as ingst_tmstmp
from {{ source('raw_kaggle', 'stg_ehp__stff') }}
where ROLE_DES = 'Doctor'
  and STF_ID is not null
