{{ config(materialized='view', schema='staging') }}

select
    r.ROM_ID                            as rom_id,
    r.DEP_ID                            as dep_id,
    d.DEP_NAME                          as nama_departemen,
    r.PRPS_CD                           as kode_tujuan,
    r.PRPS_DES                          as deskripsi_tujuan,
    r.RTYPE_CD                          as kode_tipe,
    r.RTYPE_DES                         as deskripsi_tipe,
    try_cast(r.CPCT_NO as integer)      as kapasitas,
    current_timestamp                   as dw_load_date
from {{ source('raw_kaggle', 'stg_ehp__roms') }} r
left join {{ source('raw_kaggle', 'stg_ehp__dpmt') }} d
    on r.DEP_ID = d.DEP_ID
where r.ROM_ID is not null
