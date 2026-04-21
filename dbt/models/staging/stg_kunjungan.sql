{{ config(materialized='view', schema='staging') }}

select
    REFR_NO                                     as refr_no,
    PAT_ID                                      as pat_id,
    MEDT_ID                                     as medt_id,
    try_cast(VIS_EN as timestamp)               as waktu_masuk,
    try_cast(VIS_EX as timestamp)               as waktu_keluar,
    try_cast(VIS_EN as date)                    as tanggal_masuk,
    try_cast(VIS_EX as date)                    as tanggal_keluar,
    cast(VIS_EN as time)                        as jam_masuk,
    case when VIS_EX is not null
         then cast(try_cast(VIS_EX as timestamp) as time) end as jam_keluar,
    VSTAT_CD                                    as kode_status,
    VSTAT_DES                                   as deskripsi_status,
    VTYPE_CD                                    as kode_tipe,
    VTYPE_DES                                   as deskripsi_tipe,
    ROM_ID                                      as rom_id,
    case when VSTAT_DES = 'Admitted' then true else false end as is_rawat_inap
from {{ source('raw_kaggle', 'stg_ehp__vist') }}
where REFR_NO is not null
