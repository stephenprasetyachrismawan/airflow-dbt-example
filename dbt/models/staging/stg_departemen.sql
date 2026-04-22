{{ config(materialized='view', schema='staging') }}

select
    DEP_ID                              as dep_id,
    DEP_NAME                            as nama_departemen,
    DEP_DES                             as deskripsi,
    DEP_LOC                             as lokasi,
    -- Ekstrak blok dari lokasi (contoh: "Block A, Floor 1")
    case
        when DEP_LOC like '%Block%'
        then trim(split_part(DEP_LOC, ',', 1))
        else null
    end                                 as blok,
    case
        when DEP_LOC like '%Floor%'
        then trim(split_part(DEP_LOC, ',', 2))
        else null
    end                                 as lantai,
    current_timestamp                   as dw_load_date
from {{ source('raw_kaggle', 'stg_ehp__dpmt') }}
where DEP_ID is not null
