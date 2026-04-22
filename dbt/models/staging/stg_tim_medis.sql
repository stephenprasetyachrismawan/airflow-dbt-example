{{ config(materialized='view', schema='staging') }}

select
    MEDT_ID                             as medt_id,
    TEAM_NO                             as nomor_tim,
    STF_ID                              as stf_id,
    ROLE_CD                             as kode_peran,
    ROLE_DES                            as deskripsi_peran,
    case when ROLE_DES = 'Doctor' then true else false end as is_dokter
from {{ source('raw_kaggle', 'stg_ehp__medt') }}
where MEDT_ID is not null
  and STF_ID is not null
