{{ config(
    materialized='view',
    schema='staging',
    tags=['staging', 'treatment']
) }}

with source as (
    select
        refr_no,
        trtm_tot,
        trtm_en,
        ttype_cd,
        ttype_des,
        trtm_cd,
        tstat_cd,
        trtm_des,
        tstat_des,
        trtm_seq,
        current_timestamp as dw_load_date
    from {{ source('healthcare_raw', 'stg_ehp__trtm') }}
)

select
    refr_no,
    trtm_tot as total_treatments,
    trtm_en as treatment_start_date,
    ttype_cd as treatment_type_code,
    ttype_des as treatment_type_description,
    trtm_cd as treatment_code,
    tstat_cd as treatment_status_code,
    trtm_des as treatment_description,
    tstat_des as treatment_status_description,
    trtm_seq as treatment_sequence,
    dw_load_date
from source
