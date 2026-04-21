{{ config(
    materialized='view',
    schema='staging',
    tags=['staging', 'diagnosis']
) }}

with source as (
    select
        refr_no,
        dig_tot,
        dig_en,
        dig_seq,
        dig_cd,
        dig_des,
        dstat_cd,
        dstat_des,
        dtype_cd,
        dtype_des,
        rom_id,
        current_timestamp as dw_load_date
    from {{ source('healthcare_raw', 'stg_ehp__diag') }}
)

select
    refr_no,
    dig_tot as total_diagnosis,
    dig_en as diagnosis_entry_date,
    dig_seq as diagnosis_sequence,
    dig_cd as diagnosis_code,
    dig_des as diagnosis_description,
    dstat_cd as diagnosis_status_code,
    dstat_des as diagnosis_status_description,
    dtype_cd as diagnosis_type_code,
    dtype_des as diagnosis_type_description,
    rom_id,
    dw_load_date
from source
