{{ config(
    materialized='view',
    schema='staging',
    tags=['staging', 'room']
) }}

with source as (
    select
        rom_id,
        dep_id,
        prps_cd,
        prps_des,
        rtype_cd,
        rtype_des,
        cpct_no,
        current_timestamp as dw_load_date
    from {{ source('healthcare_raw', 'stg_ehp__roms') }}
)

select
    rom_id as room_id,
    dep_id as department_id,
    prps_cd as room_purpose_code,
    prps_des as room_purpose_description,
    rtype_cd as room_type_code,
    rtype_des as room_type_description,
    cpct_no as room_capacity,
    dw_load_date
from source
