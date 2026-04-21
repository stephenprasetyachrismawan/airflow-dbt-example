{{ config(
    materialized='view',
    schema='staging',
    tags=['staging', 'department']
) }}

with source as (
    select
        dep_id,
        dep_name,
        dep_des,
        dep_loc,
        current_timestamp as dw_load_date
    from {{ source('healthcare_raw', 'stg_ehp__dpmt') }}
)

select
    dep_id as department_id,
    dep_name as department_name,
    dep_des as department_description,
    dep_loc as department_location,
    dw_load_date
from source
