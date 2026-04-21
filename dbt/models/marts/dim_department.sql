{{ config(
    materialized='table',
    schema='marts',
    tags=['marts', 'dimension', 'department']
) }}

with department as (
    select * from {{ ref('stg_department') }}
),

enriched as (
    select
        row_number() over (order by department_id) as department_key,
        department_id,
        department_name,
        department_description,
        department_location,
        dw_load_date
    from department
)

select * from enriched
