{{ config(
    materialized='table',
    schema='marts',
    tags=['marts', 'dimension', 'room']
) }}

with room as (
    select * from {{ ref('stg_room') }}
),

department as (
    select * from {{ ref('stg_department') }}
),

enriched as (
    select
        row_number() over (order by r.room_id) as room_key,
        r.room_id,
        r.room_type_code,
        r.room_type_description,
        r.room_purpose_code,
        r.room_purpose_description,
        r.room_capacity,
        r.department_id,
        d.department_name,
        d.department_location,
        concat(d.department_name, ' - ', r.room_type_description) as room_description,
        r.dw_load_date
    from room r
    left join department d on r.department_id = d.department_id
)

select * from enriched
