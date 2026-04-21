{{ config(materialized='table', schema='marts') }}

with unknown_row as (
    select
        -1              as departemen_key,
        'UNKNOWN'       as dep_id,
        'Unknown'       as nama_departemen,
        null::varchar   as deskripsi,
        null::varchar   as lokasi,
        null::varchar   as blok,
        null::varchar   as lantai,
        current_timestamp as dw_load_date,
        current_timestamp as dw_updated_date
),

base as (
    select
        row_number() over (order by dep_id) as departemen_key,
        dep_id,
        nama_departemen,
        deskripsi,
        lokasi,
        blok,
        lantai,
        dw_load_date,
        dw_load_date    as dw_updated_date
    from {{ ref('stg_departemen') }}
)

select * from unknown_row
union all
select * from base
