{{ config(materialized='table', schema='marts') }}

with unknown_row as (
    select
        -1              as ruangan_key,
        'UNKNOWN'       as rom_id,
        'UNKNOWN'       as departemen_id,
        'Unknown'       as nama_departemen,
        null::varchar   as kode_tujuan,
        null::varchar   as deskripsi_tujuan,
        null::varchar   as kode_tipe,
        null::varchar   as deskripsi_tipe,
        null::integer   as kapasitas,
        current_timestamp as dw_load_date,
        current_timestamp as dw_updated_date
),

base as (
    select
        row_number() over (order by rom_id) as ruangan_key,
        rom_id,
        dep_id              as departemen_id,
        nama_departemen,
        kode_tujuan,
        deskripsi_tujuan,
        kode_tipe,
        deskripsi_tipe,
        kapasitas,
        dw_load_date,
        dw_load_date        as dw_updated_date
    from {{ ref('stg_ruangan') }}
)

select * from unknown_row
union all
select * from base
