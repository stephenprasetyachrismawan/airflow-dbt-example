{{ config(materialized='table', schema='marts') }}

-- SCD Type 2: satu versi per pasien (data historis didapat jika pipeline dijalankan ulang)
-- Untuk initial load: satu baris per pasien dengan dw_is_current = TRUE
with unknown_row as (
    select
        -1              as pasien_key,
        'UNKNOWN'       as pat_id,
        'Unknown'       as nama_lengkap,
        'Unknown'       as nama_depan,
        ''              as nama_tengah,
        'Unknown'       as nama_belakang,
        null::varchar   as jenis_kelamin,
        null::date      as tanggal_lahir,
        null::varchar   as golongan_darah,
        null::varchar   as status_pernikahan,
        null::varchar   as nomor_kontak,
        null::varchar   as email,
        null::varchar   as jenis_penjamin,
        null::varchar   as nama_asuransi,
        '2020-01-01'::date as dw_valid_from,
        null::date      as dw_valid_to,
        true            as dw_is_current,
        current_timestamp as dw_load_date
),

base as (
    select
        row_number() over (order by pat_id) as pasien_key,
        pat_id,
        nama_lengkap,
        nama_depan,
        nama_tengah,
        nama_belakang,
        jenis_kelamin,
        tanggal_lahir,
        golongan_darah,
        status_pernikahan,
        nomor_kontak,
        email,
        jenis_penjamin,
        nama_asuransi,
        '2020-01-01'::date  as dw_valid_from,
        null::date          as dw_valid_to,
        true                as dw_is_current,
        current_timestamp   as dw_load_date
    from {{ ref('int_pasien_dengan_asuransi') }}
)

select * from unknown_row
union all
select * from base
