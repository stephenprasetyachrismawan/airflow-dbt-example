{{ config(materialized='table', schema='marts') }}

-- SCD Type 2: hanya dokter (ROLE_DES = 'Doctor')
-- Initial load: satu baris per dokter dengan dw_is_current = TRUE
with unknown_row as (
    select
        -1              as dokter_key,
        'UNKNOWN'       as stf_id,
        'Unknown'       as nama_lengkap,
        'Unknown'       as nama_depan,
        'Unknown'       as nama_belakang,
        null::varchar   as jenis_kelamin,
        null::date      as tanggal_lahir,
        null::varchar   as departemen_id,
        'Unknown'       as nama_departemen,
        null::varchar   as kode_peran,
        'Doctor'        as deskripsi_peran,
        null::varchar   as status_karyawan,
        null::date      as tanggal_masuk,
        null::date      as tanggal_peran,
        null::decimal   as gaji_tahunan,
        null::varchar   as nomor_kontak,
        null::varchar   as email,
        '2020-01-01'::date as dw_valid_from,
        null::date      as dw_valid_to,
        true            as dw_is_current,
        current_timestamp as dw_load_date
),

base as (
    select
        row_number() over (order by s.stf_id) as dokter_key,
        s.stf_id,
        s.nama_lengkap,
        s.nama_depan,
        s.nama_belakang,
        s.jenis_kelamin,
        s.tanggal_lahir,
        s.dep_id            as departemen_id,
        coalesce(d.DEP_NAME, 'Unknown') as nama_departemen,
        s.kode_peran,
        s.deskripsi_peran,
        s.status_karyawan,
        s.tanggal_masuk,
        s.tanggal_peran,
        s.gaji_tahunan,
        s.nomor_kontak,
        s.email,
        '2020-01-01'::date  as dw_valid_from,
        null::date          as dw_valid_to,
        true                as dw_is_current,
        current_timestamp   as dw_load_date
    from {{ ref('stg_staff_dokter') }} s
    left join {{ source('raw_kaggle', 'stg_ehp__dpmt') }} d
        on s.dep_id = d.DEP_ID
)

select * from unknown_row
union all
select * from base
