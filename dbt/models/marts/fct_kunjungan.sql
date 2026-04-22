{{ config(materialized='table', schema='marts') }}

with kunjungan as (
    select * from {{ ref('int_kunjungan_enriched') }}
),

dim_pasien as (
    select pasien_key, pat_id from {{ ref('dim_pasien') }}
    where dw_is_current = true and pat_id != 'UNKNOWN'
),

dim_dokter as (
    select dokter_key, stf_id from {{ ref('dim_dokter') }}
    where dw_is_current = true and stf_id != 'UNKNOWN'
),

dim_ruangan as (
    select ruangan_key, rom_id, departemen_id from {{ ref('dim_ruangan') }}
    where rom_id != 'UNKNOWN'
),

dim_departemen as (
    select departemen_key, dep_id from {{ ref('dim_departemen') }}
    where dep_id != 'UNKNOWN'
),

dim_tanggal as (
    select tanggal_key, tanggal from {{ ref('dim_tanggal') }}
    where tanggal is not null
)

select
    row_number() over (order by k.refr_no)      as kunjungan_key,
    coalesce(dp.pasien_key, -1)                 as pasien_key,
    coalesce(dd.dokter_key, -1)                 as dokter_key,
    coalesce(dr.ruangan_key, -1)                as ruangan_key,
    coalesce(ddpt.departemen_key, -1)           as departemen_key,
    coalesce(dt_masuk.tanggal_key, -1)          as tanggal_masuk_key,
    coalesce(dt_keluar.tanggal_key, -1)         as tanggal_keluar_key,
    -- Degenerate dimensions
    k.refr_no,
    k.medt_id,
    k.appt_refr_no,
    k.kode_status                               as kode_status_kunjungan,
    k.deskripsi_status                          as deskripsi_status_kunjungan,
    k.kode_tipe                                 as kode_tipe_kunjungan,
    k.deskripsi_tipe                            as deskripsi_tipe_kunjungan,
    k.kode_status_appt                          as kode_status_appointment,
    k.deskripsi_status_appt                     as deskripsi_status_appointment,
    k.kode_tipe_appt                            as kode_tipe_appointment,
    -- Measures
    k.durasi_kunjungan_jam,
    k.is_rawat_inap,
    k.is_dari_appointment,
    k.is_appointment_converted,
    k.jam_masuk,
    k.jam_keluar,
    k.waktu_masuk,
    k.waktu_keluar,
    current_timestamp                           as dw_load_date
from kunjungan k
left join dim_pasien dp
    on k.pat_id = dp.pat_id
left join dim_dokter dd
    on k.stf_id_dokter = dd.stf_id
left join dim_ruangan dr
    on k.rom_id = dr.rom_id
left join dim_departemen ddpt
    on dr.departemen_id = ddpt.dep_id
left join dim_tanggal dt_masuk
    on k.tanggal_masuk = dt_masuk.tanggal
left join dim_tanggal dt_keluar
    on k.tanggal_keluar = dt_keluar.tanggal
