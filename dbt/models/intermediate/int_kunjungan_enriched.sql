{{ config(materialized='view', schema='intermediate') }}

-- Join kunjungan dengan appointment via PAT_ID + tanggal (REFR_NO berbeda format)
with appt_candidates as (
    select
        v.refr_no,
        a.appt_refr_no,
        a.kode_status         as kode_status_appt,
        a.deskripsi_status    as deskripsi_status_appt,
        a.kode_tipe           as kode_tipe_appt,
        a.deskripsi_tipe      as deskripsi_tipe_appt,
        a.is_completed        as is_appointment_completed,
        row_number() over (
            partition by v.refr_no
            order by abs(epoch(v.waktu_masuk) - epoch(a.waktu_appointment))
        ) as rn
    from {{ ref('int_dokter_per_kunjungan') }} v
    join {{ ref('stg_appointment') }} a
        on  v.pat_id = a.pat_id
        and a.tanggal_appointment between
            (v.tanggal_masuk - interval '30 days') and v.tanggal_masuk
),

appt_best as (
    select * from appt_candidates where rn = 1
)

select
    v.refr_no,
    v.pat_id,
    v.medt_id,
    v.stf_id_dokter,
    v.waktu_masuk,
    v.waktu_keluar,
    v.tanggal_masuk,
    v.tanggal_keluar,
    v.jam_masuk,
    v.jam_keluar,
    v.kode_status,
    v.deskripsi_status,
    v.kode_tipe,
    v.deskripsi_tipe,
    v.rom_id,
    v.is_rawat_inap,
    case when v.waktu_keluar is not null
         then round(
             cast(epoch(v.waktu_keluar) - epoch(v.waktu_masuk) as decimal) / 3600.0,
             2
         )
    end                                                             as durasi_kunjungan_jam,
    a.appt_refr_no,
    a.kode_status_appt,
    a.deskripsi_status_appt,
    a.kode_tipe_appt,
    a.deskripsi_tipe_appt,
    case when a.appt_refr_no is not null then true else false end   as is_dari_appointment,
    coalesce(a.is_appointment_completed, false)                     as is_appointment_converted
from {{ ref('int_dokter_per_kunjungan') }} v
left join appt_best a on v.refr_no = a.refr_no
