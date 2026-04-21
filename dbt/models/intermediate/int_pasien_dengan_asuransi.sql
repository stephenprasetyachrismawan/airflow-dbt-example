{{ config(materialized='view', schema='intermediate') }}

-- Ambil satu asuransi per pasien: prioritas AKTIF, lalu terbaru berdasarkan tanggal_selesai
with ranked_insr as (
    select
        pat_id,
        nama_asuransi,
        jenis_penjamin,
        tanggal_mulai,
        tanggal_selesai,
        is_aktif,
        row_number() over (
            partition by pat_id
            order by is_aktif desc, tanggal_selesai desc nulls last
        ) as rn
    from {{ ref('stg_asuransi') }}
)

select
    p.pat_id,
    p.nama_depan,
    p.nama_tengah,
    p.nama_belakang,
    p.nama_lengkap,
    p.jenis_kelamin,
    p.tanggal_lahir,
    p.golongan_darah,
    p.status_pernikahan,
    p.nomor_kontak,
    p.email,
    i.jenis_penjamin,
    i.nama_asuransi,
    i.is_aktif        as asuransi_aktif
from {{ ref('stg_pasien') }} p
left join ranked_insr i
    on p.pat_id = i.pat_id and i.rn = 1
