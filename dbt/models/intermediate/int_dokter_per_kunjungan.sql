{{ config(materialized='view', schema='intermediate') }}

-- Resolve VIST.MEDT_ID → MEDT.STF_ID (filter ROLE_DES = 'Doctor') → dim_dokter
-- Jika satu MEDT punya >1 dokter, ambil yang pertama (row_number = 1)
with dokter_per_medt as (
    select
        medt_id,
        stf_id,
        row_number() over (partition by medt_id order by stf_id) as rn
    from {{ ref('stg_tim_medis') }}
    where is_dokter = true
)

select
    v.refr_no,
    v.pat_id,
    v.medt_id,
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
    coalesce(dm.stf_id, 'UNKNOWN')  as stf_id_dokter
from {{ ref('stg_kunjungan') }} v
left join dokter_per_medt dm
    on v.medt_id = dm.medt_id and dm.rn = 1
