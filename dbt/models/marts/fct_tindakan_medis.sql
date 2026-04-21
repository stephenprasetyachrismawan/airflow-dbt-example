{{ config(materialized='table', schema='marts') }}

with tindakan as (
    select * from {{ ref('stg_tindakan_medis') }}
),

kunjungan as (
    select
        refr_no,
        pat_id,
        stf_id_dokter,
        rom_id
    from {{ ref('int_dokter_per_kunjungan') }}
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
    row_number() over (order by t.refr_no, t.urutan_tindakan)  as tindakan_key,
    coalesce(dp.pasien_key, -1)                                 as pasien_key,
    coalesce(dd.dokter_key, -1)                                 as dokter_key,
    coalesce(dr.ruangan_key, -1)                                as ruangan_key,
    coalesce(ddpt.departemen_key, -1)                           as departemen_key,
    coalesce(dt.tanggal_key, -1)                                as tanggal_tindakan_key,
    -- Degenerate dimensions
    t.refr_no,
    t.urutan_tindakan,
    t.kode_tindakan,
    t.kode_tipe                                                 as kode_tipe_tindakan,
    t.deskripsi_tipe                                            as deskripsi_tipe_tindakan,
    t.kode_status                                               as kode_status_tindakan,
    t.deskripsi_status                                          as deskripsi_status_tindakan,
    t.deskripsi_tindakan,
    -- Measures
    t.total_biaya                                               as total_biaya_tindakan,
    1                                                           as jumlah_tindakan,
    t.waktu_tindakan,
    t.is_selesai,
    t.is_operasi,
    t.is_obat,
    current_timestamp                                           as dw_load_date
from tindakan t
left join kunjungan k
    on t.refr_no = k.refr_no
left join dim_pasien dp
    on k.pat_id = dp.pat_id
left join dim_dokter dd
    on k.stf_id_dokter = dd.stf_id
left join dim_ruangan dr
    on k.rom_id = dr.rom_id
left join dim_departemen ddpt
    on dr.departemen_id = ddpt.dep_id
left join dim_tanggal dt
    on t.tanggal_tindakan = dt.tanggal
