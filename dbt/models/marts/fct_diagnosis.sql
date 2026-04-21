{{ config(materialized='table', schema='marts') }}

with diagnosis as (
    select * from {{ ref('stg_diagnosis') }}
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
    row_number() over (order by d.refr_no, d.urutan_diagnosis)  as diagnosis_key,
    coalesce(dp.pasien_key, -1)                                 as pasien_key,
    coalesce(dd.dokter_key, -1)                                 as dokter_key,
    -- ROM_ID dari diagnosis (jika ada), fallback ke ROM_ID kunjungan
    coalesce(dr_diag.ruangan_key, dr_visit.ruangan_key, -1)     as ruangan_key,
    coalesce(ddpt_diag.departemen_key, ddpt_visit.departemen_key, -1) as departemen_key,
    coalesce(dt.tanggal_key, -1)                                as tanggal_diagnosis_key,
    -- Degenerate dimensions
    d.refr_no,
    d.urutan_diagnosis,
    d.kode_diagnosis,
    d.deskripsi_diagnosis,
    d.kode_status,
    d.deskripsi_status,
    d.kode_tipe,
    d.deskripsi_tipe,
    -- Measures
    d.total_biaya                                               as total_biaya_diagnosis,
    1                                                           as jumlah_diagnosis,
    d.waktu_diagnosis,
    case when d.kode_tipe = '0' then true else false end        as is_diagnosis_utama,
    case when d.deskripsi_status like '%Confirmed%'
         or d.deskripsi_status = 'Recurrent' then true else false end as is_confirmed,
    current_timestamp                                           as dw_load_date
from diagnosis d
left join kunjungan k
    on d.refr_no = k.refr_no
left join dim_pasien dp
    on k.pat_id = dp.pat_id
left join dim_dokter dd
    on k.stf_id_dokter = dd.stf_id
-- Join ruangan dari diagnosis
left join dim_ruangan dr_diag
    on d.rom_id = dr_diag.rom_id
left join dim_departemen ddpt_diag
    on dr_diag.departemen_id = ddpt_diag.dep_id
-- Fallback: ruangan dari kunjungan
left join dim_ruangan dr_visit
    on k.rom_id = dr_visit.rom_id
left join dim_departemen ddpt_visit
    on dr_visit.departemen_id = ddpt_visit.dep_id
left join dim_tanggal dt
    on d.tanggal_diagnosis = dt.tanggal
