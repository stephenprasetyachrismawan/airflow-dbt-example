{{ config(materialized='view', schema='staging') }}

-- REFR_NO di APPT berbeda format dengan VIST (numerik vs W-prefix)
-- Join ke kunjungan dilakukan via PAT_ID + DOC_ID di intermediate layer
select
    cast(REFR_NO as varchar)                as appt_refr_no,
    PAT_ID                                  as pat_id,
    try_cast(APT_TIME as timestamp)         as waktu_appointment,
    try_cast(APT_TIME as date)              as tanggal_appointment,
    CTD_ID                                  as ctd_id,
    DOC_ID                                  as doc_id,
    ASTAT_CD                                as kode_status,
    ASTAT_DES                               as deskripsi_status,
    ATYPE_CD                                as kode_tipe,
    ATYPE_DES                               as deskripsi_tipe,
    case when ASTAT_DES = 'Completed' then true else false end as is_completed
from {{ source('raw_kaggle', 'stg_ehp__appt') }}
where PAT_ID is not null
