{{ config(
    materialized='table',
    schema='marts',
    tags=['marts', 'fact', 'visit']
) }}

with visit_enriched as (
    select * from {{ ref('int_visit_enriched') }}
),

dim_patient as (
    select patient_key, pat_id from {{ ref('dim_patient') }}
),

dim_doctor as (
    select doctor_key, doctor_id from {{ ref('dim_doctor') }}
),

dim_room as (
    select room_key, room_id from {{ ref('dim_room') }}
),

dim_department as (
    select department_key, department_id from {{ ref('dim_department') }}
),

dim_date as (
    select date_key, calendar_date from {{ ref('dim_date') }}
),

fact_data as (
    select
        row_number() over (order by ve.refr_no) as visit_key,
        ve.refr_no as visit_id,
        coalesce(dp.patient_key, -1) as patient_key,
        coalesce(dr.doctor_key, -1) as doctor_key,
        coalesce(drm.room_key, -1) as room_key,
        coalesce(dpt.department_key, -1) as department_key,
        coalesce(dd.date_key, -1) as visit_date_key,
        ve.visit_start_time,
        ve.visit_end_time,
        ve.length_of_stay_hours,
        ve.is_inpatient,
        ve.visit_status_code,
        ve.visit_status_description,
        ve.visit_type_code,
        ve.visit_type_description,
        ve.patient_first_name,
        ve.patient_last_name,
        ve.gender as patient_gender,
        ve.patient_dob,
        ve.blood_type,
        ve.marital_status,
        ve.room_type_code,
        ve.room_type_description,
        ve.room_capacity,
        ve.room_purpose_code,
        ve.department_name,
        ve.department_location,
        ve.dw_load_date
    from visit_enriched ve
    left join dim_patient dp on ve.pat_id = dp.pat_id
    left join dim_doctor dr on ve.medt_id = dr.doctor_id
    left join dim_room drm on ve.rom_id = drm.room_id
    left join dim_department dpt on ve.department_id = dpt.department_id
    left join dim_date dd on cast(ve.visit_start_time as date) = dd.calendar_date
)

select * from fact_data
