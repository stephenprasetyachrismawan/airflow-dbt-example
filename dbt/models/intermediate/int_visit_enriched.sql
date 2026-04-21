{{ config(
    materialized='view',
    schema='intermediate',
    tags=['intermediate', 'visit']
) }}

with visit as (
    select * from {{ ref('stg_visit') }}
),

patient as (
    select * from {{ ref('stg_patient') }}
),

room as (
    select * from {{ ref('stg_room') }}
),

department as (
    select * from {{ ref('stg_department') }}
),

joined as (
    select
        v.refr_no,
        v.pat_id,
        p.f_name as patient_first_name,
        p.l_name as patient_last_name,
        p.gender,
        p.dob as patient_dob,
        p.blood_type,
        p.marital_status,
        v.medt_id,
        v.visit_start_time,
        v.visit_end_time,
        v.length_of_stay_hours,
        v.is_inpatient,
        v.visit_status_code,
        v.visit_status_description,
        v.visit_type_code,
        v.visit_type_description,
        v.rom_id,
        r.room_id,
        r.room_type_code,
        r.room_type_description,
        r.room_capacity,
        r.room_purpose_code,
        r.room_purpose_description,
        r.department_id,
        d.department_name,
        d.department_location,
        v.dw_load_date
    from visit v
    left join patient p on v.pat_id = p.pat_id
    left join room r on v.rom_id = r.room_id
    left join department d on r.department_id = d.department_id
)

select * from joined
