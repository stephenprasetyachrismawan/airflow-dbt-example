{{ config(
    materialized='table',
    schema='marts',
    tags=['marts', 'dimension', 'patient']
) }}

with patient as (
    select * from {{ ref('stg_patient') }}
),

enriched as (
    select
        row_number() over (order by pat_id) as patient_key,
        pat_id,
        f_name,
        l_name,
        m_name,
        concat(f_name, ' ', l_name) as full_name,
        gender,
        dob,
        extract(year from dob)::int as birth_year,
        extract(year from current_date()) - extract(year from dob)::int as current_age,
        blood_type,
        marital_status,
        contact_number,
        email_address,
        national_id_number,
        national_id_expiry,
        emergency_contact_first_name,
        emergency_contact_last_name,
        emergency_contact_number,
        dw_load_date
    from patient
)

select * from enriched
