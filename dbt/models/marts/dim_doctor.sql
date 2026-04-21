{{ config(
    materialized='table',
    schema='marts',
    tags=['marts', 'dimension', 'doctor']
) }}

with doctor as (
    select * from {{ ref('stg_doctor') }}
),

enriched as (
    select
        row_number() over (order by doctor_id) as doctor_key,
        doctor_id,
        first_name,
        last_name,
        concat(first_name, ' ', last_name) as full_name,
        gender_code,
        gender_description,
        date_of_birth,
        extract(year from date_of_birth)::int as birth_year,
        extract(year from current_date()) - extract(year from date_of_birth)::int as current_age,
        department_id,
        role_code,
        role_description,
        contact_number,
        email_address,
        status_code,
        status_description,
        hire_date,
        role_date,
        annual_salary,
        monthly_salary,
        dw_load_date
    from doctor
)

select * from enriched
