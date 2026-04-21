{{ config(
    materialized='view',
    schema='staging',
    tags=['staging', 'doctor']
) }}

with source as (
    select
        stf_id,
        f_name,
        l_name,
        gen_cd,
        gen_des,
        dt_brt,
        dep_id,
        role_cd,
        role_des,
        con_no,
        em_add,
        stat_cd,
        stat_des,
        hire_date,
        role_date,
        sal_yr,
        sal_mn,
        current_timestamp as dw_load_date
    from {{ source('healthcare_raw', 'stg_ehp__stff') }}
    where role_cd = 'DOC'  -- Filter only doctors
)

select
    stf_id as doctor_id,
    f_name as first_name,
    l_name as last_name,
    gen_cd as gender_code,
    gen_des as gender_description,
    dt_brt as date_of_birth,
    dep_id as department_id,
    role_cd as role_code,
    role_des as role_description,
    con_no as contact_number,
    em_add as email_address,
    stat_cd as status_code,
    stat_des as status_description,
    hire_date,
    role_date,
    sal_yr as annual_salary,
    sal_mn as monthly_salary,
    dw_load_date
from source
