{{ config(
    materialized='view',
    schema='staging',
    tags=['staging', 'patient']
) }}

with source as (
    select
        pat_id,
        f_name,
        l_name,
        m_name,
        gen_cd,
        gen_des,
        dob,
        con_no,
        em_add,
        ec_f_name,
        ec_l_name,
        ec_con_no,
        b_type,
        mar_st,
        nat_id_n,
        nat_id_e,
        current_timestamp as dw_load_date
    from {{ source('healthcare_raw', 'stg_ehp__patn') }}
)

select
    pat_id,
    f_name,
    l_name,
    m_name,
    case when gen_cd = 'M' then 'Male' else 'Female' end as gender,
    gen_cd as gender_code,
    gen_des as gender_description,
    dob,
    con_no as contact_number,
    em_add as email_address,
    ec_f_name as emergency_contact_first_name,
    ec_l_name as emergency_contact_last_name,
    ec_con_no as emergency_contact_number,
    b_type as blood_type,
    mar_st as marital_status,
    nat_id_n as national_id_number,
    nat_id_e as national_id_expiry,
    dw_load_date
from source
