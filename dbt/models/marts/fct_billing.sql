{{ config(
    materialized='table',
    schema='marts',
    tags=['marts', 'fact', 'billing']
) }}

with billing_enriched as (
    select * from {{ ref('int_billing_enriched') }}
),

dim_patient as (
    select patient_key, pat_id from {{ ref('dim_patient') }}
),

dim_date as (
    select date_key, calendar_date from {{ ref('dim_date') }}
),

fact_data as (
    select
        row_number() over (order by be.refr_no) as billing_key,
        be.refr_no as billing_id,
        coalesce(dp.patient_key, -1) as patient_key,
        coalesce(dd.date_key, -1) as billing_date_key,
        be.billing_date,
        be.billing_amount,
        be.payment_amount,
        be.outstanding_amount,
        be.billing_status_code,
        be.billing_status_description,
        be.billing_type_code,
        be.billing_source,
        be.payment_date,
        be.payment_method_code,
        be.payment_method_description,
        be.payment_status_code,
        be.payment_status_description,
        be.payment_status,
        be.patient_first_name,
        be.patient_last_name,
        be.dw_load_date
    from billing_enriched be
    left join dim_patient dp on be.pat_id = dp.pat_id
    left join dim_date dd on cast(be.billing_date as date) = dd.calendar_date
)

select * from fact_data
