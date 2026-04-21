{{ config(
    materialized='view',
    schema='intermediate',
    tags=['intermediate', 'billing']
) }}

with billing as (
    select * from {{ ref('stg_billing') }}
),

payment as (
    select * from {{ ref('stg_payment') }}
),

patient as (
    select * from {{ ref('stg_patient') }}
),

combined as (
    select
        b.refr_no,
        b.pat_id,
        p.f_name as patient_first_name,
        p.l_name as patient_last_name,
        b.billing_date,
        b.billing_amount,
        b.billing_status_code,
        b.billing_status_description,
        b.billing_type_code,
        b.billing_source,
        coalesce(pay.payment_amount, 0) as payment_amount,
        pay.payment_date,
        pay.payment_method_code,
        pay.payment_method_description,
        pay.payment_status_code,
        pay.payment_status_description,
        (b.billing_amount - coalesce(pay.payment_amount, 0)) as outstanding_amount,
        case
            when (b.billing_amount - coalesce(pay.payment_amount, 0)) <= 0 then 'Paid'
            when (b.billing_amount - coalesce(pay.payment_amount, 0)) < b.billing_amount then 'Partially Paid'
            else 'Outstanding'
        end as payment_status,
        b.dw_load_date
    from billing b
    left join patient p on b.pat_id = p.pat_id
    left join payment pay on b.refr_no = pay.refr_no
)

select * from combined
