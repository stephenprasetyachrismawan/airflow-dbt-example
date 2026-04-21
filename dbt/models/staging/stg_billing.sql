{{ config(
    materialized='view',
    schema='staging',
    tags=['staging', 'billing']
) }}

with regular_billing as (
    select
        refr_no,
        pat_id,
        bill_date,
        bill_amt,
        bstat_cd,
        bstat_des,
        btype_cd,
        'Regular' as billing_source,
        current_timestamp as dw_load_date
    from {{ source('healthcare_raw', 'stg_ehp__bill') }}
),

medical_billing as (
    select
        refr_no,
        pat_id,
        bill_date,
        bill_amt,
        bstat_cd,
        bstat_des,
        btype_cd,
        'Medical' as billing_source,
        current_timestamp as dw_load_date
    from {{ source('healthcare_raw', 'stg_ehp__mdbl') }}
),

combined_billing as (
    select * from regular_billing
    union all
    select * from medical_billing
)

select
    refr_no,
    pat_id,
    bill_date as billing_date,
    bill_amt as billing_amount,
    bstat_cd as billing_status_code,
    bstat_des as billing_status_description,
    btype_cd as billing_type_code,
    billing_source,
    dw_load_date
from combined_billing
