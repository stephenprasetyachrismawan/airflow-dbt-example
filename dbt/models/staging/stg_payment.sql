{{ config(
    materialized='view',
    schema='staging',
    tags=['staging', 'payment']
) }}

with source as (
    select
        refr_no,
        pat_id,
        btype_cd,
        tot_pmnt,
        pmnt_tms,
        pmnt_no,
        pmnt_date,
        pmnt_amt,
        meth_cd,
        meth_des,
        pstat_cd,
        pstat_des,
        current_timestamp as dw_load_date
    from {{ source('healthcare_raw', 'stg_ehp__pmnt') }}
)

select
    refr_no,
    pat_id,
    btype_cd as billing_type_code,
    tot_pmnt as total_payment_amount,
    pmnt_tms as payment_times,
    pmnt_no as payment_number,
    pmnt_date as payment_date,
    pmnt_amt as payment_amount,
    meth_cd as payment_method_code,
    meth_des as payment_method_description,
    pstat_cd as payment_status_code,
    pstat_des as payment_status_description,
    dw_load_date
from source
