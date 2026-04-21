{{ config(
    materialized='view',
    schema='staging',
    tags=['staging', 'medicine']
) }}

with source as (
    select
        med_id,
        med_name,
        type_cd,
        type_des,
        form_cd,
        form_des,
        strn_no,
        strn_un,
        cost_un,
        mstat_cd,
        mstat_des,
        current_timestamp as dw_load_date
    from {{ source('healthcare_raw', 'stg_ehp__mdcn') }}
)

select
    med_id as medicine_id,
    med_name as medicine_name,
    type_cd as medicine_type_code,
    type_des as medicine_type_description,
    form_cd as medicine_form_code,
    form_des as medicine_form_description,
    strn_no as strength_number,
    strn_un as strength_unit,
    cost_un as unit_cost,
    mstat_cd as medicine_status_code,
    mstat_des as medicine_status_description,
    dw_load_date
from source
