{{ config(
    materialized='view',
    schema='staging',
    tags=['staging', 'visit']
) }}

with source as (
    select
        refr_no,
        pat_id,
        medt_id,
        vis_en,
        vis_ex,
        vstat_cd,
        vstat_des,
        vtype_cd,
        vtype_des,
        rom_id,
        current_timestamp as dw_load_date
    from {{ source('healthcare_raw', 'stg_ehp__vist') }}
)

select
    refr_no,
    pat_id,
    medt_id,
    vis_en as visit_start_time,
    vis_ex as visit_end_time,
    case
        when vis_ex is not null then datediff('hour', vis_en, vis_ex)
        else null
    end as length_of_stay_hours,
    case when vtype_cd = 'IPT' then true else false end as is_inpatient,
    vstat_cd as visit_status_code,
    vstat_des as visit_status_description,
    vtype_cd as visit_type_code,
    vtype_des as visit_type_description,
    rom_id,
    dw_load_date
from source
