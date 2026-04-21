{{ config(
    materialized='table',
    schema='marts',
    tags=['marts', 'dimension', 'date']
) }}

with calendar as (
    select unnest(
        generate_series(
            '2020-01-01'::timestamp,
            '2026-12-31'::timestamp,
            '1 day'::interval
        )
    )::date as calendar_date
),

enriched as (
    select
        row_number() over (order by calendar_date) as date_key,
        calendar_date,
        extract(year from calendar_date)::int as year,
        extract(month from calendar_date)::int as month,
        extract(day from calendar_date)::int as day,
        extract(quarter from calendar_date)::int as quarter,
        extract(week from calendar_date)::int as week,
        extract(dayofweek from calendar_date)::int as day_of_week,
        extract(dayofyear from calendar_date)::int as day_of_year,
        strftime(calendar_date, '%B') as month_name,
        strftime(calendar_date, '%A') as day_name,
        case
            when extract(dayofweek from calendar_date) in (6, 7) then true
            else false
        end as is_weekend,
        case
            when extract(month from calendar_date) in (12, 1, 2) then 'Winter'
            when extract(month from calendar_date) in (3, 4, 5) then 'Spring'
            when extract(month from calendar_date) in (6, 7, 8) then 'Summer'
            else 'Fall'
        end as season
    from calendar
)

select * from enriched
