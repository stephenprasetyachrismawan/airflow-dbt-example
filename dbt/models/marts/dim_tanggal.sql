{{ config(materialized='table', schema='marts') }}

with calendar as (
    select unnest(
        generate_series(
            '2020-01-01'::timestamp,
            '2027-12-31'::timestamp,
            interval '1 day'
        )
    )::date as tanggal
),

unknown_row as (
    select
        -1                      as tanggal_key,
        null::date              as tanggal,
        null::integer           as hari,
        null::integer           as bulan,
        null::integer           as tahun,
        null::integer           as kuartal,
        null::integer           as minggu,
        'Unknown'::varchar      as nama_hari,
        'Unknown'::varchar      as nama_bulan,
        null::boolean           as is_weekend,
        null::boolean           as is_weekday,
        null::integer           as semester,
        null::integer           as hari_dalam_tahun
),

enriched as (
    select
        row_number() over (order by tanggal)    as tanggal_key,
        tanggal,
        extract(day from tanggal)::integer      as hari,
        extract(month from tanggal)::integer    as bulan,
        extract(year from tanggal)::integer     as tahun,
        extract(quarter from tanggal)::integer  as kuartal,
        extract(week from tanggal)::integer     as minggu,
        -- Nama hari dalam Bahasa Indonesia (dayofweek: 0=Sun,1=Mon,...,6=Sat)
        case extract(dayofweek from tanggal)::integer
            when 0 then 'Minggu'
            when 1 then 'Senin'
            when 2 then 'Selasa'
            when 3 then 'Rabu'
            when 4 then 'Kamis'
            when 5 then 'Jumat'
            when 6 then 'Sabtu'
        end                                     as nama_hari,
        -- Nama bulan dalam Bahasa Indonesia
        case extract(month from tanggal)::integer
            when 1  then 'Januari'
            when 2  then 'Februari'
            when 3  then 'Maret'
            when 4  then 'April'
            when 5  then 'Mei'
            when 6  then 'Juni'
            when 7  then 'Juli'
            when 8  then 'Agustus'
            when 9  then 'September'
            when 10 then 'Oktober'
            when 11 then 'November'
            when 12 then 'Desember'
        end                                     as nama_bulan,
        extract(dayofweek from tanggal)::integer in (0, 6) as is_weekend,
        extract(dayofweek from tanggal)::integer not in (0, 6) as is_weekday,
        case when extract(month from tanggal)::integer <= 6 then 1 else 2 end as semester,
        extract(dayofyear from tanggal)::integer as hari_dalam_tahun
    from calendar
)

select * from unknown_row
union all
select * from enriched
