-- One row per country, city and year, with the three pollutants side by side
-- and a coverage-derived confidence flag.
--
-- The source is already unique on that grain, so this model adds meaning
-- rather than collapsing rows: it carries the surrogate key the marts join on
-- and states how much of the year each measurement actually covers.

with staged as (

    select * from {{ ref('stg_air_quality') }}

),

keyed as (

    select
        md5(concat_ws('|', country_code, city_name))    as city_key,
        country_code,
        country_name,
        city_name,
        who_region,
        measurement_year,
        station_type,
        pm25_ugm3,
        pm10_ugm3,
        no2_ugm3,
        pm25_coverage_pct,
        pm10_coverage_pct,
        no2_coverage_pct,
        population,
        latitude,
        longitude,

        -- A pollutant measured over a short window is not comparable with one
        -- measured across the year; 75% is the WHO reporting convention.
        coalesce(pm25_coverage_pct, 0) >= 75                   as pm25_is_annual,
        coalesce(pm10_coverage_pct, 0) >= 75                   as pm10_is_annual,
        coalesce(no2_coverage_pct, 0)  >= 75                   as no2_is_annual,

        (pm25_ugm3 is not null)::int
            + (pm10_ugm3 is not null)::int
            + (no2_ugm3 is not null)::int                      as pollutants_reported

    from staged

)

select * from keyed
