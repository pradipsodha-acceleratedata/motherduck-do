-- One row per city and year: the reported pollutant concentrations, plus how
-- far PM2.5 sits above the WHO annual guideline.
--
-- A city-year with no PM2.5 value is kept rather than filtered: absence of a
-- measurement is itself reportable, and dropping it here would silently shrink
-- country coverage in anything built on top.

with measurements as (

    select * from {{ ref('int_city_year_pollutants') }}

),

scored as (

    select
        city_key,
        measurement_year,
        country_code,
        country_name,
        city_name,
        who_region,
        station_type,
        pm25_ugm3,
        pm10_ugm3,
        no2_ugm3,
        pm25_is_annual,
        pm10_is_annual,
        no2_is_annual,
        pollutants_reported,
        population,

        -- WHO 2021 annual guideline for PM2.5 is 5 ug/m3.
        case
            when pm25_ugm3 is null then null
            else round(pm25_ugm3 / 5.0, 2)
        end as pm25_times_who_guideline,

        case
            when pm25_ugm3 is null then 'not_reported'
            when pm25_ugm3 <= 5  then 'within_guideline'
            when pm25_ugm3 <= 15 then 'interim_target_4'
            when pm25_ugm3 <= 25 then 'interim_target_3'
            else 'above_interim_targets'
        end as pm25_guideline_band

    from measurements

)

select * from scored
