-- Typed, renamed passthrough of the WHO measurement feed.
-- Rows without a city or a year cannot be attributed to a place in time, so
-- they are dropped here rather than carried forward as nulls.

with source as (

    select * from {{ source('who', 'ambient_air_quality') }}

),

renamed as (

    select
        trim(iso3)                  as country_code,
        trim(country_name)          as country_name,
        trim(city)                  as city_name,
        trim(who_region)            as who_region,
        year                        as measurement_year,
        trim(version)               as source_version,
        trim(type_of_stations)      as station_type,
        pm25_concentration          as pm25_ugm3,
        pm10_concentration          as pm10_ugm3,
        no2_concentration           as no2_ugm3,
        pm25_tempcov                as pm25_coverage_pct,
        pm10_tempcov                as pm10_coverage_pct,
        no2_tempcov                 as no2_coverage_pct,
        population                  as population,
        latitude                    as latitude,
        longitude                   as longitude
    from source
    where city is not null
      and year is not null

)

select * from renamed
