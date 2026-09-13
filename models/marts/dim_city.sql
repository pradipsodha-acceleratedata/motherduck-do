-- One row per city.
--
-- Population, coordinates and region are re-stated by WHO each year and drift
-- between publications, so the city's attributes are taken from its most recent
-- year rather than averaged or arbitrarily picked.

with measurements as (

    select * from {{ ref('int_city_year_pollutants') }}

),

ranked as (

    select
        *,
        row_number() over (
            partition by city_key
            order by measurement_year desc
        ) as recency_rank
    from measurements

),

latest as (

    select
        city_key,
        country_code,
        country_name,
        city_name,
        who_region,
        population        as latest_population,
        latitude,
        longitude,
        measurement_year  as latest_measurement_year
    from ranked
    where recency_rank = 1

),

history as (

    select
        city_key,
        min(measurement_year) as first_measurement_year,
        count(*)              as measured_years
    from measurements
    group by city_key

)

select
    latest.*,
    history.first_measurement_year,
    history.measured_years
from latest
left join history using (city_key)
