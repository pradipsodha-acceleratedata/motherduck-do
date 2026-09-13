# motherduck-do

dbt project for the `prd` MotherDuck database.

## Source

`sample_data.who.ambient_air_quality` — WHO ambient air quality measurements,
published as a shared MotherDuck sample database. One row per city, year and
monitoring programme version.

## Models

| Layer | Model | Grain |
| --- | --- | --- |
| staging | `stg_air_quality` | city / year |
| intermediate | `int_city_year_pollutants` | city / year |
| marts | `dim_city` | city |
| marts | `fct_air_quality_yearly` | city / year |

Staging and intermediate materialize as views; marts as tables.

## Running

The CI bundle supplies the profiles. Locally, point `dbt_motherduck_prd` at
`md:prd` with `MOTHERDUCK_TOKEN` in the environment.

```
dbt build --profiles-dir <dir>
```
