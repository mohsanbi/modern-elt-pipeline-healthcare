{{ config(materialized='table') }}

-- Generate a date dimension covering a wider range
{{ dbt_date.get_date_dimension("2000-01-01", "2035-12-31") }}