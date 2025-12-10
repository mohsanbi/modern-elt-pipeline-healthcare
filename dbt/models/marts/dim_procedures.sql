-- models/marts/dim_procedures.sql

WITH dedup AS (
    SELECT
        cpt_code,
        ANY_VALUE(procedure_description) AS procedure_description
    FROM {{ ref('stg_procedures') }}
    GROUP BY cpt_code
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['cpt_code']) }} AS procedure_id,
    cpt_code,
    procedure_description
FROM dedup
