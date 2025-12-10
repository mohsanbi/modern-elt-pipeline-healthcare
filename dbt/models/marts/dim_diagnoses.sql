-- models/marts/dim_diagnoses.sql

WITH dedup AS (
    SELECT
        icd10_code,
        ANY_VALUE(diagnosis_description) AS diagnosis_description
    FROM {{ ref('stg_diagnoses') }}
    GROUP BY icd10_code
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['icd10_code']) }} AS diagnosis_id,
    icd10_code,
    diagnosis_description
FROM dedup
