-- models/fact_encounter_diagnosis_bridge.sql

WITH dedup AS (
    SELECT DISTINCT
        encounter_id,
        icd10_code
    FROM {{ ref('stg_diagnoses') }}
)

SELECT
    d.encounter_id,
    diag.diagnosis_id
FROM dedup d
JOIN {{ ref('dim_diagnoses') }} diag
    ON d.icd10_code = diag.icd10_code
