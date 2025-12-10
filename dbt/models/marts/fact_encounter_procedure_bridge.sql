-- models/fact_encounter_procedure_bridge.sql

WITH dedup AS (
    SELECT DISTINCT
        encounter_id,
        cpt_code
    FROM {{ ref('stg_procedures') }}
)

SELECT
    d.encounter_id,
    p.procedure_id
FROM dedup d
JOIN {{ ref('dim_procedures') }} p
    ON d.cpt_code = p.cpt_code
