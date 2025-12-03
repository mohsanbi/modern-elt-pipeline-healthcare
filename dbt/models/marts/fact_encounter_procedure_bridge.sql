SELECT
    e.encounter_id,
    p.procedure_id
FROM {{ ref('stg_procedures') }} s
JOIN {{ ref('dim_procedures') }} p
    ON s.cpt_code = p.cpt_code
JOIN {{ ref('stg_encounters') }} e
    ON s.encounter_id = e.encounter_id