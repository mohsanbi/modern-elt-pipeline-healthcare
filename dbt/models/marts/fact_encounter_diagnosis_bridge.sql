SELECT
    e.encounter_id,
    d.diagnosis_id
FROM {{ ref('stg_diagnoses') }} d
JOIN {{ ref('dim_diagnoses') }} diag
    ON d.icd10_code = diag.icd10_code
JOIN {{ ref('stg_encounters') }} e
    ON d.encounter_id = e.encounter_id