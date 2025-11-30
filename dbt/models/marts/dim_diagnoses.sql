SELECT DISTINCT
    diagnosis_id,
    icd10_code,
    diagnosis_description
FROM
    {{ ref('stg_diagnoses') }}