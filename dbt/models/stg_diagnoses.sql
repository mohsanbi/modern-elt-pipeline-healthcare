-- models/stg_diagnoses.sql
SELECT
    CAST(DiagnosisID AS STRING) AS diagnosis_id,
    CAST(EncounterID AS STRING) AS encounter_id,
    CAST(ICD10_Code AS STRING) AS icd10_code,
    CAST(DiagnosisDescription AS STRING) AS diagnosis_description
FROM
    {{ source('healthcare_data_raw', 'diagnoses') }}