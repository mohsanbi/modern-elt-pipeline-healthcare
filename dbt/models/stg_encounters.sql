-- models/stg_encounters.sql
SELECT
    CAST(EncounterID AS STRING) AS encounter_id,
    CAST(PatientID AS STRING) AS patient_id,
    CAST(DepartmentID AS STRING) AS department_id,
    CAST(PayerID AS STRING) AS payer_id,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', EncounterDate) AS encounter_date, -- Adjust format if needed
    CAST(EncounterType AS STRING) AS encounter_type,
    CAST(AdmittingPhysicianID AS STRING) AS admitting_physician_id,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', DischargeDate) AS discharge_date -- Adjust format if needed
FROM
    {{ source('healthcare_data_raw', 'encounters') }}