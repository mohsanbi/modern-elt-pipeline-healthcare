-- models/stg_encounters.sql
SELECT
    CAST(EncounterID AS STRING) AS encounter_id,
    CAST(PatientID AS STRING) AS patient_id,
    CAST(DepartmentID AS STRING) AS department_id,
    CAST(PayerID AS STRING) AS payer_id,
    SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M', EncounterDate) AS encounter_date,
    CAST(EncounterType AS STRING) AS encounter_type,
    CAST(AdmittingPhysicianID AS STRING) AS admitting_physician_id,
    SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M', DischargeDate) AS discharge_date
FROM
    {{ source('healthcare_data_raw', 'encounters') }}