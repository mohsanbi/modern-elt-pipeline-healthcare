-- models/stg_claims.sql
SELECT
    CAST(ClaimID AS STRING) AS claim_id,
    CAST(ProcedureID AS STRING) AS procedure_id,
    CAST(PatientID AS STRING) AS patient_id,
    PARSE_DATE('%m/%d/%Y', ClaimDate) AS claim_date,
    SAFE_CAST(REPLACE(CAST(ServiceAmount AS STRING), '$', '') AS NUMERIC) AS service_amount, 
    SAFE_CAST(REPLACE(CAST(AmountPaid AS STRING), '$', '') AS NUMERIC) AS amount_paid       
FROM
    {{ source('healthcare_data_raw', 'claims') }}