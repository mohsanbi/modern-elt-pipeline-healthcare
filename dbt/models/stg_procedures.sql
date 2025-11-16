-- models/stg_procedures.sql
SELECT
    CAST(ProcedureID AS STRING) AS procedure_id,
    CAST(EncounterID AS STRING) AS encounter_id,
    SAFE_CAST(CPT_Code AS INT64) AS cpt_code,
    CAST(ProcedureDescription AS STRING) AS procedure_description
FROM
    {{ source('healthcare_data_raw', 'procedures') }}