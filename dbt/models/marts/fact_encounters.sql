SELECT
    -- Keys
    encounter_id,
    patient_id,
    department_id,
    payer_id,
    provider_id,
    encounter_date_id, 
    -- Timestamps
    encounter_date,
    discharge_date,

    -- Degenerate Dimensions
    encounter_type,
    all_diagnoses,
    all_procedures,

    -- Measures
    length_of_stay_days,
    COALESCE(number_of_procedures, 0) AS number_of_procedures

FROM
    {{ ref('int_encounters_enriched') }}