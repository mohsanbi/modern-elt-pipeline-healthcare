SELECT
    -- Keys
    ice.claim_id,
    ice.encounter_id,
    ice.patient_id,
    ice.payer_id,
    {{ dbt_utils.generate_surrogate_key(['ice.cpt_code']) }} AS procedure_id,

    -- Dates
    ice.claim_date,

    -- Degenerate Dimensions
    ice.procedure_description,

    -- Measures
    ice.service_amount,
    ice.amount_paid

FROM
    {{ ref('int_claims_enriched') }} AS ice

INNER JOIN
    {{ ref('dim_patients') }} AS pat
    ON ice.patient_id = pat.patient_id