SELECT
    -- From stg_claims
    cl.claim_id,
    cl.claim_date,
    cl.patient_id,
    cl.service_amount,
    proc.cpt_code as cpt_code,
    proc.procedure_id,
    cl.amount_paid,

    -- Enriched from other tables
    enc.encounter_id,
    enc.payer_id,
    proc.procedure_description

FROM
    {{ ref('stg_claims') }} AS cl
LEFT JOIN
    {{ ref('stg_procedures') }} AS proc
    ON cl.procedure_id = proc.procedure_id
LEFT JOIN
    {{ ref('stg_encounters') }} AS enc
    ON proc.encounter_id = enc.encounter_id