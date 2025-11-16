-- models/marts/fact_claims.sql
SELECT
    c.claim_id,
    c.claim_date,
    c.service_amount,
    c.amount_paid,
    c.patient_id,
    c.procedure_id,
    p.payer_name,
    d.diagnosis_description
FROM
    {{ ref('stg_claims') }} AS c
LEFT JOIN
    {{ ref('stg_payers') }} AS p
    ON c.patient_id = p.payer_id -- Assuming patient_id and payer_id link in claims in some way for demo, adjust if actual relation is different
LEFT JOIN
    {{ ref('stg_diagnoses') }} AS d
    ON c.patient_id = d.encounter_id -- Again, simplified join for demo, adjust as per schema
WHERE
    c.amount_paid > 0