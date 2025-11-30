SELECT
    payer_id,
    payer_name,
    payer_type
FROM
    {{ ref('stg_payers') }}