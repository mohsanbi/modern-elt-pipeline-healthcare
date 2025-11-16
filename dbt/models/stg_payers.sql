-- models/stg_payers.sql
SELECT
    CAST(PayerID AS STRING) AS payer_id,
    CAST(PayerName AS STRING) AS payer_name,
    CAST(PayerType AS STRING) AS payer_type
FROM
    {{ source('healthcare_data_raw', 'payers') }}