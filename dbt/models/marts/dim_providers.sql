SELECT
    provider_id,
    CONCAT(provider_first_name, ' ', provider_last_name) AS provider_full_name,
    role,
    specialty,
    hire_date,
    termination_date
FROM
    {{ ref('stg_providers') }}