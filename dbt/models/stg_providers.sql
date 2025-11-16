-- models/stg_providers.sql
SELECT
    CAST(ProviderID AS STRING) AS provider_id,
    CAST(ProviderFirstName AS STRING) AS provider_first_name,
    CAST(ProviderLastName AS STRING) AS provider_last_name,
    CAST(Role AS STRING) AS role,
    CAST(Specialty AS STRING) AS specialty,
    PARSE_DATE('%Y-%m-%d', HireDate) AS hire_date,
    PARSE_DATE('%Y-%m-%d', TerminationDate) AS termination_date
FROM
    {{ source('healthcare_data_raw', 'providers') }}