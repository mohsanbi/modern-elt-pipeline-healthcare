-- models/marts/dim_patients.sql
SELECT
    patient_id,
    first_name,
    last_name,
    date_of_birth,
    gender,
    address,
    contact_number,
    registration_date
FROM
    {{ ref('stg_patients') }}