-- models/stg_patients.sql
SELECT
    CAST(PatientID AS STRING) AS patient_id,
    CAST(FirstName AS STRING) AS first_name,
    CAST(LastName AS STRING) AS last_name,
    CAST(DateOfBirth AS STRING) AS date_of_birth, 
    CAST(Gender AS STRING) AS gender,
    CAST(Address AS STRING) AS address,
    CAST(ContactNumber AS STRING) AS contact_number,
    PARSE_DATE('%Y-%m-%d', RegistrationDate) AS registration_date
FROM
    {{ source('healthcare_data_raw', 'patients') }}