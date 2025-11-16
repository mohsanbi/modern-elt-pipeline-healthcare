-- models/intermediate/int_patient_encounters.sql
SELECT
    p.patient_id,
    p.first_name,
    p.last_name,
    p.date_of_birth,
    p.gender,
    p.address,
    p.contact_number,
    p.registration_date,
    e.encounter_id,
    e.department_id,
    e.payer_id,
    e.encounter_date,
    e.encounter_type,
    e.admitting_physician_id,
    e.discharge_date,
    DATE_DIFF(e.discharge_date, e.encounter_date, DAY) AS length_of_stay_days
FROM
    {{ ref('stg_patients') }} AS p
JOIN
    {{ ref('stg_encounters') }} AS e
    ON p.patient_id = e.patient_id
WHERE
    e.encounter_date IS NOT NULL