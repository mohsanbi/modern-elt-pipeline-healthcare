WITH encounters_with_keys AS (

    SELECT
        enc.encounter_id,
        enc.patient_id,
        enc.department_id,
        enc.payer_id,
        enc.admitting_physician_id AS provider_id,
        enc.encounter_date,
        CAST(enc.encounter_date AS DATE) AS encounter_date_id,
        enc.discharge_date,
        enc.encounter_type,

        diag.all_diagnoses,
        proc.all_procedures,
        proc.number_of_procedures

    FROM {{ ref('stg_encounters') }} AS enc

    LEFT JOIN (
        SELECT
            encounter_id,
            STRING_AGG(diagnosis_description, '; ') AS all_diagnoses
        FROM {{ ref('stg_diagnoses') }}
        GROUP BY 1
    ) AS diag
        ON enc.encounter_id = diag.encounter_id

    LEFT JOIN (
        SELECT
            encounter_id,
            STRING_AGG(procedure_description, '; ') AS all_procedures,
            COUNT(procedure_id) AS number_of_procedures
        FROM {{ ref('stg_procedures') }}
        GROUP BY 1
    ) AS proc
        ON enc.encounter_id = proc.encounter_id
)

SELECT
    f.*,
    DATE_DIFF(f.discharge_date, f.encounter_date, DAY) AS length_of_stay_days

FROM encounters_with_keys AS f

-- Enforce all dimension foreign keys for dbt tests
JOIN {{ ref('dim_patients') }}     AS p ON f.patient_id      = p.patient_id
JOIN {{ ref('dim_departments') }} AS d ON f.department_id   = d.department_id
LEFT JOIN {{ ref('dim_providers') }} AS pr ON f.provider_id = pr.provider_id
JOIN {{ ref('dim_payers') }}      AS py ON f.payer_id       = py.payer_id
JOIN {{ ref('dim_date') }}        AS dt ON f.encounter_date_id = dt.date_day
