-- models/stg_departments.sql
SELECT
    CAST(DeptID AS STRING) AS dept_id,
    CAST(DeptName AS STRING) AS dept_name
FROM
    {{ source('healthcare_data_raw', 'departments') }}