SELECT
    dept_id AS department_id,
    dept_name AS department_name
FROM
    {{ ref('stg_departments') }}