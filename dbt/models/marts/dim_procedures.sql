SELECT DISTINCT
    procedure_id,
    cpt_code,
    procedure_description

FROM
    {{ ref('stg_procedures') }}