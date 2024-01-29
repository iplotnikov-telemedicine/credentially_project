SELECT 
    jpvl.jp_id,
    df.document_id,
    d.type_id AS document_type_id
FROM {{ ref('reporting_dr_job_position_view_limited') }} jpvl
JOIN {{ source('public', 'document') }} d 
    ON jpvl.employee_id = d.employee_id
JOIN {{ source('public', 'document_files_xref') }} df
    ON d.id = df.document_id 
    AND jpvl.organisation_id = df.organisation_id
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ ref('reporting_dr_mandatory_dts') }} dtfp
    WHERE dtfp.jp_id = jpvl.jp_id
        AND dtfp.document_type_id = d.type_id
)