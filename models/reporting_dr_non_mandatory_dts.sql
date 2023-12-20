SELECT 
    jp.id AS jp_id,
    df.document_id,
    d.type_id AS document_type_id
FROM {{ source('public', 'job_position') }} jp
JOIN {{ source('public', 'personnel_types') }} pt ON jp.role_id = pt.personnel_type_id
JOIN {{ source('public', 'organisation') }} o ON pt.organisation_id = o.id
JOIN {{ source('public', 'employee') }} e ON jp.employee_id = e.id
JOIN {{ source('public', 'document') }} d ON e.id = d.employee_id
JOIN {{ source('public', 'document_files_xref') }} df
    ON d.id = df.document_id AND o.id = df.organisation_id
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ ref('reporting_dr_mandatory_dts') }} dtfp
    WHERE dtfp.jp_id = jp.id
        AND dtfp.document_type_id = d.type_id
)