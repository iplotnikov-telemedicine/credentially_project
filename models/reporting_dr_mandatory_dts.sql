SELECT 
    m.jp_id,
    m.compliance_package_id,
    m.document_type_id,
    m.compliance_status,
    m.marked_done_by_jp_id,
    m.is_mandatory,
    d.id as document_id
FROM {{ ref('reporting_dr_mandatory') }} m
JOIN {{ ref('reporting_dr_job_position_view_limited') }} jpvl
    ON m.jp_id = jpvl.jp_id
LEFT JOIN {{ source('public', 'document') }} d
    ON jpvl.employee_id = d.employee_id
    AND m.document_type_id = d.type_id
WHERE m.requirement_type = 'DOCUMENT_TYPE'