SELECT 
    m.jp_id,
    m.compliance_package_id,
    m.document_type_id,
    'Reference' AS record_type,
    rf.title AS record_name,
    m.is_mandatory,
    m.compliance_status,
    m.marked_done_by_jp_id
FROM {{ ref('reporting_dr_mandatory') }} m
JOIN {{ source('public', 'reference_form') }} rf 
    ON m.reference_form_id = rf.reference_form_id
WHERE m.requirement_type = 'REFERENCE_FORM'