SELECT
    cpa.job_position_id as jp_id,
    cpa.compliance_package_id,
    cga.compliance_package_assignment_id,
    cra.compliance_status,
    cra.marked_done_by_jp_id,
    true                AS is_mandatory,
    cra.type          AS requirement_type,
    cra.integration_type,
    cra.document_type_id,
    cra.text_requirement_id,
    cra.reference_form_id,
    MD5(
        CONCAT(
            CASE cra.type
                WHEN 'INTEGRATION' THEN cra.integration_type
                WHEN 'REFERENCE_FORM' THEN CAST(cra.reference_form_id AS TEXT)
                WHEN 'DOCUMENT_TYPE' THEN CAST(cra.document_type_id AS TEXT)
                WHEN 'TEXT_REQUIREMENT' THEN CAST(cra.text_requirement_id AS TEXT)
                END, cra.type
        )
    )                   AS surrogate_key
FROM {{ source('public', 'compliance_requirement_assignment_v2') }} cra
JOIN {{ source('public', 'compliance_group_assignment_v2') }} cga
    ON cra.compliance_group_assignment_id = cga.compliance_group_assignment_id
JOIN {{ source('public', 'compliance_package_assignment_v2') }} cpa
    ON cga.compliance_package_assignment_id = cpa.compliance_package_assignment_id
WHERE cra.type IN ('INTEGRATION', 'DOCUMENT_TYPE', 'REFERENCE_FORM', 'TEXT_REQUIREMENT')