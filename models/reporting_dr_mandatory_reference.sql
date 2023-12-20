SELECT cpav2.job_position_id as jp_id,
        cpav2.compliance_package_id,
        crav2.document_type_id,
        'Reference'::text     AS record_type,
        rf.title              AS record_name,
        true                  AS is_mandatory,
        crav2.compliance_status,
        crav2.marked_done_by_jp_id
FROM {{ source('public', 'compliance_requirement_assignment_v2') }} crav2
JOIN {{ source('public', 'compliance_group_assignment_v2') }} cgav2
    ON crav2.compliance_group_assignment_id = cgav2.compliance_group_assignment_id
JOIN {{ source('public', 'compliance_package_assignment_v2') }} cpav2
    ON cgav2.compliance_package_assignment_id = cpav2.compliance_package_assignment_id
JOIN {{ source('public', 'reference_form') }} rf 
    ON crav2.reference_form_id = rf.reference_form_id
WHERE crav2.type::text = 'REFERENCE_FORM'::text