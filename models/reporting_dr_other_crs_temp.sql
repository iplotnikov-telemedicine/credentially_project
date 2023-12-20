SELECT cpa.job_position_id as jp_id,
        cpa.compliance_package_id,
        cra.compliance_status,
        cra.marked_done_by_jp_id,
        true                AS is_mandatory,
        'Other'::text       AS record_type,
        tr.name             as record_name
FROM {{ source('public', 'compliance_requirement_assignment_v2') }} cra
JOIN {{ source('public', 'compliance_group_assignment_v2') }} cga
    ON cga.compliance_group_assignment_id = cra.compliance_group_assignment_id
JOIN {{ source('public', 'compliance_package_assignment_v2') }} cpa
    ON cga.compliance_package_assignment_id = cpa.compliance_package_assignment_id
JOIN {{ source('public', 'compliance_requirement_package') }} crp
    ON cpa.compliance_package_id = crp.compliance_requirement_package_id
JOIN {{ source('public', 'text_requirement') }} tr
    ON cra.text_requirement_id = tr.text_requirement_id
WHERE cra.type::text = 'TEXT_REQUIREMENT'