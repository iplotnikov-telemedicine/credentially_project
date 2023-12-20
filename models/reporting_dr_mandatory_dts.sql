SELECT 
    cpa.job_position_id as jp_id,
    cpa.compliance_package_id,
    cra.document_type_id,
    d.id                as document_id,
    cra.compliance_status,
    cra.marked_done_by_jp_id
FROM {{ source('public', 'compliance_requirement_assignment_v2') }} cra
JOIN {{ source('public', 'compliance_group_assignment_v2') }} cga
    ON cga.compliance_group_assignment_id = cra.compliance_group_assignment_id
JOIN {{ source('public', 'compliance_package_assignment_v2') }} cpa
    ON cga.compliance_package_assignment_id = cpa.compliance_package_assignment_id
JOIN {{ source('public', 'compliance_requirement_package') }} crp
    ON cpa.compliance_package_id = crp.compliance_requirement_package_id
join {{ source('public', 'job_position') }} jp 
    on cpa.job_position_id = jp.id
join {{ source('public', 'personnel_types') }} pt 
    on jp.role_id = pt.personnel_type_id
join {{ source('public', 'organisation') }} o 
    on pt.organisation_id = o.id
left join {{ source('public', 'document') }} d
    on jp.employee_id = d.employee_id
    and cra.document_type_id = d.type_id
where cra.type::text = 'DOCUMENT_TYPE'