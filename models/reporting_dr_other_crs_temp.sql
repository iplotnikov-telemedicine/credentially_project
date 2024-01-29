SELECT 
    m.jp_id,
    m.compliance_package_id,
    m.compliance_status,
    m.marked_done_by_jp_id,
    m.is_mandatory,
    'Other' AS record_type,
    tr.name AS record_name
FROM {{ ref('reporting_dr_mandatory') }} m
join {{ source('public', 'text_requirement') }} tr
    on m.text_requirement_id = tr.text_requirement_id
where m.requirement_type = 'TEXT_REQUIREMENT'