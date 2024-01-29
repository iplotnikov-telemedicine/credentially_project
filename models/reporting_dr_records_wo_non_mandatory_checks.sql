select 
    u.jp_id,
    u.compliance_package_id,
    u.document_type_id,
    u.document_id,
    u.integration_type_short,
    u.record_name as "Record Name",
    u.record_type as "Record Type",
    CASE u.is_mandatory
        WHEN TRUE THEN 'Mandatory'
        WHEN FALSE THEN 'Non-mandatory'
        ELSE NULL END                as "Record Mandatory",
    case
        when u.is_mandatory is true and u.compliance_status = 'COMPLIANT' then 'Compliant'
        when u.is_mandatory is true and u.compliance_status = 'NOT_COMPLIANT' then 'Not Compliant'
        else NULL end                as "Compliance Requirement Status",
    case
        when u.is_mandatory is true and u.compliance_status = 'COMPLIANT' then 
        	case
                when marked_done_by_jp_id is not null then jpvl.full_name_with_role
                else 'System' end
        else NULL end                as "Compliance Requirement Fulfilled By"
FROM {{ ref('reporting_dr_unioned') }} u
JOIN {{ ref('reporting_dr_job_position_view_limited') }} jpvl
	ON u.jp_id = jpvl.jp_id
JOIN {{ ref('reporting_dr_visible') }} vv
    ON u.jp_id = vv.jp_id AND vv.is_visible is true