select 
    reporting_dr_unioned.jp_id,
    reporting_dr_unioned.compliance_package_id,
    reporting_dr_unioned.document_type_id,
    reporting_dr_unioned.document_id,
    reporting_dr_unioned.integration_type_short,
    reporting_dr_unioned.record_name as "Record Name",
    reporting_dr_unioned.record_type as "Record Type",
    CASE reporting_dr_unioned.is_mandatory
        WHEN TRUE THEN 'Mandatory'
        WHEN FALSE
            THEN 'Non-mandatory'
        ELSE NULL END                as "Record Mandatory",
    case
        when reporting_dr_unioned.is_mandatory is true and
            reporting_dr_unioned.compliance_status =
            'COMPLIANT'
            then 'Compliant'
        when reporting_dr_unioned.is_mandatory is true and
            reporting_dr_unioned.compliance_status =
            'NOT_COMPLIANT'
            then 'Not Compliant'
        else NULL end                as "Compliance Requirement Status",
    case
        when reporting_dr_unioned.is_mandatory is true and
            reporting_dr_unioned.compliance_status =
            'COMPLIANT'
            then
            case
                when marked_done_by_jp_id is not null
                    then e.first_name || ' ' || e.last_name || ' (' || pt.name || ')'
                else 'System' end
        else NULL end                as "Compliance Requirement Fulfilled By"
from {{ ref('reporting_dr_unioned') }} reporting_dr_unioned
left join {{ source('public', 'job_position') }} jp on reporting_dr_unioned.jp_id = jp.id
JOIN {{ ref('reporting_dr_visible') }} reporting_dr_visible
    on reporting_dr_unioned.jp_id = reporting_dr_visible.jp_id
    and reporting_dr_visible.is_visible is true
JOIN {{ source('public', 'employee') }} e ON jp.employee_id = e.id
JOIN {{ source('public', 'personnel_types') }} pt ON jp.role_id = pt.personnel_type_id
