select 
    reporting_dr_nmc.jp_id,
    null::bigint as compliance_package_id,
    null::bigint as document_type_id,
    null::bigint as document_id,
    reporting_dr_nmc.integration_type_short,
    reporting_dr_nmc."Record Name",
    reporting_dr_nmc."Record Type",
    reporting_dr_nmc."Record Mandatory",
    null::text   as "Compliance Requirement Status",
    null::text   as "Compliance Requirement Fulfilled By"
from {{ ref('reporting_dr_nmc') }} reporting_dr_nmc
left join {{ ref('reporting_dr_records_wo_non_mandatory_checks') }} rwnmc
    on reporting_dr_nmc.jp_id = rwnmc.jp_id 
    and reporting_dr_nmc.integration_type_short = rwnmc.integration_type_short
where rwnmc.jp_id is null
    and rwnmc.integration_type_short is null