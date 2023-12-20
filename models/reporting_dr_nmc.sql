select 
    reporting_dr_visible.org_id,
    reporting_dr_unioned_integrations.*,
    'Non-mandatory' as "Record Mandatory"
from {{ ref('reporting_dr_unioned_integrations') }} reporting_dr_unioned_integrations
JOIN {{ ref('reporting_dr_visible') }} reporting_dr_visible
    on reporting_dr_unioned_integrations.jp_id = reporting_dr_visible.jp_id
    and reporting_dr_visible.is_visible is true
    and reporting_dr_visible.job_position_status in ('ACTIVE', 'IMPORTED', 'INVITED')