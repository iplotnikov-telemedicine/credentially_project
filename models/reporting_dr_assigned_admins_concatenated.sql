select jp_id,
        string_agg(assigned_admin, ', ') as assigned_admin
from {{ ref('reporting_dr_assigned_admins') }}
group by jp_id