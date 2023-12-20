select jp_id,
        string_agg(user_tags, ', ') as user_tags
from {{ ref('reporting_dr_user_tags') }}
group by jp_id