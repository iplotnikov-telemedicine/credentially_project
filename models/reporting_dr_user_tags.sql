SELECT sm.job_position_id as jp_id,
    ct.name            as user_tags
from {{ ref('reporting_dr_reporting_job_position_view') }} sm
left join {{ source('public', 'reporting_job_position_custom_tags_view') }} jpct
    on sm.job_position_id = jpct.job_position_id
inner join {{ source('public', 'reporting_custom_tag_view') }} ct
    on ct.tag_id = jpct.tag_id