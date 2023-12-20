select el.job_position_id    as jp_id,
        max(create_timestamp) as latest_invite_at
from {{ source('public', 'event_log') }} el
group by jp_id, type
having type = ('JOB_POSITION_INVITED')