select distinct on (jp.id) jp.id  as jp_id,
    case
        when jp.signed_off = true
            THEN
            (select max(el.create_timestamp)
            from {{ source('public', 'event_log') }} el
            where jp.id = el.job_position_id
                and el.type = 'EMPLOYEE_SIGNED_OFF')
        else
            (select max(el.create_timestamp)
            from {{ source('public', 'event_log') }} el
            where jp.id = el.job_position_id
                and el.type = 'EMPLOYEE_SIGN_OFF_CANCELLED') END as sign_off_status_last_change_at
from {{ source('public', 'event_log') }} el
left join {{ source('public', 'job_position') }} jp
    on el.job_position_id = jp.id
where el.type in ('EMPLOYEE_SIGNED_OFF', 'EMPLOYEE_SIGN_OFF_CANCELLED')