select
	event_log.job_position_id,
	max(CASE WHEN type in ('EMPLOYEE_SIGNED_IN') 
		THEN event_log.create_timestamp END)            as latest_sign_in_at,
	max(CASE WHEN type in ('JOB_POSITION_INVITED') 
		THEN event_log.create_timestamp END)            as latest_invite_at,
	max(CASE WHEN type in ('EMPLOYEE_SIGNED_OFF', 'EMPLOYEE_SIGN_OFF_CANCELLED') 
		THEN event_log.create_timestamp END)            as sign_off_status_last_change_at,
	max(CASE WHEN type in ('EMPLOYEE_ADDED_TO_ORGANISATION') 
		THEN event_log.create_timestamp END)            as added_to_org_at
from {{ source('public', 'event_log') }} event_log
group by 1