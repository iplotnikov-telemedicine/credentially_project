WITH onboarding_record AS (
	 SELECT DISTINCT ON (c.organisation_id, jp.employee_id) ca.id AS onboarding_id,
	    c.organisation_id,
	    jp.employee_id,
	    c.version AS onboarding_version,
	    c.id AS checklist_id,
	    ca.first_incomplete_step_id AS current_step_id,
	    ca.create_timestamp AS onboarding_created_at,
	    ca.update_timestamp AS onboarding_updated_at,
	    ca.started_date_time AS onboarding_started_at,
	    ca.completed_date_time AS onboarding_completed_at,
	    ca.status AS onboarding_status
	  FROM {{ source('public', 'checklist_assignment') }} ca
	  JOIN {{ source('public', 'checklist') }} c 
	   	ON ca.checklist_id = c.id
	  JOIN {{ source('public', 'job_position') }} jp 
	   	ON ca.job_position_id = jp.id
	  WHERE c.type = 'ONBOARDING'
	  ORDER BY c.organisation_id, jp.employee_id, ca.create_timestamp DESC
), 
onboarding_with_jp AS (
	SELECT DISTINCT ON (r.onboarding_id) 
		jpvl.jp_id as job_position_id,
		r.onboarding_id,
		r.organisation_id,
		r.employee_id,
		r.onboarding_version,
		r.checklist_id,
		r.current_step_id,
		r.onboarding_created_at,
		r.onboarding_updated_at,
		r.onboarding_started_at,
		r.onboarding_completed_at,
		r.onboarding_status,
		jpvl.job_position_status
	FROM onboarding_record r
	JOIN {{ ref('reporting_dr_job_position_view_limited') }} jpvl
		ON jpvl.employee_id = r.employee_id
		AND jpvl.organisation_id = r.organisation_id
	WHERE jpvl.job_position_status IN ('ACTIVE', 'ARCHIVED') 
		AND r.onboarding_status <> 'DECLINED' 
		OR jpvl.job_position_status = 'DENIED'
		AND r.onboarding_status = 'DECLINED'
	ORDER BY r.onboarding_id, jpvl.create_timestamp DESC
), 
onboarding_with_step AS (
	 SELECT record.job_position_id,
	    record.onboarding_id,
	    record.organisation_id,
	    record.employee_id,
	    record.onboarding_version,
	    record.checklist_id,
	    record.current_step_id,
	    record.onboarding_created_at,
	    record.onboarding_updated_at,
	    record.onboarding_started_at,
	    record.onboarding_completed_at,
	    record.onboarding_status,
	    record.job_position_status,
	    cs.name AS current_step_name,
	    cs.type AS current_step_type
	   FROM onboarding_with_jp record
	     LEFT JOIN checklist_step cs ON record.current_step_id = cs.id
), 
completed_onboarding AS (
	SELECT 
 		DISTINCT record.onboarding_id
    FROM onboarding_with_jp record
    JOIN {{ source('public', 'event_log') }} event 
    	ON event.job_position_id = record.job_position_id
    WHERE (event.payload ->> 'notificationType'::text) = 'EMPLOYEE_ONBOARDING_COMPLETED'
  		AND abs(EXTRACT(epoch FROM event.create_timestamp - record.onboarding_completed_at)) <= 60::numeric
)
SELECT 
 	onboarding.organisation_id,
    onboarding.employee_id,
    onboarding.job_position_id,
    onboarding.onboarding_id,
    onboarding.checklist_id,
    onboarding.onboarding_version,
    onboarding.current_step_name AS current_step_name,
    onboarding.current_step_type AS current_step_type,
    onboarding.onboarding_created_at,
    onboarding.onboarding_started_at,
    onboarding.onboarding_updated_at,
    onboarding.onboarding_completed_at,
    CASE
        WHEN onboarding.onboarding_status = 'COMPLETED'
        	THEN EXTRACT(epoch FROM onboarding.onboarding_completed_at - onboarding.onboarding_started_at)
        ELSE NULL::numeric
    END AS completion_time,
    onboarding.onboarding_status,
    CASE
        WHEN onboarding.onboarding_status <> 'COMPLETED'
        	THEN NULL
        WHEN completed.onboarding_id IS NULL 
        	THEN 'User'
        ELSE 'Admin'
    END AS completed_by
 FROM onboarding_with_step onboarding
 LEFT JOIN completed_onboarding completed 
  	ON onboarding.onboarding_id = completed.onboarding_id