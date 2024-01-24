SELECT 
    employee_id,
    rjpvat1.job_position_id as jp_id,
    rjpvat1.organisation_id as org_id,
    job_position_status,
    CASE
        WHEN job_position_status IN ('ACTIVE')
            THEN true
        WHEN job_position_status IN ('IMPORTED', 'INVITED', 'ARCHIVED') AND
            NOT EXISTS (SELECT 1
                        FROM {{ ref('reporting_dr_reporting_job_position_view_limited') }} rjpvat2
                        WHERE rjpvat2.employee_id = rjpvat1.employee_id
                            AND rjpvat2.organisation_id != rjpvat1.organisation_id)
            THEN true
        ELSE false
        END                 AS is_visible,
    CASE
        WHEN job_position_status IN ('ACTIVE')
            THEN true
        WHEN job_position_status IN ('IMPORTED', 'INVITED') AND
            NOT EXISTS (SELECT 1
                        FROM {{ ref('reporting_dr_reporting_job_position_view_limited') }} rjpvat2
                        WHERE rjpvat2.employee_id = rjpvat1.employee_id
                            AND rjpvat2.organisation_id != rjpvat1.organisation_id)
            THEN true
        ELSE false
        END                 AS is_visible_wo_archived
FROM {{ ref('reporting_dr_reporting_job_position_view_limited') }} rjpvat1