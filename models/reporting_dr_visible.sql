SELECT
    DISTINCT ON (rjpvat1.jp_id)
    rjpvat1.employee_id,
    rjpvat1.jp_id,
    rjpvat1.organisation_id as org_id,
    rjpvat1.job_position_status,
    CASE
        WHEN rjpvat1.job_position_status IN ('ACTIVE') THEN true
        WHEN rjpvat1.job_position_status IN ('IMPORTED', 'INVITED', 'ARCHIVED') AND rjpvat2.employee_id IS NULL THEN true
        ELSE false END AS is_visible,
    CASE
        WHEN rjpvat1.job_position_status IN ('ACTIVE') THEN true
        WHEN rjpvat1.job_position_status IN ('IMPORTED', 'INVITED') AND rjpvat2.employee_id IS NULL THEN true
        ELSE false END AS is_visible_wo_archived
FROM {{ ref('reporting_dr_job_position_view_limited') }} rjpvat1
LEFT JOIN {{ ref('reporting_dr_job_position_view_limited') }} rjpvat2
    ON rjpvat2.employee_id = rjpvat1.employee_id
    AND rjpvat2.organisation_id != rjpvat1.organisation_id