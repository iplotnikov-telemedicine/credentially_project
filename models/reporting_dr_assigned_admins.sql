SELECT sm.job_position_id as jp_id,
    CASE
        WHEN CONCAT(ee.first_name, CONCAT(' ', ee.last_name)) = ' '
            THEN ''
        ELSE CONCAT(ee.first_name, CONCAT(' ', ee.last_name)) END as assigned_admin
FROM {{ ref('reporting_dr_reporting_job_position_view_limited') }} sm
left join {{ source('public', 'staff_assignee_xref') }} sa
        on sm.job_position_id = sa.job_position_id
LEFT JOIN {{ source('public', 'job_position') }} jp
        on sa."assignee_job_position_id" = jp."id"
LEFT JOIN {{ source('public', 'employee') }} ee
        on jp.employee_id = ee.id
WHERE sm.job_position_status in ('ACTIVE', 'IMPORTED', 'INVITED', 'ARCHIVED')