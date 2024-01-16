SELECT 
        sm.job_position_id as jp_id,
        string_agg(sm_assigned.full_name, ', ') as assigned_admin
FROM {{ ref('reporting_dr_reporting_job_position_view_limited') }} sm
LEFT JOIN {{ source('public', 'staff_assignee_xref') }} sa
        ON sm.job_position_id = sa.job_position_id
LEFT JOIN {{ ref('reporting_dr_reporting_job_position_view_limited') }} sm_assigned
        ON sa.assignee_job_position_id = sm_assigned.job_position_id
GROUP BY sm.job_position_id