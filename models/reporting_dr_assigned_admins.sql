SELECT
    sm.jp_id,
    string_agg(sm_assigned.full_name, ', ' order by sa.id desc) as assigned_admin
FROM {{ ref('reporting_dr_job_position_view_limited') }} sm
LEFT JOIN {{ source('public', 'staff_assignee_xref') }} sa
    ON sm.jp_id = sa.job_position_id
LEFT JOIN {{ ref('reporting_dr_job_position_view_limited') }} sm_assigned
    ON sa.assignee_job_position_id = sm_assigned.jp_id
GROUP BY sm.jp_id