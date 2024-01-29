SELECT
    sm.jp_id,
    sm.employee_phone as "Phone",
    sm.title as "title",
    sm.employee_city as "City",
    sm.country as country,
    sm.postcode as "ZIP/Postcode",
    sm.professional_registration_number as "Professional registration number",
    sm.medical_category as "Medical category",
    sm.medical_specialty as medical_specialty,
    sm.job_position_role_description,
    sm.staff_type as "Clinical / Non-Clinical",
    sm.tags,
    sm.skip_onboarding,
    case
        when sm.doctor_grade = 'STR1' then 'StR1 (Core Training)'
        when sm.doctor_grade = 'STR2' then 'StR2 (Core Training)'
        when sm.doctor_grade = 'STR3' then 'StR3 (Core Training)'
        when sm.doctor_grade = 'SPR' then 'SpR'
        when sm.doctor_grade = 'CT' then 'Core Training'
        when sm.doctor_grade = 'STAFF_GRADE' then 'Staff Grade'
        when sm.doctor_grade = 'CONSULTANT' then 'Consultant'
        when sm.doctor_grade = 'OTHER' then Concat('Other/', sm.other_doctor_grade)
        else sm.doctor_grade
        end as "Grade",
    sm.state,
    sm.country as country_code,
    sm.birth_date as "Date of Birth",
    case WHEN onb.onboarding_id IS NULL THEN 'Not assigned' ELSE 'Assigned' END as "Assign Onboarding",
    CASE WHEN sm.tags = ' ' THEN replace(sm.tags, ' ', null) ELSE sm.tags END as "User Tag(s)",
    nullif(aa.assigned_admin, '') as "Assigned To",
    CASE WHEN sm.job_position_status = 'ACTIVE' THEN
        (CASE WHEN sm.country = 'United States' THEN 
            sm.state_name ELSE sm.state END
        ) ELSE NULL END as "State/Province"
FROM {{ ref('reporting_dr_job_position_view_limited') }} sm
INNER JOIN {{ ref('reporting_dr_visible') }} vv
    on sm.jp_id = vv.jp_id and vv.is_visible is true
LEFT JOIN {{ ref('reporting_dr_onboarding_raw') }} onb
    on sm.jp_id = onb.job_position_id
LEFT JOIN {{ ref('reporting_dr_assigned_admins') }} aa
    on sm.jp_id = aa.jp_id