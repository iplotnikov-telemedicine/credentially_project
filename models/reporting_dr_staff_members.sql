select 
    sm.organisation_id                                                 as org_id,
    sm.job_position_id                                                 as jp_id,
    sm.employee_id,
    sm.employee_email                                                  as "Email",
    CASE
        WHEN vv.is_visible is true
            THEN sm.employee_phone
        ELSE NULL END                                                  as "Phone",
    CASE
        WHEN vv.is_visible is true
            THEN (case when sm.title = '(blank)' then null else sm.title END)
        ELSE NULL END                                                  as "title",
    CASE
        WHEN vv.is_visible is true THEN (case
                                            when sm.employee_city = '(blank)'
                                                then null
                                            else sm.employee_city END)
        ELSE NULL END                                                  as "City",
    CASE
        WHEN vv.is_visible is true
            THEN (case when sm.country = '(blank)' then null else sm.country END)
        ELSE NULL END                                                  as country,
    CASE
        WHEN vv.is_visible is true
            THEN (case when sm.postcode = '(blank)' then null else sm.postcode END)
        ELSE NULL END                                                  as "ZIP/Postcode",
    CASE
        WHEN vv.is_visible is true THEN (case
                                            when sm.professional_registration_number = '(blank)'
                                                then null
                                            else sm.professional_registration_number END)
        ELSE NULL END                                                  as "Professional registration number",
    CASE
        WHEN vv.is_visible is true THEN (case
                                            when sm.medical_category = '(blank)'
                                                then null
                                            else sm.medical_category END)
        ELSE NULL END                                                  as "Medical category",
    CASE
        WHEN vv.is_visible is true THEN (case
                                            when sm.medical_specialty = '(blank)'
                                                then null
                                            else sm.medical_specialty END)
        ELSE NULL END                                                  as medical_specialty,
    sm.job_position_role                                               as "Role(s)",
    sm.job_position_role                                               as "Role",
    CASE
        WHEN vv.is_visible is true
            THEN sm.job_position_role_description
        ELSE NULL END                                                  as job_position_role_description,
    CASE
        WHEN vv.is_visible is true THEN
            (case
                when sm.staff_type = 'CLINICAL'
                    THEN 'Clinical'
                when sm.staff_type = 'NON_CLINICAL'
                    THEN 'Non-Clinical'
                END)
        ELSE NULL END                                                  as "Clinical / Non-Clinical",
    case
        when sm.job_position_status = 'ACTIVE'
            THEN 'Active'
        when sm.job_position_status = 'IMPORTED'
            THEN 'Not invited'
        when sm.job_position_status = 'INVITED'
            THEN 'Invited'
        when sm.job_position_status = 'ARCHIVED'
            THEN 'Archived'
        END                                                            as "Staff Member Status(es)",
    CASE
        WHEN sm.signed_off = true THEN 'Signed Off'
        ELSE 'Not Signed Off' END                                      as "Signed off",
    CASE
        WHEN vv.is_visible is true
            THEN sm.skip_onboarding
        ELSE NULL END                                                  as skip_onboarding,
    CASE
        when sm.job_position_status = 'ARCHIVED'
            then 'n/a'
        when sm.compliance_status = 'COMPLIANT'
            THEN 'Compliant'
        when sm.compliance_status = 'NOT_COMPLIANT'
            THEN 'Non-Compliant'
        END                                                            as "Compliance",
    CASE
        WHEN vv.is_visible is true
            THEN (case when sm.tags = '(blank)' then null else sm.tags END)
        ELSE NULL END                                                  as tags,
    case
        when full_name = '(blank)' then null
        ELSE full_name END                                             as "Full Name",
    CASE
        WHEN vv.is_visible is true THEN
            (case
                when sm.doctor_grade = 'STR1'
                    then 'StR1 (Core Training)'
                when sm.doctor_grade = 'STR2'
                    then 'StR2 (Core Training)'
                when sm.doctor_grade = 'STR3'
                    then 'StR3 (Core Training)'
                when sm.doctor_grade = 'SPR'
                    then 'SpR'
                when sm.doctor_grade = 'CT'
                    then 'Core Training'
                when sm.doctor_grade = 'STAFF_GRADE'
                    then 'Staff Grade'
                when sm.doctor_grade = 'CONSULTANT'
                    then 'Consultant'
                when sm.doctor_grade = 'OTHER'
                    then Concat('Other/', COALESCE(sm.other_doctor_grade, '(blank)'))
                else (case when sm.doctor_grade = '(blank)' then null else sm.doctor_grade END)
                end)
        ELSE NULL END                                                  as "Grade",
    CASE
        WHEN vv.is_visible is true THEN sm.state
        ELSE NULL END                                                  as state,
    CASE
        WHEN vv.is_visible is true THEN sm.country
        ELSE NULL END                                                  as country_code,
    CASE
        WHEN vv.is_visible is true THEN sm.birth_date
        ELSE NULL END                                                  as "Date of Birth",
    CASE
        WHEN vv.is_visible is true THEN
            (case
                WHEN onb.onboarding_id IS NULL
                    THEN 'Not assigned'
                ELSE 'Assigned' END)
        ELSE NULL END                                                  as "Assign Onboarding",
    CASE
        WHEN vv.is_visible is true THEN
            nullif(reporting_dr_assigned_admins.assigned_admin, '')
            ELSE NULL END     										as "Assigned To",
    CASE
        WHEN vv.is_visible is true THEN
            (CASE
                WHEN sm.tags = ' '
                    THEN replace(sm.tags, ' ', null)
                ELSE sm.tags END)
        ELSE NULL END                                                  as "User Tag(s)",
    CASE
        WHEN vv.is_visible is true THEN
            (CASE
                WHEN sm.job_position_status = 'ACTIVE'
                    THEN
                    (case
                        when sm.country = 'United States'
                            THEN sm.state_name
                        ELSE sm.state END)
                ELSE NULL END)
        ELSE NULL END                                                  as "State/Province",
    convert_to_org_timezone(sm.organisation_id,
                            coalesce(
                                (select el.create_timestamp
                                from "public"."event_log" el
                                where sm.job_position_id = el.job_position_id
                                    and el.type = 'EMPLOYEE_ADDED_TO_ORGANISATION'),
                                sm.create_timestamp))                  as "Staff Member Added Date",
    convert_to_org_timezone(sm.organisation_id,
                            (select events.occured_at
                            from "public"."reporting_events_view" events
                            where sm.job_position_id = events.job_position_id
                                and events.event_type = 'Signed up'))   as "Staff Member Signed Up Date",
    convert_to_org_timezone(sm.organisation_id,
                            le.sign_off_status_last_change_at)        as "Sign Off Status Last Change Date",
    convert_to_org_timezone(sm.organisation_id, le.latest_sign_in_at) as "Latest Sign In Date",
    convert_to_org_timezone(sm.organisation_id, le.latest_invite_at)   as "Latest Invite Date"
from {{ ref('reporting_dr_reporting_job_position_view_limited') }} sm
left join {{ source('public', 'reporting_onboarding_view') }} onb
    on sm.job_position_id = onb.job_position_id
LEFT JOIN {{ ref('reporting_dr_visible') }} vv
    on sm.job_position_id = vv.jp_id
left join {{ ref('reporting_dr_assigned_admins') }} reporting_dr_assigned_admins
    on sm.job_position_id = reporting_dr_assigned_admins.jp_id
left join {{ ref('reporting_dr_latest_events_by_jp') }} le
    on le.job_position_id = sm.job_position_id