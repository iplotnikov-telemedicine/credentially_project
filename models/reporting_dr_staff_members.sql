with sm as (
    select 
        sm.organisation_id as org_id,
        sm.jp_id,
        sm.employee_id,
        sm.employee_email as "Email",
        sm.job_position_role as "Role",
        vd."Phone",
        vd."title",
        vd."City",
        vd.country,
        vd."ZIP/Postcode",
        vd."Professional registration number",
        vd."Medical category",
        vd.medical_specialty,
        vd.job_position_role_description,
        vd."Clinical / Non-Clinical",
        vd.tags,
        vd.skip_onboarding,
        vd."Grade",
        vd.state,
        vd.country_code,
        vd."Date of Birth",
        vd."Assign Onboarding",
        vd."Assigned To",
        vd."User Tag(s)",
        vd."State/Province",
        case
            when sm.job_position_status = 'ACTIVE' THEN 'Active'
            when sm.job_position_status = 'IMPORTED' THEN 'Not invited'
            when sm.job_position_status = 'INVITED' THEN 'Invited'
            when sm.job_position_status = 'ARCHIVED' THEN 'Archived'
            END                                                            as "Staff Member Status(es)",
        CASE WHEN sm.signed_off = true THEN 'Signed Off' ELSE 'Not Signed Off' END as "Signed off",
        CASE
            when sm.job_position_status = 'ARCHIVED' then 'n/a'
            when sm.compliance_status = 'COMPLIANT' THEN 'Compliant'
            when sm.compliance_status = 'NOT_COMPLIANT' THEN 'Non-Compliant'
            END                                                            as "Compliance",
        NULLIF(full_name, ' ') as "Full Name",
        convert_to_org_timezone(sm.organisation_id, coalesce(le.added_to_org_at, sm.create_timestamp)) as "Staff Member Added Date",
        convert_to_org_timezone(sm.organisation_id, signed_ups.occured_at) as "Staff Member Signed Up Date",
        convert_to_org_timezone(sm.organisation_id, le.sign_off_status_last_change_at) as "Sign Off Status Last Change Date",
        convert_to_org_timezone(sm.organisation_id, le.latest_sign_in_at) as "Latest Sign In Date",
        convert_to_org_timezone(sm.organisation_id, le.latest_invite_at)   as "Latest Invite Date"
    from {{ ref('reporting_dr_job_position_view_limited') }} sm
    left join {{ ref('reporting_dr_sm_visible_data') }} vd
        on sm.jp_id = vd.jp_id
    left join {{ ref('reporting_dr_latest_events_by_jp') }} le
        on le.job_position_id = sm.jp_id
    left join {{ source('public', 'reporting_events_view') }} signed_ups
        on sm.jp_id = signed_ups.job_position_id
        and signed_ups.event_type = 'Signed up'
)
select
    sm.*,
    COALESCE(
        sm."Staff Member Added Date" >= CURRENT_DATE - 30
        AND sm."Staff Member Added Date" < CURRENT_DATE, false
    ) as "Is Added Last 30 Days",
    COALESCE(
        sm."Staff Member Added Date" >= CURRENT_DATE - 7
        AND sm."Staff Member Added Date" < CURRENT_DATE, false
    ) as "Is Added Last 7 Days",
    COALESCE(
        sm."Staff Member Added Date" >= CURRENT_DATE - 1
        AND sm."Staff Member Added Date" < CURRENT_DATE, false
    ) as "Is Added Yesterday",
    COALESCE(
        sm."Sign Off Status Last Change Date" >= CURRENT_DATE - 30
        AND sm."Sign Off Status Last Change Date" < CURRENT_DATE, false
    ) as "Is Signed Off Last 30 Days",
    COALESCE(
        sm."Sign Off Status Last Change Date" >= CURRENT_DATE - 7
        AND sm."Sign Off Status Last Change Date" < CURRENT_DATE, false
    ) as "Is Signed Off Last 7 Days",
    COALESCE(
        sm."Sign Off Status Last Change Date" >= CURRENT_DATE - 1
        AND sm."Sign Off Status Last Change Date" < CURRENT_DATE, false
    ) as "Is Signed Off Yesterday",
    COALESCE(
        sm."Staff Member Signed Up Date" >= CURRENT_DATE - 30
        AND sm."Staff Member Signed Up Date" < CURRENT_DATE, false
    ) as "Is Signed Up Last 30 Days",
    COALESCE(
        sm."Staff Member Signed Up Date" >= CURRENT_DATE - 7
        AND sm."Staff Member Signed Up Date" < CURRENT_DATE, false
    ) as "Is Signed Up Last 7 Days",
    COALESCE(
        sm."Staff Member Signed Up Date" >= CURRENT_DATE - 1
        AND sm."Staff Member Signed Up Date" < CURRENT_DATE, false
    ) as "Is Signed Up Yesterday",
    sm."Staff Member Status(es)" = 'Invited' as "Is Invited",
    sm."Assigned To" IS NULL as "Is Not Assigned",
    sm."Signed off" = 'Signed Off' AND sm."Compliance" = 'Non-Compliant' as "Is Signed Off & Non-Compliant",
    COALESCE(
        sm."Latest Sign In Date" < CURRENT_DATE - 2, true
    ) as "Haven't Logged In For 3 Days",
    COALESCE(
        sm."Latest Sign In Date" < CURRENT_DATE - 6, true
    ) as "Haven't Logged In For 7 Days",
    COALESCE(
        sm."Latest Sign In Date" < CURRENT_DATE - 13, true
    ) as "Haven't Logged In For 14 Days"
from sm