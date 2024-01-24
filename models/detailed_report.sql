select 
    sm."jp_id",
    sm.org_id,
    sm."Email",
    sm."Phone",
    sm."title",
    sm."Role",
    sm."Staff Member Status(es)",
    sm."Signed off",
    sm."Compliance",
    sm."Full Name",
    sm."Date of Birth",
    sm."Assigned To",
    sm."User Tag(s)",
    sm."Staff Member Added Date",
    sm."Staff Member Signed Up Date",
    sm."Sign Off Status Last Change Date",
    sm."Latest Sign In Date",
    sm."Latest Invite Date",
    onb."Current step name",
    onb."Onboarding Start Time",
    onb."Onboarding Completion Time",
    onb."Onboarding Status",
    onb."Onboarding completed by",
    cp."Compliance Percentage",
    cpk."Compliance Package",
    cpk."Package Compliance Percentage",
    rwd."integration_type_short",
    rwd."Record Name",
    rwd."Record Type",
    rwd."Record Mandatory",
    rwd."Compliance Requirement Status",
    rwd."Compliance Requirement Fulfilled By",
    rwd."Document Version (Current Version)",
    rwd."Document Upload Date (Current Version)",
    rwd."Document Status (Current Version)",
    rwd."Document Issue Date (Current Version)",
    rwd."Document Expiry Date (Current Version)",
    rwd."Document Approval Status (Current Version)",
    rwd."Document Approved/Declined By (Current Version)",
    rwd."Document Approval/Decline Date (Current Version)",
    rwd."Document Version (Latest Version)",
    rwd."Document Upload Date (Latest Version)",
    rwd."Document Status (Latest Version)",
    rwd."Document Expiry Date (Latest Version)",
    rwd."Document Approval Status (Latest Version)",
    rwd."Document Approved/Declined By (Latest Version)",
    rwd."Document Approval/Decline Date (Latest Version)",
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
    ) as "Haven't Logged In For 14 Days",
    COALESCE(
        onb."Onboarding Completion Time" >= CURRENT_DATE - 30
        AND onb."Onboarding Completion Time" < CURRENT_DATE, false
    ) as "Completed Onboarding Last 30 Days",
    COALESCE(
        onb."Onboarding Completion Time" >= CURRENT_DATE - 7
        AND onb."Onboarding Completion Time" < CURRENT_DATE, false
    ) as "Completed Onboarding Last 7 Days",
    COALESCE(
        onb."Onboarding Completion Time" >= CURRENT_DATE - 1
        AND onb."Onboarding Completion Time" < CURRENT_DATE, false
    ) as "Completed Onboarding Yesterday",
    COALESCE(
        rwd."Document Expiry Date (Current Version)" >= CURRENT_DATE - 30
        AND rwd."Document Expiry Date (Current Version)" < CURRENT_DATE, false
    ) as "Expired Last 30 Days",
    COALESCE(
        rwd."Document Expiry Date (Current Version)" >= CURRENT_DATE - 7
        AND rwd."Document Expiry Date (Current Version)" < CURRENT_DATE, false
    ) as "Expired Last 7 Days",
    COALESCE(
        rwd."Document Expiry Date (Current Version)" >= CURRENT_DATE - 1
        AND rwd."Document Expiry Date (Current Version)" < CURRENT_DATE, false
    ) as "Expired Yesterday",
    COALESCE(
        rwd."Document Expiry Date (Current Version)" >= CURRENT_DATE
        AND rwd."Document Expiry Date (Current Version)" < CURRENT_DATE + 1, false
    ) as "Expired Today",
    COALESCE(
        rwd."Document Expiry Date (Current Version)" >= CURRENT_DATE + 1
        AND rwd."Document Expiry Date (Current Version)" < CURRENT_DATE + 8, false
    ) as "Expires In 1 Week",
    COALESCE(
        rwd."Document Expiry Date (Current Version)" >= CURRENT_DATE + 1
        AND rwd."Document Expiry Date (Current Version)" < CURRENT_DATE + 15, false
    ) as "Expires In 2 Weeks",
    COALESCE(
        rwd."Document Expiry Date (Current Version)" >= CURRENT_DATE + 1
        AND rwd."Document Expiry Date (Current Version)" < CURRENT_DATE + 22, false
    ) as "Expires In 3 Weeks",
    COALESCE(
        rwd."Document Expiry Date (Current Version)" >= CURRENT_DATE + 1
        AND rwd."Document Expiry Date (Current Version)" < CURRENT_DATE + 29, false
    ) as "Expires In 4 Weeks"
from {{ ref('reporting_dr_staff_members') }} sm
left join {{ ref('reporting_dr_onboarding') }} onb
    on sm.jp_id = onb.jp_id
left join {{ ref('reporting_dr_compliance_percentage') }} cp
    on sm.jp_id = cp.jp_id
left join {{ ref('reporting_dr_records_with_details') }} rwd
    on sm.jp_id = rwd.jp_id
left join {{ ref('reporting_dr_compliance_packages') }} cpk
    on sm.jp_id = cpk.jp_id and rwd.compliance_package_id = cpk."Compliance Package ID"