with current_version as (
    select
        cv.jp_id,
        cv.document_id,
        cv."Document Version",
        cv."Document Upload Date",
        cv."Document Approval Status",
        cv."Document Status",
        cv."Document Issue Date",
        cv."Document Expiry Date",
        cv."Document Approved/Declined By",
        cv."Document Approval/Decline Date"
    from {{ ref('reporting_dr_document_versions') }} cv
    where is_current_version = true
),
latest_version as (
    select  
        lv.jp_id,
        lv.document_id,
        lv."Document Version",
        lv."Document Upload Date",
        lv."Document Status",
        lv."Document Expiry Date",
        lv."Document Approval Status",
        lv."Document Approved/Declined By",
        lv."Document Approval/Decline Date"
    from {{ ref('reporting_dr_document_versions') }} lv
    where is_latest_version = true
    and is_current_version <> true
)
select 
    recs.*,
    cv."Document Version"               as "Document Version (Current Version)",
    cv."Document Upload Date"           as "Document Upload Date (Current Version)",
    cv."Document Approval Status"       as "Document Approval Status (Current Version)",
    cv."Document Status"                as "Document Status (Current Version)",
    cv."Document Issue Date"			as "Document Issue Date (Current Version)",
    cv."Document Expiry Date"           as "Document Expiry Date (Current Version)",
    cv."Document Approved/Declined By"  as "Document Approved/Declined By (Current Version)",
    cv."Document Approval/Decline Date" as "Document Approval/Decline Date (Current Version)",
    lv."Document Version"               as "Document Version (Latest Version)",
    lv."Document Upload Date"           as "Document Upload Date (Latest Version)",
    lv."Document Status"                as "Document Status (Latest Version)",
    lv."Document Expiry Date"           as "Document Expiry Date (Latest Version)",
    lv."Document Approval Status"       as "Document Approval Status (Latest Version)",
    lv."Document Approved/Declined By"  as "Document Approved/Declined By (Latest Version)",
    lv."Document Approval/Decline Date" as "Document Approval/Decline Date (Latest Version)",
    case
        when recs."Record Type" = 'Document' and recs."Record Mandatory" = 'Mandatory' and cv."Document Status" is NULL 
            THEN 'Not Uploaded'
        else cv."Document Status" END   as "Document Status",
    cv."Document Expiry Date" >= CURRENT_DATE - 30
        AND cv."Document Expiry Date" < CURRENT_DATE as "Expired Last 30 Days",
    cv."Document Expiry Date" >= CURRENT_DATE - 7
        AND cv."Document Expiry Date" < CURRENT_DATE as "Expired Last 7 Days",
    cv."Document Expiry Date" >= CURRENT_DATE - 1
        AND cv."Document Expiry Date" < CURRENT_DATE as "Expired Yesterday",
    cv."Document Expiry Date" >= CURRENT_DATE
        AND cv."Document Expiry Date" < CURRENT_DATE + 1 as "Expired Today",
    cv."Document Expiry Date" >= CURRENT_DATE + 1
        AND cv."Document Expiry Date" < CURRENT_DATE + 8 as "Expires In 1 Week",
    cv."Document Expiry Date" >= CURRENT_DATE + 1
        AND cv."Document Expiry Date" < CURRENT_DATE + 15 as "Expires In 2 Weeks",
    cv."Document Expiry Date" >= CURRENT_DATE + 1
        AND cv."Document Expiry Date" < CURRENT_DATE + 22 as "Expires In 3 Weeks",
    cv."Document Expiry Date" >= CURRENT_DATE + 1
        AND cv."Document Expiry Date" < CURRENT_DATE + 29 as "Expires In 4 Weeks"
from {{ ref('reporting_dr_records_temp') }} recs
left join current_version cv
    on recs.jp_id = cv.jp_id and recs.document_id = cv.document_id
left join latest_version lv
    on recs.jp_id = lv.jp_id and recs.document_id = lv.document_id