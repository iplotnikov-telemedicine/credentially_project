with current_version as (
    select *
    from {{ ref('reporting_dr_document_versions') }}
    where is_current_version = true
),
latest_version as (
    select *
    from {{ ref('reporting_dr_document_versions') }}
    where is_latest_version = true
    and is_current_version <> true
)
select recs.*,
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
        when recs."Record Type" = 'Document' 
            and recs."Record Mandatory" = 'Mandatory' 
            and cv."Document Status" is NULL 
            THEN 'Not Uploaded'
        else cv."Document Status" END   as "Document Status"
from {{ ref('reporting_dr_records_temp') }} recs
left join current_version cv
    on recs.jp_id = cv.jp_id 
    and recs.document_id = cv.document_id
left join latest_version lv
    on recs.jp_id = lv.jp_id 
    and recs.document_id = lv.document_id