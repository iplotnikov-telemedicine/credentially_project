select 
    org_id,
    jp_id,
    document_id,
    document_type_id,
    file_id,
    file_version                                     as "Document Version",
    create_timestamp                                 as doc_upload_at,
    issued 										  as "Document Issue Date",
    expiry                                           as "Document Expiry Date",
    CASE WHEN file_expiry_status = 'Not expired' and file_verification_status = 'Approved' THEN 'Valid'
        WHEN file_expiry_status in ('Not expired', 'Expires soon') and file_verification_status is null THEN 'Awaiting Approval'
        WHEN file_expiry_status = 'Expires soon' and file_verification_status = 'Approved' THEN 'Expires soon'
        WHEN file_verification_status = 'Declined' THEN 'Declined'
        WHEN file_expiry_status = 'Expired' THEN 'Expired'
        ELSE NULL end                                as "Document Status",
    file_verification_status                         as "Document Approval Status",
    verified_by                                      as "Document Approved/Declined By",
    verified_at                                      as doc_verified_at,
    create_desc,
    row_number() OVER (partition by org_id, document_id ORDER by
        CASE WHEN file_expiry_status = 'Not expired' and file_verification_status = 'Approved' THEN 0
            WHEN file_expiry_status = 'Not expired' and file_verification_status != 'Declined' THEN 1
            WHEN file_expiry_status != 'Expired' and file_verification_status = 'Approved' THEN 2
            WHEN file_expiry_status != 'Expired' and file_verification_status != 'Declined' THEN 3
            WHEN file_verification_status = 'Declined' THEN 4
            WHEN expiry is null THEN 5
            ELSE 6 end asc , expiry desc, create_timestamp desc)        AS expiry_create_desc,
    case create_desc when 1 then true else false end as is_latest_version
from {{ ref('reporting_dr_files') }} reporting_dr_files