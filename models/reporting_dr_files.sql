select 
    o.id                                                                 as org_id,
    e.email,
    jp.id                                                                as jp_id,
    d.id                                                                 as document_id,
    f.id                                                                 as file_id,
    dt.id                                                                as document_type_id,
    dt.name,
    f.issued,
    f.expiry,
    f.create_timestamp, -- latest
    row_number()
    OVER (partition by df.organisation_id, df.document_id ORDER BY f.id) AS file_version,
    CASE
        WHEN f.expiry is null
            THEN 'Not expired'
        WHEN f.expiry::date < now()
            THEN 'Expired'
        WHEN (f.expiry::date - interval '1 day' * dt.expire_soon_period_in_days) <
            now()
            THEN 'Expires soon'
        WHEN f.expiry::date > now()
            THEN 'Not expired'
        ELSE NULL
        end                                                              as file_expiry_status,
    reporting_dr_ver.file_verification_status,
    reporting_dr_ver.verified_by,
    reporting_dr_ver.verified_at,
    row_number()
    OVER (partition by df.organisation_id, df.document_id
        ORDER BY f.create_timestamp desc)                                AS create_desc
from {{ source('public', 'job_position') }} jp
join {{ ref('reporting_dr_visible') }} vv
    on jp.id = vv.jp_id 
    and vv.is_visible is True
join {{ source('public', 'personnel_types') }} pt on jp.role_id = pt.personnel_type_id
join {{ source('public', 'organisation') }} o on pt.organisation_id = o.id
join {{ source('public', 'employee') }} e ON jp.employee_id = e.id
join {{ source('public', 'document') }} d ON e.id = d.employee_id
join {{ source('public', 'document_files_xref') }} df
    on d.id = df.document_id 
    and o.id = df.organisation_id
join {{ source('public', 'file') }} f on f.id = df.file_id
join {{ source('public', 'document_type') }} dt on d.type_id = dt.id
left join {{ ref('reporting_dr_ver') }} reporting_dr_ver
    on reporting_dr_ver.file_id = df.file_id
    and reporting_dr_ver.organisation_id = df.organisation_id