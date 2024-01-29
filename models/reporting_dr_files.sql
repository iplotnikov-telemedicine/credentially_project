select 
    jpvl.organisation_id as org_id,
    jpvl.employee_email as email,
    jpvl.jp_id,
    d.id as document_id,
    f.id as file_id,
    dt.id as document_type_id,
    dt.name,
    f.issued,
    f.expiry,
    f.create_timestamp,
    row_number() OVER (partition by df.organisation_id, df.document_id ORDER BY f.id) AS file_version,
    CASE
        WHEN f.expiry is null THEN 'Not expired'
        WHEN f.expiry::date < now() THEN 'Expired'
        WHEN (f.expiry::date - interval '1 day' * dt.expire_soon_period_in_days) < now() THEN 'Expires soon'
        WHEN f.expiry::date > now() THEN 'Not expired'
        ELSE NULL
        END                                                              as file_expiry_status,
    reporting_dr_ver.file_verification_status,
    reporting_dr_ver.verified_by,
    reporting_dr_ver.verified_at,
    row_number() OVER (
        partition by df.organisation_id, df.document_id
        order by f.create_timestamp desc
    )                                AS create_desc
from {{ ref('reporting_dr_job_position_view_limited') }} jpvl
join {{ ref('reporting_dr_visible') }} vv
    on jpvl.jp_id = vv.jp_id and vv.is_visible_wo_archived is True
join {{ source('public', 'document') }} d ON jpvl.employee_id = d.employee_id
join {{ source('public', 'document_files_xref') }} df
    on d.id = df.document_id
    and jpvl.organisation_id = df.organisation_id
join {{ source('public', 'file') }} f on f.id = df.file_id
join {{ source('public', 'document_type') }} dt on d.type_id = dt.id
left join {{ ref('reporting_dr_ver') }} reporting_dr_ver
    on reporting_dr_ver.file_id = df.file_id
    and reporting_dr_ver.organisation_id = df.organisation_id