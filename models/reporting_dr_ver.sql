select
    distinct on (v.file_id, v.organisation_id)
    v.file_id,
    v.organisation_id,
    initcap(verification_type)                              as file_verification_status,
    jpvl.full_name_with_role                                as verified_by,
    v.create_timestamp                                      as verified_at
from {{ source('public', 'verification') }} v
join {{ ref('reporting_dr_job_position_view_limited') }} jpvl
    on v.verifier_id = jpvl.employee_id
order by v.organisation_id, v.file_id, v.create_timestamp desc