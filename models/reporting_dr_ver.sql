select distinct on (v.file_id, v.organisation_id) 
    v.file_id,
    v.organisation_id,
    initcap(verification_type)                                   as file_verification_status,
    e.first_name || ' ' || e.last_name || ' (' || pt.name || ')' as verified_by,
    v.create_timestamp                                           as verified_at
from {{ source('public', 'verification') }} v
join {{ source('public', 'employee') }} e 
    on v.verifier_id = e.id
join {{ source('public', 'job_position') }} jp 
    ON e.id = jp.employee_id
join {{ source('public', 'personnel_types') }} pt 
    on jp.role_id = pt.personnel_type_id
order by v.organisation_id, v.file_id, v.create_timestamp desc