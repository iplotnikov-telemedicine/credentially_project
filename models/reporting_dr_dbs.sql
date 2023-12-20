select distinct on (jp.id) 
    jp.id                      as jp_id,
    'DBS'                      as integration_type_short,
    'Check'                    as "Record Type",
    'DBS Update Service Check' as "Record Name"
from {{ source('public', 'dbs_check') }} dc
join {{ source('public', 'employee') }} e
    on e.id = dc.employee_id
join {{ source('public', 'personnel_types') }} pt
    on dc.organisation_id = pt.organisation_id
join {{ source('public', 'job_position') }} jp
    on e.id = jp.employee_id
    and pt.personnel_type_id = jp.role_id
order by jp.id, dc.print_date desc