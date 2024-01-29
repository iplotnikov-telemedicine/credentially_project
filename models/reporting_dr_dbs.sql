select 
    distinct on (jpvl.jp_id) 
    jpvl.jp_id,
    'DBS'                      as integration_type_short,
    'Check'                    as "Record Type",
    'DBS Update Service Check' as "Record Name"
from {{ source('public', 'dbs_check') }} dc
join {{ ref('reporting_dr_job_position_view_limited') }} jpvl
    on dc.employee_id = jpvl.employee_id
    and dc.organisation_id = jpvl.organisation_id
order by jpvl.jp_id, dc.print_date desc