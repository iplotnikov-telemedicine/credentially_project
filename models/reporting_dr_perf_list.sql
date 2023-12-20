select 
    jp.id                                  as jp_id,
    'PERFORMERS_LIST'                      as integration_type_short,
    'Check'                                as "Record Type",
    'NHS England National Performers List' as "Record Name"
from {{ source('public', 'job_position') }} jp
join {{ source('public', 'employee') }} e 
    on jp.employee_id = e.id
join {{ source('public', 'performers_list') }} p 
    on e.registration_number = p.code
where e.registration_number is not null