select 
    jpvl.jp_id,
    'PERFORMERS_LIST'                      as integration_type_short,
    'Check'                                as "Record Type",
    'NHS England National Performers List' as "Record Name"
from {{ ref('reporting_dr_job_position_view_limited') }} jpvl
join {{ source('public', 'performers_list') }} p 
    on jpvl.professional_registration_number = p.code
where jpvl.professional_registration_number is not null