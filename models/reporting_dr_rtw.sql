select distinct on (rtws.job_position_id) 
    rtws.job_position_id  as jp_id,
    'RTW'                 as integration_type_short,
    'Check'               as "Record Type",
    'Right to Work Check' as "Record Name"
from {{ source('public', 'file') }} file
inner join {{ source('public', 'document_files_xref') }} dfx
    on file.id = dfx.file_id
inner join {{ source('public', 'right_to_work_document_status_item') }} rtwdsi
    on dfx.document_id = rtwdsi.document_id
inner join {{ source('public', 'right_to_work_status_item') }} rtwsi
    on rtwdsi.right_to_work_status_item_id = rtwsi.right_to_work_status_item_id
inner join {{ source('public', 'right_to_work_status') }} rtws
    on rtwsi.right_to_work_status_id = rtws.right_to_work_status_id
inner join {{ source('public', 'right_to_work_document_configuration_item') }} rtwdci
    on rtwdsi.right_to_work_configuration_item_id = rtwdci.right_to_work_configuration_item_id
inner join {{ source('public', 'right_to_work_configuration_item') }} rtwci
    on rtwdci.right_to_work_configuration_item_id = rtwci.right_to_work_configuration_item_id
inner join {{ source('public', 'right_to_work_configuration') }} rtwc
    on rtwci.right_to_work_configuration_id = rtwc.right_to_work_configuration_id
order by jp_id, file.expiry asc