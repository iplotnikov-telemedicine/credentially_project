SELECT 
    all_dts.jp_id,
    all_dts.document_type_id,
    all_dts.document_id,
    all_dts.compliance_package_id,
    'Document'::text                            AS record_type,
    case dt.name
        when 'Other' then d.other_type_name
        else coalesce(dtr.name, dt.name) end    AS record_name,
    null::text                                  AS integration_type_short,
    all_dts.is_mandatory,
    all_dts.compliance_status,
    all_dts.marked_done_by_jp_id
FROM {{ ref('reporting_dr_all_dts') }} all_dts
left join {{ source('public', 'document') }} d 
    ON d.id = all_dts.document_id
JOIN {{ source('public', 'document_type') }} dt 
    ON all_dts.document_type_id = dt.id
JOIN {{ ref('reporting_dr_job_position_view_limited') }} jpvl
    ON all_dts.jp_id = jpvl.jp_id
LEFT JOIN {{ source('public', 'document_type_requirements') }} dtr
    ON dt.id = dtr.document_type_id
    and jpvl.organisation_id = dtr.organisation_id

union all

SELECT 
    i.jp_id,
    i.document_type_id,
    null::integer as document_id,
    i.compliance_package_id,
    i.record_type,
    i.record_name,
    i.integration_type_short,
    i.is_mandatory,
    i.compliance_status,
    i.marked_done_by_jp_id
FROM {{ ref('reporting_dr_mandatory_integrations') }} i
where i.record_name is not null

union all

SELECT 
    r.jp_id,
    r.document_type_id,
    null::integer as document_id,
    r.compliance_package_id,
    r.record_type,
    r.record_name,
    null::text as integration_type_short,
    r.is_mandatory,
    r.compliance_status,
    r.marked_done_by_jp_id
FROM {{ ref('reporting_dr_mandatory_reference') }} r

union all

SELECT 
    o.jp_id,
    null::bigint as document_type_id,
    null::bigint as document_id,
    o.compliance_package_id,
    o.record_type,
    o.record_name,
    null::text as integration_type_short,
    o.is_mandatory,
    o.compliance_status,
    o.marked_done_by_jp_id
from {{ ref('reporting_dr_other_crs_temp') }} o