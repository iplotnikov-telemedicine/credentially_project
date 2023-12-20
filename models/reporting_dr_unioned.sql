SELECT reporting_dr_all_dts.jp_id,
        reporting_dr_all_dts.document_type_id,
        reporting_dr_all_dts.document_id,
        reporting_dr_all_dts.compliance_package_id,
        'Document'::text                        AS record_type,
        case dt.name when 'Other' then d.other_type_name
            else coalesce(dtr.name, dt.name) end AS record_name,
        null::text                              as integration_type_short,
        reporting_dr_all_dts.is_mandatory,
        reporting_dr_all_dts.compliance_status,
        reporting_dr_all_dts.marked_done_by_jp_id
FROM {{ ref('reporting_dr_all_dts') }}
JOIN {{ source('public', 'document_type') }} dt ON reporting_dr_all_dts.document_type_id = dt.id
JOIN {{ source('public', 'job_position') }} jp ON reporting_dr_all_dts.jp_id = jp.id
JOIN {{ source('public', 'personnel_types') }} pt ON jp.role_id = pt.personnel_type_id
LEFT JOIN {{ source('public', 'document') }} d ON d.id = reporting_dr_all_dts.document_id
LEFT JOIN {{ source('public', 'document_type_requirements') }} dtr
    ON dt.id = dtr.document_type_id and pt.organisation_id = dtr.organisation_id

union all

SELECT reporting_dr_mandatory_integrations.jp_id,
        reporting_dr_mandatory_integrations.document_type_id,
        null::integer as document_id,
        reporting_dr_mandatory_integrations.compliance_package_id,
        reporting_dr_mandatory_integrations.record_type,
        reporting_dr_mandatory_integrations.record_name,
        reporting_dr_mandatory_integrations.integration_type_short,
        reporting_dr_mandatory_integrations.is_mandatory,
        reporting_dr_mandatory_integrations.compliance_status,
        reporting_dr_mandatory_integrations.marked_done_by_jp_id
FROM {{ ref('reporting_dr_mandatory_integrations') }}
where record_name is not null

union all

SELECT reporting_dr_mandatory_reference.jp_id,
        reporting_dr_mandatory_reference.document_type_id,
        null::integer as document_id,
        reporting_dr_mandatory_reference.compliance_package_id,
        reporting_dr_mandatory_reference.record_type,
        reporting_dr_mandatory_reference.record_name,
        null::text    as integration_type_short,
        reporting_dr_mandatory_reference.is_mandatory,
        reporting_dr_mandatory_reference.compliance_status,
        reporting_dr_mandatory_reference.marked_done_by_jp_id
FROM {{ ref('reporting_dr_mandatory_reference') }}

union all

SELECT reporting_dr_other_crs_temp.jp_id,
        null::bigint as document_type_id,
        null::bigint as document_id,
        reporting_dr_other_crs_temp.compliance_package_id,
        reporting_dr_other_crs_temp.record_type,
        reporting_dr_other_crs_temp.record_name,
        null::text   as integration_type_short,
        reporting_dr_other_crs_temp.is_mandatory,
        reporting_dr_other_crs_temp.compliance_status,
        reporting_dr_other_crs_temp.marked_done_by_jp_id
from {{ ref('reporting_dr_other_crs_temp') }}