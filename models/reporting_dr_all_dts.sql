SELECT reporting_dr_mandatory_dts.jp_id,
        reporting_dr_mandatory_dts.compliance_package_id,
        reporting_dr_mandatory_dts.document_type_id,
        reporting_dr_mandatory_dts.document_id,
        true as is_mandatory,
        compliance_status,
        marked_done_by_jp_id
FROM {{ ref('reporting_dr_mandatory_dts') }}
union
select reporting_dr_non_mandatory_dts.jp_id,
        null  as compliance_package_id,
        reporting_dr_non_mandatory_dts.document_type_id,
        reporting_dr_non_mandatory_dts.document_id,
        false as is_mandatory,
        null  as compliance_status,
        null  as marked_done_by_jp_id
FROM {{ ref('reporting_dr_non_mandatory_dts') }}