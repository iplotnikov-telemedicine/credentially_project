with a as (
    select cpa.job_position_id as jp_id,
            MD5(
                CONCAT(
                    CASE cra.type
                        WHEN 'INTEGRATION' THEN cra.integration_type
                        WHEN 'REFERENCE_FORM' THEN CAST(cra.reference_form_id AS TEXT)
                        WHEN 'DOCUMENT_TYPE' THEN CAST(cra.document_type_id AS TEXT)
                        WHEN 'TEXT_REQUIREMENT' THEN CAST(cra.text_requirement_id AS TEXT)
                    END,
                    cra.type
                )
            )                   AS surrogate_key,
            cra.compliance_status
    FROM {{ source('public', 'compliance_requirement_assignment_v2') }} cra
    JOIN {{ source('public', 'compliance_group_assignment_v2') }} cga
        ON cga.compliance_group_assignment_id = cra.compliance_group_assignment_id
    JOIN {{ source('public', 'compliance_package_assignment_v2') }} cpa
        ON cga.compliance_package_assignment_id = cpa.compliance_package_assignment_id
    JOIN {{ source('public', 'compliance_requirement_package') }} crp
        ON cpa.compliance_package_id = crp.compliance_requirement_package_id
    LEFT JOIN {{ source('public', 'text_requirement') }} tr 
        ON cra.text_requirement_id = tr.text_requirement_id
    LEFT JOIN {{ source('public', 'document_type') }} dt 
        ON dt.id = cra.document_type_id
    LEFT JOIN {{ source('public', 'reference_form') }} rf 
        ON rf.reference_form_id = cra.reference_form_id
)
select 
    jp_id,
    count(distinct
        case compliance_status
            when 'COMPLIANT'
                then surrogate_key end) as "Compliant Records Count",
    count(distinct surrogate_key)         as "Total Records Count",
    floor(count(
            distinct
            case compliance_status
                when 'COMPLIANT'
                    then surrogate_key end)::decimal
            /
        count(distinct surrogate_key)::decimal *
        100)::integer                   as "Compliance Percentage"
FROM a
group by 1