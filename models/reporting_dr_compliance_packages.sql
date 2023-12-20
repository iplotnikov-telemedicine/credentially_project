SELECT cpa.job_position_id                                           AS jp_id,
        cpa.compliance_package_id                                     AS "Compliance Package ID",
        crp.name                                                      AS "Compliance Package",
        CASE
            WHEN cpa.compliance_package_id IS NOT NULL AND
                is_package_customized(cpa.compliance_package_assignment_id)
                THEN 'Customized'
            ELSE 'Default'
            END                                                       AS "Package Type",
        COUNT(CASE cra.compliance_status WHEN 'COMPLIANT' THEN 1 END) AS "Compliant Requirement Count",
        COUNT(cra.compliance_status)                                  AS "Total Requirement Count",
        (COUNT(CASE cra.compliance_status WHEN 'COMPLIANT' THEN 1 END) * 100)::integer /
        COUNT(cra.compliance_status)                                  AS "Package Compliance Percentage"
FROM {{ source('public', 'compliance_requirement_assignment_v2') }} cra
JOIN {{ source('public', 'compliance_group_assignment_v2') }} cga
    ON cga.compliance_group_assignment_id = cra.compliance_group_assignment_id
JOIN {{ source('public', 'compliance_package_assignment_v2') }} cpa
    ON cga.compliance_package_assignment_id = cpa.compliance_package_assignment_id
JOIN {{ source('public', 'compliance_requirement_package') }} crp
    ON cpa.compliance_package_id = crp.compliance_requirement_package_id
GROUP BY 1, 2, 3, 4