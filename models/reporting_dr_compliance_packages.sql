SELECT 
    m.jp_id,
    m.compliance_package_id,
    crp.name                                                      AS "Compliance Package",
    CASE
        WHEN m.compliance_package_id IS NOT NULL AND is_package_customized(m.compliance_package_assignment_id)
            THEN 'Customized'
        ELSE 'Default' END                                        AS "Package Type",
    COUNT(CASE m.compliance_status WHEN 'COMPLIANT' THEN 1 END) AS "Compliant Requirement Count",
    COUNT(m.compliance_status)                                  AS "Total Requirement Count",
    (COUNT(CASE m.compliance_status WHEN 'COMPLIANT' THEN 1 END) * 100)::integer /
    COUNT(m.compliance_status)                                  AS "Package Compliance Percentage"
FROM {{ ref('reporting_dr_mandatory') }} m
JOIN {{ source('public', 'compliance_requirement_package') }} crp
    ON m.compliance_package_id = crp.compliance_requirement_package_id
GROUP BY 1, 2, 3, 4