SELECT 
    cpav2.job_position_id  as jp_id,
    cpav2.compliance_package_id,
    crav2.document_type_id,
    'Check'::text          AS record_type,
    crav2.integration_type as integration_type_short,
    CASE
        WHEN crav2.integration_type = 'DBS'
            THEN 'DBS Update Service Check'
        WHEN crav2.integration_type = 'GDC'
            THEN 'General Dental Council (GDC)'
        WHEN crav2.integration_type = 'GOC'
            THEN 'General Optical Council (GOC)'
        WHEN crav2.integration_type = 'GOSC'
            THEN 'General Osteopathic Council (GOSC)'
        WHEN crav2.integration_type = 'GPHC'
            THEN 'General Pharmaceutical Council (GPHC)'
        WHEN crav2.integration_type = 'PERFORMERS_LIST'
            THEN 'NHS England National Performers List'
        WHEN crav2.integration_type = 'PSNI'
            THEN 'Pharmaceutical Society of Northern Ireland Council (PSNI)'
        WHEN crav2.integration_type = 'RTW'
            THEN 'Right to Work Check'
        WHEN crav2.integration_type = 'SWE'
            THEN 'Social Work England (SWE)'
        WHEN crav2.integration_type = 'GCC'
            THEN 'The General Chiropractic Council (GCC)'
        WHEN crav2.integration_type = 'NMC'
            THEN 'The Nursing and Midwifery Council (NMC)'
        WHEN crav2.integration_type = 'GMC'
            THEN 'UK General Medical Council (GMC)'
        WHEN crav2.integration_type = 'HCPC'
            THEN 'UK Health & Care Professionals Council (HCPC)'
        WHEN crav2.integration_type = 'USA_NURSYS'
            THEN 'Nursys e-Notify'
        ELSE null
        END                as record_name,
    true                   AS is_mandatory,
    crav2.compliance_status,
    crav2.marked_done_by_jp_id
FROM {{ source('public', 'compliance_requirement_assignment_v2') }} crav2
JOIN {{ source('public', 'compliance_group_assignment_v2') }} cgav2
    ON crav2.compliance_group_assignment_id = cgav2.compliance_group_assignment_id
JOIN {{ source('public', 'compliance_package_assignment_v2') }} cpav2
    ON cgav2.compliance_package_assignment_id = cpav2.compliance_package_assignment_id
WHERE crav2.type::text = 'INTEGRATION'::text