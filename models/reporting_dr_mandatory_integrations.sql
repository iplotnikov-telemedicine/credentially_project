SELECT 
    m.jp_id,
    m.compliance_package_id,
    m.document_type_id,
    'Check' as record_type,
    m.integration_type as integration_type_short,
    CASE
        WHEN m.integration_type = 'DBS' THEN 'DBS Update Service Check'
        WHEN m.integration_type = 'GDC' THEN 'General Dental Council (GDC)'
        WHEN m.integration_type = 'GOC' THEN 'General Optical Council (GOC)'
        WHEN m.integration_type = 'GOSC' THEN 'General Osteopathic Council (GOSC)'
        WHEN m.integration_type = 'GPHC' THEN 'General Pharmaceutical Council (GPHC)'
        WHEN m.integration_type = 'PERFORMERS_LIST' THEN 'NHS England National Performers List'
        WHEN m.integration_type = 'PSNI' THEN 'Pharmaceutical Society of Northern Ireland Council (PSNI)'
        WHEN m.integration_type = 'RTW' THEN 'Right to Work Check'
        WHEN m.integration_type = 'SWE' THEN 'Social Work England (SWE)'
        WHEN m.integration_type = 'GCC' THEN 'The General Chiropractic Council (GCC)'
        WHEN m.integration_type = 'NMC' THEN 'The Nursing and Midwifery Council (NMC)'
        WHEN m.integration_type = 'GMC' THEN 'UK General Medical Council (GMC)'
        WHEN m.integration_type = 'HCPC' THEN 'UK Health & Care Professionals Council (HCPC)'
        WHEN m.integration_type = 'USA_NURSYS' THEN 'Nursys e-Notify'
        ELSE null END                as record_name,
    m.is_mandatory,
    m.compliance_status,
    m.marked_done_by_jp_id
FROM {{ ref('reporting_dr_mandatory') }} m
WHERE m.requirement_type = 'INTEGRATION'