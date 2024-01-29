select 
    jpvl.jp_id,
    jpvl.professional_registration_type as integration_type_short,
    'Check'                    as "Record Type",
    case
        WHEN jpvl.professional_registration_type = 'GCC' THEN 'The General Chiropractic Council (GCC)'
        WHEN jpvl.professional_registration_type = 'GDC' THEN 'General Dental Council (GDC)'
        WHEN jpvl.professional_registration_type = 'GMC' THEN 'UK General Medical Council (GMC)'
        WHEN jpvl.professional_registration_type = 'GOC' THEN 'General Optical Council (GOC)'
        WHEN jpvl.professional_registration_type = 'GOSC' THEN 'General Osteopathic Council (GOSC)'
        WHEN jpvl.professional_registration_type = 'GPHC' THEN 'General Pharmaceutical Council (GPHC)'
        WHEN jpvl.professional_registration_type = 'HCPC' THEN 'UK Health & Care Professionals Council (HCPC)'
        WHEN jpvl.professional_registration_type = 'NMC' THEN 'The Nursing and Midwifery Council (NMC)'
        WHEN jpvl.professional_registration_type = 'PSNI' THEN 'Pharmaceutical Society of Northern Ireland Council (PSNI)'
        WHEN jpvl.professional_registration_type = 'SWE' THEN 'Social Work England (SWE)'
        WHEN jpvl.professional_registration_type = 'USA_NURSYS' THEN 'Nursys e-Notify'
        WHEN jpvl.professional_registration_type is not null THEN 'Other'
        ELSE null end          as "Record Name"
from {{ ref('reporting_dr_job_position_view_limited') }} jpvl
where not jpvl.professional_registration_type is null
    and jpvl.professional_registration_type <> 'UNKNOWN'