select 
    jp.id                      as jp_id,
    e.registration_number_type as integration_type_short,
    'Check'                    as "Record Type",
    case
        WHEN e.registration_number_type = 'GCC'
            THEN 'The General Chiropractic Council (GCC)'
        WHEN e.registration_number_type = 'GDC'
            THEN 'General Dental Council (GDC)'
        WHEN e.registration_number_type = 'GMC'
            THEN 'UK General Medical Council (GMC)'
        WHEN e.registration_number_type = 'GOC'
            THEN 'General Optical Council (GOC)'
        WHEN e.registration_number_type = 'GOSC'
            THEN 'General Osteopathic Council (GOSC)'
        WHEN e.registration_number_type = 'GPHC'
            THEN 'General Pharmaceutical Council (GPHC)'
        WHEN e.registration_number_type = 'HCPC'
            THEN 'UK Health & Care Professionals Council (HCPC)'
        WHEN e.registration_number_type = 'NMC'
            THEN 'The Nursing and Midwifery Council (NMC)'
        WHEN e.registration_number_type = 'PSNI'
            THEN 'Pharmaceutical Society of Northern Ireland Council (PSNI)'
        WHEN e.registration_number_type = 'SWE'
            THEN 'Social Work England (SWE)'
        WHEN e.registration_number_type = 'USA_NURSYS'
            THEN 'Nursys e-Notify'
        WHEN e.registration_number_type is not null
            THEN 'Other'
        ELSE null end          as "Record Name"
from {{ source('public', 'job_position') }} jp
    join {{ source('public', 'employee') }} e on jp.employee_id = e.id
where not e.registration_number_type is null
    and e.registration_number_type <> 'UNKNOWN'