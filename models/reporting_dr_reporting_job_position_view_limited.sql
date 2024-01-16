WITH rjpv AS (
    SELECT o.id AS organisation_id,
        e.email AS employee_email,
        e.phone AS employee_phone,
        e.birth_date,
        ut.default_label AS title,
        e.first_name AS employee_first_name,
        e.last_name AS employee_last_name,
        concat_ws(' '::text, e.first_name, e.last_name) AS full_name,
        e.registration_number_type AS professional_registration_type,
        e.registration_number AS professional_registration_number,
        e.city AS employee_city,
        e.state,
        e.home_postcode AS postcode,
        e.medical_category,
        e.medical_specialty,
        concat_ws(' '::text, e.doctor_grade, e.other_doctor_grade) AS employee_grade,
        e.doctor_grade,
        e.other_doctor_grade,
        jp.id AS job_position_id,
        jp.status as job_position_status,
        jp.employee_id,
        jp.create_timestamp,
        jp.start_date,
        jp.skip_onboarding,
        jp.status,
        jp.signed_off,
        jp.compliance_status,
        pt.name AS job_position_role,
        pt.description AS job_position_role_description,
        pt.staff_type,
        c.name AS country,
        s.name AS state_name
    FROM {{ source('public', 'organisation') }} o
    JOIN {{ source('public', 'personnel_types') }} pt 
        ON o.id = pt.organisation_id
    JOIN {{ source('public', 'job_position') }} jp 
        ON jp.role_id = pt.personnel_type_id
    JOIN {{ source('public', 'employee') }} e 
        ON e.id = jp.employee_id
    LEFT JOIN {{ source('public', 'user_title') }} ut 
        ON e.title_key = ut.key
    LEFT JOIN {{ source('public', 'country') }} c 
        ON e.country = c.iso2
    LEFT JOIN {{ source('public', 'state') }} s
        on e.state = s.iso2 and e.country = s.country_code
    WHERE jp.status IN ('ACTIVE', 'IMPORTED', 'INVITED', 'ARCHIVED')
), 
tags AS (
    SELECT 
        jt.job_position_id,
        string_agg(t_1.name::text, ','::text) AS tags
    FROM {{ source('public', 'job_position_tags_xref') }} jt
    JOIN {{ source('public', 'tags') }} t_1 ON t_1.tag_id = jt.tag_id
    GROUP BY jt.job_position_id
)
SELECT 
    rjpv.employee_id,
    rjpv.job_position_id,
    rjpv.organisation_id,
    rjpv.job_position_status,
    rjpv.employee_email,
    rjpv.employee_phone,
    rjpv.birth_date,
    rjpv.title,
    rjpv.state,
    rjpv.employee_city,
    rjpv.country,
    rjpv.state_name,
    rjpv.postcode,
    rjpv.medical_specialty,
    rjpv.medical_category,
    rjpv.professional_registration_number,
    rjpv.job_position_role,
    rjpv.job_position_role_description,
    rjpv.doctor_grade,
    rjpv.other_doctor_grade,
    rjpv.signed_off,
    rjpv.staff_type,
    rjpv.skip_onboarding,
    rjpv.compliance_status,
    rjpv.full_name,
    t.tags,
    rjpv.create_timestamp
FROM rjpv
LEFT JOIN tags t 
  ON t.job_position_id = rjpv.job_position_id