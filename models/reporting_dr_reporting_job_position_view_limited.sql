SELECT rjpv.employee_id,
        rjpv.job_position_id,
        rjpv.organisation_id,
        rjpv.job_position_status,
        rjpv.employee_email,
        rjpv.employee_phone,
        rjpv.title,
        rjpv.employee_city,
        rjpv.country,
        rjpv.postcode,
        rjpv.medical_specialty,
        rjpv.medical_category,
        rjpv.doctor_grade,
        rjpv.other_doctor_grade,
        professional_registration_number,
        job_position_role,
        job_position_role_description,
        signed_off,
        staff_type,
        skip_onboarding,
        compliance_status,
        full_name,
        tags,
        create_timestamp
FROM {{ ref('reporting_dr_reporting_job_position_view') }} rjpv
WHERE rjpv.job_position_status IN ('ACTIVE', 'IMPORTED', 'INVITED', 'ARCHIVED')