select 
    onb.job_position_id                                    as jp_id,
    onb.organisation_id                                    as org_id,
    onb.employee_id,
    onb.onboarding_id,
    onb.checklist_id,
    onb.onboarding_version,
    case
        when onb.current_step_name = '(blank)'
            then null
        else onb.current_step_name END                     as "Current step name",
    case
        when onb.current_step_type = '(blank)'
            then null
        ELSE
            replace(CONCAT(
                        UPPER(SUBSTRING(onb.current_step_type, 1, 1)),
                        LOWER(SUBSTRING(
                            onb.current_step_type,
                            2,
                            CHAR_LENGTH(onb.current_step_type) -
                            1))
                    ), '_', ' '
            ) END                                          as "Current step type",
    convert_to_org_timezone(
        onb.organisation_id,
        onb.onboarding_created_at)                         as onboarding_created_at,
    date_trunc('minute',
                convert_to_org_timezone(
                    onb.organisation_id,
                    onb.onboarding_started_at))             as "Onboarding Start Time",
    convert_to_org_timezone(
        onb.organisation_id,
        onb.onboarding_updated_at)                         as onboarding_updated_at,
    date_trunc('minute',
                convert_to_org_timezone(
                    onb.organisation_id,
                    onb.onboarding_completed_at))           as "Onboarding Completion Time",
    round(cast(onb.completion_time / 86400 as numeric), 2) as completion_time_days,
    round(cast(onb.completion_time / 3600 as numeric), 2)  as completion_time_hours,
    replace(CONCAT(
                UPPER(SUBSTRING(onb.onboarding_status, 1, 1)),
                LOWER(SUBSTRING(
                    onb.onboarding_status,
                    2,
                    CHAR_LENGTH(onb.onboarding_status) -
                    1))
            ), '_', ' '
    )                                                      as "Onboarding Status",
    case
        when onb.completed_by = '(blank)'
            then null
        else onb.completed_by END                          as "Onboarding completed by"
from {{ source('public', 'reporting_onboarding_view') }} onb