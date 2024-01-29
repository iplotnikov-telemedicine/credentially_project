select 
    m.jp_id,
    count(distinct case m.compliance_status when 'COMPLIANT' then m.surrogate_key end) as "Compliant Records Count",
    count(distinct m.surrogate_key)         as "Total Records Count",
    floor(count(distinct case m.compliance_status when 'COMPLIANT' then m.surrogate_key end)::decimal 
        / count(distinct m.surrogate_key)::decimal * 100)::integer                   as "Compliance Percentage"
FROM {{ ref('reporting_dr_mandatory') }} m
group by 1