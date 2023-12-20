select *
from {{ ref('reporting_dr_records_wo_non_mandatory_checks') }}

union all

select *
from {{ ref('reporting_dr_non_mandatory_checks') }}