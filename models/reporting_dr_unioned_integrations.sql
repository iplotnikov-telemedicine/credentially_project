select *
from {{ ref('reporting_dr_prof_reg') }}

union all

select *
from {{ ref('reporting_dr_dbs') }}

union all

select *
from {{ ref('reporting_dr_rtw') }}

union all

select *
from {{ ref('reporting_dr_perf_list') }}