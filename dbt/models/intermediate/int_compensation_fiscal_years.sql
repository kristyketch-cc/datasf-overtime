-- The records the dashboard reports on: complete fiscal years only.
--
-- The source lists every year twice, once as a calendar year and once as a
-- fiscal year, so adding both together would double-count. I use fiscal
-- years because the city budgets and limits overtime by fiscal year. The
-- newest fiscal year is left out until it has ended, because a year in
-- progress is not comparable with full years. A fiscal year ends on 30 June
-- of the year it is named for.
--
-- Both analytics models read from here, so this rule lives in one place.

select *
from {{ ref('stg_controller__employee_compensation') }}
where year_type = 'Fiscal'
  and data_as_of::date > date_from_parts(year, 6, 30)
