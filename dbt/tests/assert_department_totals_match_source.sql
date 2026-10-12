-- Department totals should add up to the citywide total for every year.
-- Fails if any records were dropped or counted twice on the way.

with department_totals as (
    select year, sum(total_overtime_pay) as overtime, sum(total_regular_pay) as regular_pay
    from {{ ref('department_overtime_summary') }}
    group by year
),

source_totals as (
    select year, sum(overtime) as overtime, sum(salaries) as regular_pay
    from {{ ref('int_compensation_fiscal_years') }}
    group by year
)

select s.year, s.overtime, d.overtime as department_overtime, s.regular_pay, d.regular_pay as department_regular_pay
from source_totals s
left join department_totals d
    on s.year = d.year
where d.year is null
   or abs(s.overtime - d.overtime) > 1
   or abs(s.regular_pay - d.regular_pay) > 1
