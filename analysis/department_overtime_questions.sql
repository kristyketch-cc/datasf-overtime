-- Answers the two department questions from the latest complete fiscal year.
-- Run in a Snowflake worksheet after dbt build.

use role accountadmin;
use warehouse transforming_wh;

-- Which departments have the most overtime?
select
    department_code,
    department_name,
    employee_count,
    round(total_overtime_pay)                       as total_overtime_pay,
    round(100 * overtime_share_of_regular_pay, 1)   as overtime_pct_of_regular_pay
from analytics_dev.analytics.department_overtime_summary
where year = (select max(year) from analytics_dev.analytics.department_overtime_summary)
order by total_overtime_pay desc
limit 10;

-- Which have the most once size is taken into account?
-- Departments under 50 employees are left out, because one or two people
-- can swing a small department's average.
select
    department_code,
    department_name,
    employee_count,
    round(overtime_per_employee)                    as overtime_per_employee,
    round(100 * overtime_share_of_regular_pay, 1)   as overtime_pct_of_regular_pay
from analytics_dev.analytics.department_overtime_summary
where year = (select max(year) from analytics_dev.analytics.department_overtime_summary)
  and employee_count >= 50
order by overtime_share_of_regular_pay desc
limit 10;
