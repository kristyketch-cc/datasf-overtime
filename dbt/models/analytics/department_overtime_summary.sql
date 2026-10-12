-- Salary and overtime by department, one row per department per fiscal year.
--
-- Answers two questions: which departments have the most overtime
-- (total_overtime_pay), and which have the most once department size is
-- taken into account. For the second, there are two measures:
--   * overtime_per_employee: overtime dollars divided by headcount
--   * overtime_share_of_regular_pay: overtime dollars divided by regular pay
--
-- The second measure is the fairer comparison, because it also adjusts for
-- how well a department is paid. I did not use hours to estimate full-time
-- equivalents, because hours include overtime hours (which would shrink the
-- result for exactly the departments with the most overtime) and hours are
-- missing before 2017.
--
-- Headcount is the number of distinct employee identifiers in the
-- department that year. That counts someone with two jobs in the department
-- once, but also merges different people who share a name, so it is an
-- approximation. The source has no employee id.

with compensation as (

    -- Two records have no department. They are kept as Unknown so that the
    -- department totals add up to the citywide total.
    select
        *,
        coalesce(department_code, 'UNK') as department_key
    from {{ ref('int_compensation_fiscal_years') }}

),

department_names as (

    -- The source spells department names differently in different years, so
    -- each code gets the most recent name it was published under.
    select
        department_key,
        coalesce(department, 'Unknown') as department_name
    from compensation
    qualify row_number() over (partition by department_key order by year desc, department) = 1

),

flags as (

    select
        year,
        coalesce(department_code, 'UNK') as department_key,
        sum(unusually_high_overtime_flag) as flagged_employee_records
    from {{ ref('employee_overtime_flags') }}
    group by 1, 2

),

department_years as (

    select
        year,
        department_key,
        count(distinct employee_identifier) as employee_count,
        sum(salaries)                       as total_regular_pay,
        sum(overtime)                       as total_overtime_pay
    from compensation
    group by 1, 2

)

select
    d.year,
    d.department_key                                         as department_code,
    n.department_name,
    d.employee_count,
    d.total_regular_pay,
    d.total_overtime_pay,
    d.total_overtime_pay / nullif(d.employee_count, 0)       as overtime_per_employee,
    d.total_overtime_pay / nullif(d.total_regular_pay, 0)    as overtime_share_of_regular_pay,
    coalesce(f.flagged_employee_records, 0)                  as flagged_employee_records
from department_years d
left join department_names n
    on d.department_key = n.department_key
left join flags f
    on d.year = f.year
   and d.department_key = f.department_key
