-- Flags employee records with unusually high overtime.
--
-- A record is flagged when both are true:
--   * its overtime-to-salary ratio is above Q3 + 3 x IQR for its peer group
--   * its overtime is at least $10,000
--
-- The peer group is everyone with the same job code in the same fiscal
-- year, including people with no overtime. If a job code has fewer than 50
-- records, the peer group widens to the job family, then the organization
-- group, then citywide.
--
-- In jobs where overtime is common, the Q3 + 3 x IQR threshold decides. In
-- jobs where most people have none, the threshold is zero and the $10,000
-- floor decides, which catches one person with large overtime in a job
-- where nobody else has any.
--
-- I use the ratio to salary instead of overtime hours because the source
-- has no overtime-hours column, and the ratio adjusts for pay rate and for
-- part-time schedules. The four thresholds are set once, in settings.

with settings as (

    -- Each of these is a judgment call, so they are kept together here.
    select
        20000 as min_salary,            -- below this, the ratio is not meaningful
        50    as min_peer_group_size,   -- smallest group worth comparing within
        3     as iqr_multiplier,        -- how far above Q3 counts as unusually high
        10000 as min_overtime           -- overtime dollars needed before a record can be flagged

),

eligible as (

    -- Records with a salary too small for the ratio to mean anything are
    -- left out, such as someone paid for only a few weeks.
    select
        compensation_id,
        year,
        year_type,
        organization_group_code,
        organization_group,
        department_code,
        department,
        job_family_code,
        job_family,
        job_code,
        job,
        employee_identifier,
        salaries,
        overtime,
        overtime / salaries as overtime_salary_ratio,
        settings.min_peer_group_size,
        settings.iqr_multiplier,
        settings.min_overtime
    from {{ ref('int_compensation_fiscal_years') }}
    cross join settings
    where salaries >= settings.min_salary

),

stats as (

    -- The size and quartiles of each possible peer group, for every record.
    select
        *,

        count(*) over (partition by year, job_code) as job_code_count,
        percentile_cont(0.25) within group (order by overtime_salary_ratio)
            over (partition by year, job_code) as job_code_q1,
        percentile_cont(0.75) within group (order by overtime_salary_ratio)
            over (partition by year, job_code) as job_code_q3,

        count(*) over (partition by year, job_family_code) as job_family_count,
        percentile_cont(0.25) within group (order by overtime_salary_ratio)
            over (partition by year, job_family_code) as job_family_q1,
        percentile_cont(0.75) within group (order by overtime_salary_ratio)
            over (partition by year, job_family_code) as job_family_q3,

        count(*) over (partition by year, organization_group_code) as org_group_count,
        percentile_cont(0.25) within group (order by overtime_salary_ratio)
            over (partition by year, organization_group_code) as org_group_q1,
        percentile_cont(0.75) within group (order by overtime_salary_ratio)
            over (partition by year, organization_group_code) as org_group_q3,

        count(*) over (partition by year) as citywide_count,
        percentile_cont(0.25) within group (order by overtime_salary_ratio)
            over (partition by year) as citywide_q1,
        percentile_cont(0.75) within group (order by overtime_salary_ratio)
            over (partition by year) as citywide_q3
    from eligible

),

assigned as (

    -- Use the narrowest peer group that is big enough.
    select
        *,
        case
            when job_code_count >= min_peer_group_size then 'job_code'
            when job_family_count >= min_peer_group_size then 'job_family'
            when org_group_count >= min_peer_group_size then 'organization_group'
            else 'citywide'
        end as peer_group_type,
        case
            when job_code_count >= min_peer_group_size then job_code_count
            when job_family_count >= min_peer_group_size then job_family_count
            when org_group_count >= min_peer_group_size then org_group_count
            else citywide_count
        end as peer_group_count,
        case
            when job_code_count >= min_peer_group_size then job_code_q1
            when job_family_count >= min_peer_group_size then job_family_q1
            when org_group_count >= min_peer_group_size then org_group_q1
            else citywide_q1
        end as peer_q1,
        case
            when job_code_count >= min_peer_group_size then job_code_q3
            when job_family_count >= min_peer_group_size then job_family_q3
            when org_group_count >= min_peer_group_size then org_group_q3
            else citywide_q3
        end as peer_q3
    from stats

),

thresholds as (

    select
        *,
        peer_q3 - peer_q1 as peer_iqr,
        peer_q3 + iqr_multiplier * (peer_q3 - peer_q1)
            as high_overtime_ratio_threshold
    from assigned

)

-- Only what someone reviewing a flag needs: who, where, how much, what
-- they were compared with, and the result.
select
    compensation_id,
    year,
    department_code,
    job_code,
    job,
    employee_identifier,
    salaries,
    overtime,
    overtime_salary_ratio,
    peer_group_type,
    peer_group_count,
    high_overtime_ratio_threshold,
    case when overtime_salary_ratio > high_overtime_ratio_threshold
          and overtime >= min_overtime
         then 1 else 0 end as unusually_high_overtime_flag
from thresholds
