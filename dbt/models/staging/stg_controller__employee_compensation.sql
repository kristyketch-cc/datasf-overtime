-- One row per source row, from the most recent load.
-- Renames, casts and tidies. Drops no rows and changes no values.

with source as (

    select *
    from {{ source('controller', 'employee_compensation') }}
    -- Each load is a full copy of the dataset, so reading more than one
    -- would double-count. Failed loads are never committed, so the newest
    -- one is always complete.
    where _batch_id = (
        select max(_batch_id)
        from {{ source('controller', 'employee_compensation') }}
    )

),

extracted as (

    -- Pull each field out of the JSON as text. Text is trimmed and blanks
    -- become null. A field the source left empty is absent from the JSON
    -- and also comes back as null.
    select
        nullif(trim(record:year_type::string), '')               as year_type,
        nullif(trim(record:year::string), '')                    as year,
        nullif(trim(record:organization_group_code::string), '') as organization_group_code,
        nullif(trim(record:organization_group::string), '')      as organization_group,
        nullif(trim(record:department_code::string), '')         as department_code,
        nullif(trim(record:department::string), '')              as department,
        nullif(trim(record:job_family_code::string), '')         as job_family_code,
        nullif(trim(record:job_family::string), '')              as job_family,
        nullif(trim(record:job_code::string), '')                as job_code,
        nullif(trim(record:job::string), '')                     as job,
        nullif(trim(record:union_code::string), '')              as union_code,
        nullif(trim(record:"union"::string), '')                 as union_name,
        nullif(trim(record:employee_identifier::string), '')     as employee_identifier,
        nullif(trim(record:employment_type::string), '')         as employment_type,
        record:salaries::string                                  as salaries,
        record:overtime::string                                  as overtime,
        record:other_salaries::string                            as other_salaries,
        record:total_salary::string                              as total_salary,
        record:retirement::string                                as retirement,
        record:health_and_dental::string                         as health_and_dental,
        record:other_benefits::string                            as other_benefits,
        record:total_benefits::string                            as total_benefits,
        record:total_compensation::string                        as total_compensation,
        record:hours::string                                     as hours,
        record:data_as_of::string                                as data_as_of,
        _batch_id,
        _loaded_at
    from source

)

select
    -- The source has no row key, and name + job + department + year is not
    -- unique. This id hashes the columns that together identify a row. It
    -- identifies a row, not a person.
    {{ dbt_utils.generate_surrogate_key([
        'year_type', 'year', 'employee_identifier', 'department_code',
        'job_code', 'union_code', 'hours', 'salaries', 'overtime',
        'other_salaries', 'total_benefits', 'total_compensation'
    ]) }}                                        as compensation_id,

    year_type,
    try_to_number(year)::int                     as year,

    -- Codes stay text: union code 038 would lose its zero as a number, and
    -- job family Q000 is not a number at all. The names beside them are
    -- kept as the source spelled them that year.
    organization_group_code,
    organization_group,
    department_code,
    department,
    job_family_code,
    job_family,
    job_code,
    job,
    union_code,
    union_name,          -- source column "union", a reserved word in SQL
    employment_type,

    -- A name from 2017 on, a number before that. Sensitive.
    employee_identifier,

    -- Dollars, as fixed two-decimal numbers so sums do not pick up float
    -- rounding. Negative values are payroll corrections and are kept.
    try_to_decimal(salaries, 18, 2)              as salaries,
    try_to_decimal(overtime, 18, 2)              as overtime,
    try_to_decimal(other_salaries, 18, 2)        as other_salaries,
    try_to_decimal(total_salary, 18, 2)          as total_salary,
    try_to_decimal(retirement, 18, 2)            as retirement,
    try_to_decimal(health_and_dental, 18, 2)     as health_and_dental,
    try_to_decimal(other_benefits, 18, 2)        as other_benefits,
    try_to_decimal(total_benefits, 18, 2)        as total_benefits,
    try_to_decimal(total_compensation, 18, 2)    as total_compensation,
    try_to_decimal(hours, 18, 2)                 as hours,

    try_to_timestamp_ntz(data_as_of)             as data_as_of,
    _batch_id,
    _loaded_at
from extracted
