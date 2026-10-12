-- One-time Snowflake setup for the whole pipeline.
-- Run it all in a worksheet as ACCOUNTADMIN. Safe to run more than once.
--
-- Creates two areas, each with its own role, warehouse and service user:
--   RAW            where the loader writes. Only the loader and dbt can see it,
--                  because it holds unmasked names next to pay.
--   ANALYTICS_DEV  where dbt builds while developing.
--   ANALYTICS      where dbt builds for the dashboard.

use role accountadmin;

-- ---------------------------------------------------------------- warehouses
-- Smallest size, and they switch off after a minute of no use.
create warehouse if not exists loading_wh
  warehouse_size = xsmall auto_suspend = 60 auto_resume = true initially_suspended = true;
create warehouse if not exists transforming_wh
  warehouse_size = xsmall auto_suspend = 60 auto_resume = true initially_suspended = true;

-- ---------------------------------------------------------------- raw data
create database if not exists raw;
create schema if not exists raw.controller;
use schema raw.controller;

-- One row per source row per load. Each row is kept as the JSON the source
-- sent, so a new or renamed source column cannot break the load.
create table if not exists employee_compensation (
  _batch_id            varchar       not null,
  _loaded_at           timestamp_tz  not null default current_timestamp(),
  _source_published_at timestamp_tz  not null,
  _source_file         varchar       not null,
  record               variant       not null
);

-- One row per run of the loader, including runs that found nothing new.
create table if not exists load_audit (
  batch_id             varchar       not null,
  started_at           timestamp_tz  not null,
  finished_at          timestamp_tz,
  status               varchar       not null,   -- SUCCESS, SKIPPED_NO_NEW_DATA or FAILED
  source_published_at  timestamp_tz,
  source_row_count     number,
  rows_loaded          number,
  source_file          varchar,
  error_message        varchar
);

-- Where the loader uploads its file before copying it into the table.
create stage if not exists employee_compensation_stage file_format = (type = json);

-- ---------------------------------------------------------------- loader
create role if not exists loader;
grant role loader to role sysadmin;
grant usage on warehouse loading_wh to role loader;
grant usage on database raw to role loader;
grant usage on schema raw.controller to role loader;
grant select, insert on table employee_compensation to role loader;
grant select, insert on table load_audit to role loader;
grant read, write on stage employee_compensation_stage to role loader;

create user if not exists svc_loader
  type = service default_role = loader default_warehouse = loading_wh;
grant role loader to user svc_loader;

-- ---------------------------------------------------------------- dbt
create database if not exists analytics_dev;
create database if not exists analytics;

create role if not exists transformer;
grant role transformer to role sysadmin;
grant usage on warehouse transforming_wh to role transformer;
grant usage on database raw to role transformer;
grant usage on schema raw.controller to role transformer;
grant select on all tables in schema raw.controller to role transformer;
grant select on future tables in schema raw.controller to role transformer;
grant usage, create schema on database analytics_dev to role transformer;
grant usage, create schema on database analytics to role transformer;

create user if not exists svc_dbt
  type = service default_role = transformer default_warehouse = transforming_wh;
grant role transformer to user svc_dbt;

-- Both users sign in with a key pair. Attach each public key with:
--   alter user svc_loader set rsa_public_key = '<public key>';
--   alter user svc_dbt    set rsa_public_key = '<public key>';
-- (README, "Setting up", shows how to make the keys.)

-- Your account identifier, for SNOWFLAKE_ACCOUNT in .env:
select current_organization_name() || '-' || current_account_name() as snowflake_account;
