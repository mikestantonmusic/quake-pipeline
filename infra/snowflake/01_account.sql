-- Part 1: compute, cost guard, database, schemas and roles.
-- Run once in a Snowsight worksheet as ACCOUNTADMIN. Safe to re-run (IF NOT EXISTS).

use role accountadmin;

-- Compute: the smallest size, off after 60 s idle, wakes up on the next query.
create warehouse if not exists quake_wh
    warehouse_size = 'XSMALL'
    auto_suspend = 60
    auto_resume = true
    initially_suspended = true
    statement_timeout_in_seconds = 600;   -- kill any runaway query after 10 minutes

-- Cost guard: 4 credits (~$8) a month for this warehouse. Warn at 75%, suspend at 100%,
-- and cancel running queries at 110%. Serverless features are not covered by this.
create resource monitor if not exists quake_monthly
    with credit_quota = 4
    frequency = monthly
    start_timestamp = immediately
    triggers
        on 75 percent do notify
        on 100 percent do suspend
        on 110 percent do suspend_immediate;

alter warehouse quake_wh set resource_monitor = quake_monthly;

-- Storage: same three layers as the Postgres version.
create database if not exists quake;
create schema if not exists quake.raw;
create schema if not exists quake.staging;
create schema if not exists quake.marts;

-- Roles. pipeline_role loads and transforms; reporter_role only reads the marts.
create role if not exists pipeline_role;
create role if not exists reporter_role;
-- Attach both under SYSADMIN, so admins (including you) can use what they create.
grant role pipeline_role to role sysadmin;
grant role reporter_role to role sysadmin;

grant usage on warehouse quake_wh to role pipeline_role;
grant usage on warehouse quake_wh to role reporter_role;

grant usage on database quake to role pipeline_role;
grant ownership on schema quake.raw to role pipeline_role copy current grants;
grant ownership on schema quake.staging to role pipeline_role copy current grants;
grant ownership on schema quake.marts to role pipeline_role copy current grants;

grant usage on database quake to role reporter_role;
grant usage on schema quake.marts to role reporter_role;
grant select on all tables in schema quake.marts to role reporter_role;
grant select on all views in schema quake.marts to role reporter_role;
-- Tables dbt creates later are readable too.
grant select on future tables in schema quake.marts to role reporter_role;
grant select on future views in schema quake.marts to role reporter_role;

-- Check: should list QUAKE_WH (suspended) with resource monitor QUAKE_MONTHLY.
show warehouses like 'QUAKE_WH';
