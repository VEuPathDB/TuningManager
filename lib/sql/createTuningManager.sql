--
-- createTuningManager.sql
--
-- create the persistent tables used for housekeeping by the tuning manager
--
-- usage:
-- psql <user>/<password>@<instance> [ -v SCHEMA_PREFIX=value -v SERVICE_NAME=value ]
--
-- the argument specifies the name of the schema in which the tables
-- should be created. 

create table :SCHEMA_PREFIX.TuningTable (
   name          varchar(65) primary key,
   timestamp     timestamp not null,
   definition    text not null,
   status        varchar(20),
   last_check    timestamp,
   check_os_user varchar(20)
  );

grant select on :SCHEMA_PREFIX.TuningTable to gus_r;
grant insert, update, delete on :SCHEMA_PREFIX.TuningTable to gus_w;

create table :SCHEMA_PREFIX.ObsoleteTuningTable (
   name      varchar2(65) primary key,
   timestamp date not null);

grant select on :SCHEMA_PREFIX.ObsoleteTuningTable to gus_r;
grant insert, update, delete on :SCHEMA_PREFIX.ObsoleteTuningTable to gus_w;

create sequence :SCHEMA_PREFIX.TuningManager_sq
   start with 1111;

grant select on :SCHEMA_PREFIX.TuningManager_sq to gus_w;

create table :SCHEMA_PREFIX.TuningMgrExternalDependency (
   name         varchar2(65) primary key,
   max_mod_date date,
   timestamp    date not null,
   row_count    number not null);

grant select on :SCHEMA_PREFIX.TuningMgrExternalDependency to gus_r;
grant insert, update, delete on :SCHEMA_PREFIX.TuningMgrExternalDependency to gus_w;
-- Limit portal_dblink to TuningMgrExternalDependency, https://redmine.apidb.org/issues/28093
-- grant select, insert, update, delete on :SCHEMA_PREFIX.TuningMgrExternalDependency to portal_dblink;

create table :SCHEMA_PREFIX.InstanceMetaInfo as
select ':SERVICE_NAME' as instance_nickname,
       cast(null as varchar(50)) as current_updater,
       cast(null as date) as update_start,
       cast(null as varchar(20)) as project_id,
       cast(null as varchar(12)) as version
;

grant select on :SCHEMA_PREFIX.InstanceMetaInfo to gus_r;
grant insert, update, delete on :SCHEMA_PREFIX.InstanceMetaInfo to gus_w;

