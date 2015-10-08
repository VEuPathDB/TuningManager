--
-- createTuningManager.sql
--
-- create the persistent tables used for housekeeping by the tuning manager
--
-- usage:
-- sqlplus <user>/<password>@<instance> [ schema. ]
--
-- the optional argument specifies the name of the schema in which the tables
-- should be created. (The default is the login username; "user" and "schema"
-- are somewhat interchangeable in Oracle.)

create table &1 TuningTable (
   name          varchar2(65) primary key,
   timestamp     date not null,
   definition    clob not null,
   status        varchar2(20),
   last_check    date,
   check_os_user varchar2(20)
  );

grant select on &1 TuningTable to gus_r;
grant insert, update, delete on &1 TuningTable to gus_w;

create table &1 ObsoleteTuningTable (
   name      varchar2(65) primary key,
   timestamp date not null);

grant select on &1 ObsoleteTuningTable to gus_r;
grant insert, update, delete on &1 ObsoleteTuningTable to gus_w;

create sequence &1 TuningManager_sq
   start with 1111;

grant select on &1 TuningManager_sq to gus_w;

create table &1 TuningMgrExternalDependency (
   name         varchar2(65) primary key,
   max_mod_date date,
   timestamp    date not null,
   row_count    number not null);

grant select on &1 TuningMgrExternalDependency to gus_r;
grant insert, update, delete on &1 TuningMgrExternalDependency to gus_w;

create table &1 InstanceMetaInfo as
select sys_context ('USERENV', 'SERVICE_NAME') as instance_nickname,
       cast(null as varchar2(50)) as current_updater,
       cast(null as date) as update_start,
       cast(null as varchar2(20)) as project_id,
       cast(null as varchar2(12)) as version
from dual;

grant select on &1 InstanceMetaInfo to gus_r;
grant insert, update, delete on &1 InstanceMetaInfo to gus_w;

exit
