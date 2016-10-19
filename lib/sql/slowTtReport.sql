-- report on slow tuning-table builds
-- looks in the "apidb_r" schema for TuningTableLog
-- aggregates by instance and tuning-table

-- comment or un-comment the where-term predicates below to
-- control:
--     - how long into the past
--     - which instance(s)
--     - what table(s)
--     -

column instance format a20
column tuning_table format a40
column avg_build_duration format a30

select tuning_table, instance,
       round(avg_build_duration) || ' sec '
       || case
            when round(avg_build_duration / (60 * 60 * 24), 1) = 1
              then ' (1 day)'
            when round(avg_build_duration / (60 * 60 * 24), 1) > 1
              then ' (' || round(avg_build_duration / (60 * 60 * 24), 1) || ' days)'
            when round(avg_build_duration / (60 * 60), 1) = 1
              then ' (1 hour)'
            when round(avg_build_duration / (60 * 60), 1) > 1
              then ' (' || round(avg_build_duration / (60 * 60), 1) || ' hours)'
            when round(avg_build_duration / 60, 1) = 1
              then ' (1 minute)'
            when round(avg_build_duration / 60, 1) > 1
              then ' (' || round(avg_build_duration / 60, 1) || ' minutes)'
            else ''
          end as avg_build_duration,
       number_of_builds
from (select instance_nickname as instance,  name as tuning_table,
             avg(build_duration) as avg_build_duration, count(*) as number_of_builds
      from apidb_r.TuningTableLog
      where timestamp > sysdate - 90                -- how long (in days) in the past?
--      and instance_nickname like '%rbld'          -- which instance(s)?
--      and lower(name) like '%featurelocation%'    -- which tuning table(s)?
      group by instance_nickname, name
      order by avg(build_duration) desc)
where avg_build_duration > 3600         -- how slow (in seconds) is a too-slow average?
  and rownum <= 50                      -- limit report to slowest N (instance, table) tuples
;

