-- report on slow tuning-table builds
-- looks in the "apidb_r" schema for TuningTableLog
-- aggregates by instance and tuning table

-------------------------------------------------------------------------------
-- first, a function to print a duration in seconds in human-readable form
create or replace function apidb.readable_time (secs number)
return varchar2
is
    timestr varchar2(80);

begin
    timestr := case
                 when secs < .01 then secs
                 when secs < 2 then round(secs, 1)
                 else round(secs)
               end || ' secs'
               || case
                    when round(secs / (60 * 60 * 24 * 365.25), 1) = 1
                      then ' (1 year)'
                    when round(secs / (60 * 60 * 24 * 365.25), 1) > 1
                      then ' (' || round(secs / (60 * 60 * 24 * 365.25), 1) || ' years)'
                    when round(secs / (60 * 60 * 24), 1) = 1
                      then ' (1 day)'
                    when round(secs / (60 * 60 * 24), 1) > 1
                      then ' (' || round(secs / (60 * 60 * 24), 1) || ' days)'
                    when round(secs / (60 * 60), 1) = 1
                      then ' (1 hour)'
                    when round(secs / (60 * 60), 1) > 1
                      then ' (' || round(secs / (60 * 60), 1) || ' hours)'
                    when round(secs / 60, 1) = 1
                      then ' (1 minute)'
                    when round(secs / 60, 1) > 1
                      then ' (' || round(secs / 60, 1) || ' minutes)'
                    else ''
                  end;

    return timestr;

end readable_time;
/

show errors;

GRANT execute ON apidb.readable_time TO public;

-------------------------------------------------------------------------------


-- comment or un-comment the where-clause predicates below to
-- control:
--     - how long into the past
--     - which instance(s)
--     - what table(s)
--     - overall row limit

column instance format a10
column tuning_table format a40
column avg_build_time format a27
column min_build_time format a27
column max_build_time format a27

select tuning_table, instance, number_of_builds as "# BUILDS",
       apidb.readable_time(avg_build_duration) as avg_build_time,
       apidb.readable_time(min_build_duration) as min_build_time,
       apidb.readable_time(max_build_duration) as max_build_time
from (select instance_nickname as instance,  name as tuning_table,
             count(*) as number_of_builds,
             avg(build_duration) as avg_build_duration,
             min(build_duration) as min_build_duration,
             max(build_duration) as max_build_duration
      from apidb_r.TuningTableLog
      where timestamp > sysdate - 20                -- how long (in days) in the past?
     -- and instance_nickname like '%fung%'          -- which instance(s)?
     -- and lower(name) like '%featurelocation%'    -- which tuning table(s)?
        and name not in ('TranscriptLocation', 'BlatProtAlignLocation', 'FeatureLocation') -- recently disused
      group by instance_nickname, name)
where max_build_duration > 300         -- how slow (in seconds) is a too-slow average?
-- and rownum <= 50                      -- limit report to slowest N (instance, table) tuples
order by avg_build_duration desc
;

