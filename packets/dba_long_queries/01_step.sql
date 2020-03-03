select
    datname,
    state,
    substring(query from 0 for 32) as query,
    pid,
    backend_xid,
    case when now() - state_change >= '00:00:00.000001'
        then to_char(now() - state_change, 'HH12:MI:SS.MS')
        else '00:00:00'
    end as state_change_age,
    case when now() - xact_start >= '00:00:00.000001'
        then to_char(now() - xact_start, 'HH12:MI:SS.MS')
        else '00:00:00'
    end as xact_start_age,
    application_name as app_name,
    wait_event_type,
    wait_event
from pg_stat_activity
where state in ('active', 'idle in transaction') and pid <> pg_backend_pid()
order by state_change asc
limit 20;