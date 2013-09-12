#!/bin/bash

export PGHOST='/tmp'
export PGUSER='postgres'
export PGDATABASE='ises'

psql -t -A -c "
select case when c.relkind = 'r'
             then 'table '
             when c.relkind = 'i'
             then 'index '
        end
        || c.relname
   from pg_class c
   join pg_namespace n
     on c.relnamespace = n.oid
  where n.nspname = 'auditlog'
    and c.reltablespace = 0
    and c.relname ~ 'tb_audit_event_\d{8}'
    and c.relkind in ('r','i');" | 
while read relation; do
    echo "Moving $relation..."
    psql -c "alter $relation set tablespace audit_log;" || break
done
