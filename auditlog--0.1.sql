-- BEFORE RUNNING THIS SCRIPT:
-- 1. Create the audit_log schema in your database
-- 2. Make sure the following lines appear at the bottom of postgresql.conf.
--    Customize the last three values to match your database configuration.
--    When you have added these lines, be sure to reload postgres.
--
--    custom_variable_classes = 'audit_log'
--    audit_log.uid = -1
--    audit_log.last_txid = 0
--    audit_log.user_table = 'tb_entity'
--    audit_log.user_table_uid_col = 'entity'
--    audit_log.user_table_email_col = 'email_address'
--    audit_log.user_table_username_col = 'username'
--    audit_log.archive_tablespace = 'pg_default'

-- create or replace language plperl;
-- create or replace language plpgsql;

-- Just a command to check that audit_log schema exsits.
-- Application of the script should fail if this fails.
alter schema audit_log owner to postgres;

------------------------
------ FUNCTIONS ------
------------------------

CREATE OR REPLACE FUNCTION audit_log.fn_set_audit_uid
(
    in_uid   integer
)
returns integer as
 $_$
begin
    return set_config('audit_log.uid', in_uid::varchar, false);
end;
 $_$
    language plpgsql strict;


CREATE OR REPLACE FUNCTION audit_log.fn_get_audit_uid() returns integer as
 $_$
declare
    my_uid    integer;
begin
    my_uid := current_setting('audit_log.uid');

    if my_uid >= 0 then return my_uid; end if;

    select audit_log.fn_get_audit_uid_by_username(current_user::varchar)
      into my_uid;

    return audit_log.fn_set_audit_uid( coalesce( my_uid, 0 ) );
end
 $_$
    language plpgsql stable;



CREATE OR REPLACE FUNCTION audit_log.fn_set_last_audit_txid
(
    in_txid   bigint default txid_current()
)
returns bigint as
 $_$
begin
    return set_config('audit_log.last_txid', in_txid::varchar, false);
end
 $_$
    language plpgsql strict;


CREATE OR REPLACE FUNCTION audit_log.fn_get_last_audit_txid()
returns bigint as
 $_$
begin
    return nullif(current_setting('audit_log.last_txid'), '0');
end
 $_$
    language plpgsql stable;

CREATE OR REPLACE FUNCTION audit_log.fn_get_email_by_audit_uid
(
    in_uid  integer
)
returns varchar as
 $_$
declare
    my_email    varchar;
    my_query    varchar;
begin
    my_query := 'select ' || current_setting('audit_log.user_table_email_col')
             || '  from ' || current_setting('audit_log.user_table')
             || ' where ' || current_setting('audit_log.user_table_uid_col')
                          || ' = ' || in_uid;
    execute my_query
       into my_email;

    return my_email;
end
 $_$
    language plpgsql stable strict;

CREATE OR REPLACE FUNCTION audit_log.fn_get_audit_uid_by_username
(
    in_username varchar
)
returns integer as
 $_$
declare
    my_uid      varchar;
    my_query    varchar;
begin
    my_query := 'select ' || current_setting('audit_log.user_table_uid_col')
             || '  from ' || current_setting('audit_log.user_table')
             || ' where ' || current_setting('audit_log.user_table_username_col')
                          || ' = ''' || in_username || '''';
    execute my_query
       into my_uid;

    return my_uid;
end
 $_$
    language plpgsql stable strict;


CREATE OR REPLACE FUNCTION audit_log.fn_get_all_table_columns()
returns table
(
    table_name      varchar,
    column_name     varchar,
    data_type       varchar
) as
 $_$
begin
    return query
    with tt_tables_with_pk_col (table_name) as
    (
        select distinct tc.table_name
          from information_schema.table_constraints tc
          join information_schema.tables t
            on t.table_schema = tc.table_schema
               and t.table_name = tc.table_name
               and t.table_type::text = 'BASE TABLE'
         where tc.constraint_type = 'PRIMARY KEY'
           and tc.table_schema = 'public'
           and tc.table_name not like 'tb_audit_%'
    )
    select c.table_name::varchar, c.column_name::varchar, c.udt_name::varchar
      from information_schema.columns c
      join tt_tables_with_pk_col using(table_name)
     where c.table_schema = 'public'
     order by c.table_name, c.column_name;

    return;
end
 $_$
    language plpgsql stable;


CREATE OR REPLACE FUNCTION audit_log.fn_get_column_data_type
(
    in_table_name   varchar,
    in_column_name  varchar
)
returns varchar as
 $_$
declare
    my_data_type    varchar;
begin
    select t.typname::information_schema.sql_identifier
      into my_data_type
      from pg_attribute a
      join pg_class c on a.attrelid = c.oid
      join pg_namespace n on c.relnamespace = n.oid
      join pg_type t on a.atttypid = t.oid
     where c.relname::varchar = in_table_name
       and a.attname = in_column_name
       and n.nspname::varchar = 'public';

    return my_data_type;
end
 $_$
    language 'plpgsql' stable strict;


CREATE OR REPLACE FUNCTION audit_log.fn_get_table_pk_col
(
    in_table_name   varchar
)
returns varchar as
 $_$
declare
    my_pk_col   varchar;
begin
    select a.attname
      into strict my_pk_col
      from pg_attribute a
      join pg_constraint cn
        on a.attrelid = cn.conrelid
       and a.attnum = any(cn.conkey)
       and cn.contype = 'p'
       and array_length(cn.conkey, 1) = 1
       and a.attrelid::regclass::varchar = in_table_name;

    return my_pk_col;
exception
    when too_many_rows or no_data_found then 
        return null;
end
 $_$
    language 'plpgsql' stable strict;



CREATE OR REPLACE FUNCTION audit_log.fn_get_or_create_audit_transaction_type
(
    in_label    varchar
)
returns integer as
 $_$
declare
    my_audit_transaction_type   integer;
begin
    select audit_transaction_type
      into my_audit_transaction_type
      from audit_log.tb_audit_transaction_type
     where label = in_label;

    if not found then
        my_audit_transaction_type := nextval('sq_pk_audit_transaction_type');

        insert into audit_log.tb_audit_transaction_type
                    (
                        audit_transaction_type,
                        label
                    )
                    values
                    (
                        my_audit_transaction_type,
                        in_label
                    );
    end if;

    return my_audit_transaction_type;
end
 $_$
    language 'plpgsql' strict;

        
-- fn_label_audit_transaction
CREATE OR REPLACE FUNCTION audit_log.fn_label_audit_transaction
(
    in_label    varchar,
    in_txid     bigint default txid_current()
)
returns bigint as
 $_$
declare
    my_audit_transaction_type   integer;
begin
    select audit_log.fn_get_or_create_audit_transaction_type(in_label)
      into my_audit_transaction_type;

    update tb_audit_event_current
       set audit_transaction_type = my_audit_transaction_type
     where txid = in_txid;

    return in_txid;
end
 $_$
    language 'plpgsql' strict;



-- fn_label_last_audit_transaction
CREATE OR REPLACE FUNCTION audit_log.fn_label_last_audit_transaction
(
    in_label    varchar
)
returns bigint as
 $_$
begin
    return audit_log.fn_label_audit_transaction(in_label, fn_get_last_audit_txid());
end
 $_$
    language 'plpgsql' strict;

-- fn_undo_transaction
CREATE OR REPLACE FUNCTION audit_log.fn_undo_transaction
(
    in_txid   bigint
)
returns setof varchar as
 $_$
declare
    my_statement    varchar;
begin
    for my_statement in
           select (case 
                  when ae.row_op = 'D' then
                       'INSERT INTO ' || af.table_name || ' ('
                       || array_to_string(
                            array_agg('"'||af.column_name||'"'),
                          ',') || ') values ('
                       || array_to_string(
                            array_agg(coalesce(
                                quote_literal(ae.old_value), 'NULL'
                            )),
                          ',') ||')'
                  when ae.row_op = 'U' then
                       'UPDATE ' || af.table_name || ' set '
                       || array_to_string(array_agg(
                            af.column_name||' = '||coalesce(
                                quote_literal(ae.old_value), 'NULL'
                            )
                          ), ', ') || ' where ' || afpk.column_name || ' = ' 
                       || quote_literal(ae.row_pk_val)
                  when ae.row_op = 'I' then
                       'DELETE FROM ' || af.table_name || ' where ' 
                       || afpk.column_name ||' = '||quote_literal(ae.row_pk_val)
                  end)::varchar as query
             from audit_log.tb_audit_event ae
             join audit_log.tb_audit_field af using(audit_field)
             join audit_log.tb_audit_field afpk on af.table_pk = afpk.audit_field
            where ae.txid = in_txid
         group by af.table_name, ae.row_op, afpk.column_name, 
                  ae.row_pk_val
         order by ae.recorded desc
    loop
        execute my_statement;
        return next my_statement;
    end loop;

    perform audit_log.fn_label_audit_transaction('Undo transaction');

    return;
end
 $_$
    language 'plpgsql' strict;


-- fn_undo_last_transaction
CREATE OR REPLACE FUNCTION audit_log.fn_undo_last_transaction()
returns setof varchar as
 $_$
begin
    return query select audit_log.fn_undo_transaction(fn_get_last_audit_txid());
end
 $_$
    language 'plpgsql';



-- fn_get_or_create_audit_data_type
CREATE OR REPLACE FUNCTION audit_log.fn_get_or_create_audit_data_type
(
    in_type_name    varchar
)
returns integer as
 $_$
declare
    my_audit_data_type   integer;
begin
    select audit_data_type
      into my_audit_data_type
      from audit_log.tb_audit_data_type
     where name = in_type_name;

    if not found then
        my_audit_data_type := nextval('sq_pk_audit_data_type');

        insert into audit_log.tb_audit_data_type( audit_data_type, name ) 
            values( my_audit_data_type, in_type_name );
    end if;

    return my_audit_data_type;
end
 $_$
    language 'plpgsql' strict;



-- fn_get_or_create_audit_field
CREATE OR REPLACE FUNCTION audit_log.fn_get_or_create_audit_field
(
    in_table_name   varchar,
    in_column_name  varchar
)
returns integer as
 $_$
declare
    my_audit_field   integer;
    my_active        boolean;
begin
    select audit_field
      into my_audit_field
      from audit_log.tb_audit_field
     where table_name = in_table_name
       and column_name = in_column_name;

    if not found then
        perform *
           from audit_log.tb_audit_field
          where table_name = in_table_name
          limit 1;

        if found then
            perform *
               from audit_log.tb_audit_field
              where table_name = in_table_name
                and active = true
              limit 1;

            if found then
                my_active = true;
            else
                my_active = false;
            end if;
        else
            my_active = true;
        end if;
          
        insert into audit_log.tb_audit_field
        (
            table_name,
            column_name,
            active
        )
        values
        (
            in_table_name, 
            in_column_name,
            my_active
        )
        returning audit_field
        into my_audit_field;
    end if;

    return my_audit_field;
end
 $_$
    language 'plpgsql';
        


-- fn_new_audit_event
CREATE OR REPLACE FUNCTION audit_log.fn_new_audit_event
(
    in_audit_field      integer, -- FK into tb_audit_field
    in_row_pk_val       integer, -- value of primary key of this row
    in_recorded         timestamp, -- clock timestamp of row op
    in_row_op           varchar, -- 'I', 'U', or 'D'
    in_old_value        anyelement, -- old value or null if INSERT
    in_new_value        anyelement  -- new value or null if DELETE
)
returns void as
 $_$
begin
    if (in_row_op = 'UPDATE' and 
        in_old_value::text is not distinct from in_new_value::text) OR
       (in_row_op = 'INSERT' and 
        in_new_value is null)
    then
        return;
    end if;

    insert into audit_log.tb_audit_event
    (
        audit_field,
        row_pk_val,
        recorded,
        row_op,
        old_value,
        new_value
    )
    values
    (
        in_audit_field,
        in_row_pk_val,
        in_recorded,
        in_row_op::char(1),
        case when in_row_op = 'INSERT' then null else in_old_value end,
        case when in_row_op = 'DELETE' then null else in_new_value end
    );
end
 $_$
    language 'plpgsql';


-- fn_drop_audit_event_log_trigger
CREATE OR REPLACE FUNCTION audit_log.fn_drop_audit_event_log_trigger 
(
    in_table_name   varchar
)
returns void as
 $_$
declare
    my_trigger_name     varchar;
    my_function_name    varchar;
    my_trigger_table    varchar;
begin
    my_function_name := 'fn_log_audit_event_'||in_table_name;

    set client_min_messages to warning;
    execute 'drop function if exists '
         || 'audit_log.'||my_function_name||'() cascade';
    set client_min_messages to notice;
end
 $_$
    language 'plpgsql';



-- fn_update_audit_event_log_trigger_on_table
CREATE OR REPLACE FUNCTION audit_log.fn_update_audit_event_log_trigger_on_table
(
    in_table_name   varchar
)
returns void as
 $_$
use strict;

my $table_name = $_[0];

return if $table_name =~ 'tb_audit_.*';

my $colnames_q = "select audit_field, column_name "
               . "  from audit_log.tb_audit_field "
               . " where table_name = '$table_name' "
               . "   and active = true ";

my $colnames_rv = spi_exec_query($colnames_q);

if( $colnames_rv->{'processed'} == 0 )
{
    my $q = "select audit_log.fn_drop_audit_event_log_trigger('$table_name')";
    eval{ spi_exec_query($q) };
    return;
}

my $pk_q = "select audit_log.fn_get_table_pk_col('$table_name') as pk_col ";

my $pk_rv = spi_exec_query($pk_q);

my $pk_col = $pk_rv->{'rows'}[0]{'pk_col'};

unless( $pk_col )
{
    elog(NOTICE, 'pk_col is null');
    return;
}

my $fn_q = "CREATE OR REPLACE FUNCTION "
         . "    audit_log.fn_log_audit_event_$table_name()\n"
         . "returns trigger as \n"
         . " \$_\$\n"
         . "-- THIS FUNCTION AUTOMATICALLY GENERATED. DO NOT EDIT\n"
         . "declare\n"
         . "    my_row_pk_val       integer;\n"
         . "    my_old_row          record;\n"
         . "    my_new_row          record;\n"
         . "    my_recorded         timestamp;\n"
         . "begin\n"
         . "    perform audit_log.fn_set_last_audit_txid();\n"
         . "    \n"
         . "    my_recorded := clock_timestamp();\n"
         . "    \n"
         . "    if( TG_OP = 'INSERT' ) then\n"
         . "        my_row_pk_val := NEW.$pk_col;\n"
         . "    else\n"
         . "        my_row_pk_val := OLD.$pk_col;\n"
         . "    end if;\n\n"
         . "    if( TG_OP = 'DELETE' ) then\n"
         . "        my_new_row := OLD;\n"
         . "    else\n"
         . "        my_new_row := NEW;\n"
         . "    end if;\n\n"
         . "    if( TG_OP = 'INSERT' ) then\n"
         . "        my_old_row := NEW;\n"
         . "    else\n"
         . "        my_old_row := OLD;\n"
         . "    end if;\n\n";

foreach my $row (@{$colnames_rv->{'rows'}})
{
    my $column_name = $row->{'column_name'};
    my $audit_field = $row->{'audit_field'};

    $fn_q .= "    IF (TG_OP = 'INSERT' AND\n"
          .  "        my_new_row.$column_name IS NOT NULL) OR\n"
          .  "       (TG_OP = 'UPDATE' AND\n"
          .  "        my_new_row.${column_name}::text IS DISTINCT FROM\n"
          .  "        my_old_row.${column_name}::text) OR\n"
          .  "       (TG_OP = 'DELETE')\n"
          .  "    THEN\n"
          .  "        perform audit_log.fn_new_audit_event(\n "
          .  "                    $audit_field,\n"
          .  "                    my_row_pk_val,\n"
          .  "                    my_recorded,\n";
          .  "                    TG_OP,\n"
          .  "                    my_old_row.$column_name,\n"
          .  "                    my_new_row.$column_name\n"
          .  "                );\n"
          .  "    END IF;\n\n";
}

$fn_q .= "    return NEW; \n"
      .  "EXCEPTION\n"
      .  "    WHEN undefined_function THEN\n"
      .  "         raise notice 'undefined function';\n"
      .  "         return NEW;\n"
      .  "    WHEN invalid_column_reference THEN\n"
      .  "         raise notice 'invalid column reference';\n"
      .  "         return NEW;\n"
      .  "END\n"
      .  " \$_\$ \n"
      .  "    language 'plpgsql'; ";

elog(NOTICE, $fn_q);
eval { spi_exec_query($fn_q) };

my $tg_q = "CREATE TRIGGER tr_log_audit_event_$table_name "
         . "   after insert or update or delete on $table_name for each row "
         . "   execute procedure audit_log.fn_log_audit_event_$table_name()";
elog(NOTICE, $tg_q);
eval { spi_exec_query($tg_q) };
 $_$
    language 'plperl';



-- fn_update_audit_fields
CREATE OR REPLACE FUNCTION audit_log.fn_update_audit_fields() returns void as
 $_$
begin
    with tt_audit_fields as
    (
        select coalesce(
                   af.audit_field,
                   audit_log.fn_get_or_create_audit_field(
                       a.attrelid::regclass::varchar,
                       a.attname::varchar
                   )
               ) as audit_field,
               (a.attrelid is null and af.active) as stale
          from pg_attribute a
          join pg_constraint cn
            on cn.conrelid = a.attrelid
           and cn.contype = 'p'
           and array_length(cn.conkey, 1) = 1
           and a.attnum > 0
           and a.attisdropped is false
          join pg_namespace n
            on cn.connamespace = n.oid
           and n.nspname::varchar = 'public'
     full join audit_log.tb_audit_field af
            on a.attrelid::regclass::varchar = af.table_name
           and a.attname::varchar = af.column_name
    )
    update audit_log.tb_audit_field af
       set active = false
      from tt_audit_fields afs
     where afs.stale
       and afs.audit_field = af.audit_field;
end
 $_$
    language 'plpgsql';


--------- Audit event archiving -----------

create or replace function audit_log.fn_redirect_audit_events() 
returns trigger as
 $_$
begin
    insert into audit_log.tb_audit_event_current select NEW.*;
    return null;
end
 $_$
    language 'plpgsql';

-- fn_rotate_audit_events
CREATE OR REPLACE FUNCTION audit_log.fn_rotate_audit_events() returns void as
 $_$
declare
    my_min_recorded timestamp;
    my_max_recorded timestamp;
    my_min_txid     bigint;
    my_max_txid     bigint;
    my_table_name   varchar;
    my_query        varchar;
begin
    if (select count(1) from audit_log.tb_audit_event_current) = 0 then
        raise exception 'No events to rotate';
    end if;

    -- make a name for the archive table
    my_table_name := 'tb_audit_event_' || to_char(now(), 'YYYYMMDD_HH24MI');

    -- drop constraint
    alter table tb_audit_event_current 
        drop constraint if exists tb_audit_event_current_recorded_check;

    -- rename (archive) table
    execute 'alter table audit_log.tb_audit_event_current rename to '
         || my_table_name;

    -- create new current table
    create table audit_log.tb_audit_event_current() 
        inherits ( audit_log.tb_audit_event );

    -- add check constraint to new current table
    execute 'alter table audit_log.tb_audit_event_current '
         || 'add constraint tb_audit_event_current_recorded_check '
         || 'check(recorded >= '''||now()::timestamp::text ||''')';

    -- rename indexes on archived table
    execute 'alter index audit_log.tb_audit_event_current_txid_idx rename to '
            ||my_table_name||'_txid_idx';
    execute 'alter index audit_log.tb_audit_event_current_recorded_idx rename to '
            ||my_table_name||'_recorded_idx';
    execute 'alter index audit_log.tb_audit_event_current_audit_field_idx rename to '
            ||my_table_name||'_audit_field_idx';

    -- create indexes on new table
    create index tb_audit_event_current_txid_idx
        on audit_log.tb_audit_event_current(txid);
    create index tb_audit_event_current_recorded_idx
        on audit_log.tb_audit_event_current(recorded);
    create index tb_audit_event_current_audit_field_idx
        on audit_log.tb_audit_event_current(audit_field);

    -- get mins & maxes for creating check constraints
    execute 'select max(recorded), min(recorded), '
         || '       min(txid), max(txid) '
         || '  from audit_log.'||my_table_name
       into my_max_recorded, my_min_recorded,
            my_min_txid, my_max_txid;

    -- add check constraints to archived table
    execute 'alter table audit_log.' || my_table_name || ' add check(recorded between '''
         || my_min_recorded || ''' and ''' || my_max_recorded || ''')';
    execute 'alter table audit_log.' || my_table_name
         || '  add check(txid between '''
         ||     my_min_txid || ''' and ''' || my_max_txid || ''')';

    -- move table to appropriate tablespace
    execute 'alter table audit_log.' || my_table_name
         || ' set tablespace ' || current_setting('audit_log.archive_tablespace');
end
 $_$
    language 'plpgsql';


------------------
----- TABLES -----
------------------

-- tb_audit_data_type
create sequence audit_log.sq_pk_audit_data_type;

CREATE TABLE IF NOT EXISTS audit_log.tb_audit_data_type
(
    audit_data_type integer primary key
                    default nextval('sq_pk_audit_data_type'),
    name            varchar not null unique
);

alter sequence audit_log.sq_pk_audit_data_type
    owned by audit_log.tb_audit_data_type.audit_data_type;




-- tb_audit_field
create sequence audit_log.sq_pk_audit_field;

CREATE TABLE IF NOT EXISTS audit_log.tb_audit_field
(
    audit_field     integer primary key default nextval('sq_pk_audit_field'),
    table_name      varchar,
    column_name     varchar,
    audit_data_type integer not null references audit_log.tb_audit_data_type,   
    table_pk        integer not null references audit_log.tb_audit_field,
    active          boolean not null default true,
    CONSTRAINT tb_audit_field_table_column_key UNIQUE(table_name,column_name),
    CONSTRAINT tb_audit_field_tb_audit_event_not_allowed 
        CHECK( table_name not like 'tb_audit_event%' )
);

alter sequence audit_log.sq_pk_audit_field
    owned by audit_log.tb_audit_field.audit_field;


-- tb_audit_transaction_type
CREATE SEQUENCE audit_log.sq_pk_audit_transaction_type;

CREATE TABLE IF NOT EXISTS audit_log.tb_audit_transaction_type
(
    audit_transaction_type  integer primary key
                            default nextval('sq_pk_audit_transaction_type'),
    label                   varchar unique
);

ALTER SEQUENCE sq_pk_audit_transaction_type
    owned by audit_log.tb_audit_transaction_type.audit_transaction_type;

-- tb_audit_event
CREATE TABLE IF NOT EXISTS audit_log.tb_audit_event
(
    audit_field             integer not null 
                            references audit_log.tb_audit_field,
    row_pk_val              integer not null,
    recorded                timestamp not null,
    uid                     integer not null,
    row_op                  char(1) not null CHECK (row_op in ('I','U','D')),
    txid                    bigint not null default txid_current(),
    pid                     integer not null default pg_backend_pid(),
    audit_transaction_type  integer 
                            references audit_log.tb_audit_transaction_type,
    old_value               text,
    new_value               text
);

ALTER TABLE audit_log.tb_audit_event 
    alter column uid set default audit_log.fn_get_audit_uid();

-- tb_audit_event_current
CREATE TABLE IF NOT EXISTS audit_log.tb_audit_event_current() 
    inherits ( audit_log.tb_audit_event );

drop index if exists audit_log.tb_audit_event_current_txid_idx;
drop index if exists audit_log.tb_audit_event_current_recorded_idx;
drop index if exists audit_log.tb_audit_event_current_audit_field_idx;

create index tb_audit_event_current_txid_idx
    on audit_log.tb_audit_event_current(txid);
create index tb_audit_event_current_recorded_idx
    on audit_log.tb_audit_event_current(recorded);
create index tb_audit_event_current_audit_field_idx
    on audit_log.tb_audit_event_current(audit_field);

drop trigger if exists tr_redirect_audit_events on audit_log.tb_audit_event;
create trigger tr_redirect_audit_events 
    before insert on audit_log.tb_audit_event
    for each row execute procedure audit_log.fn_redirect_audit_events();



--------------------
------ VIEWS -------
--------------------

-- vw_audit_log
CREATE OR REPLACE VIEW audit_log.vw_audit_log as
   select ae.recorded, 
          ae.uid, 
          audit_log.fn_get_email_by_audit_uid(ae.uid) as user_email,
          ae.txid, 
          att.label as description,
          af.table_name,
          af.column_name,
          ae.row_pk_val as pk_val,
          ae.row_op as op,
          ae.old_value,
          ae.new_value
     from audit_log.tb_audit_event ae
     join audit_log.tb_audit_field af using(audit_field)
left join audit_log.tb_audit_transaction_type att using(audit_transaction_type)
 order by ae.recorded desc, af.table_name, af.column_name;


-- vw_audit_transaction_statement
CREATE OR REPLACE VIEW audit_log.vw_audit_transaction_statement as
   select ae.txid, 
          ae.recorded,
          audit_log.fn_get_email_by_audit_uid(ae.uid) as user_email,
          att.label as description, 
          (case 
          when ae.row_op = 'I' then
               'INSERT INTO ' || af.table_name || ' ('
               || array_to_string(array_agg('"'||af.column_name||'"'), ',') 
               || ') values ('
               || array_to_string(array_agg(coalesce(
                    quote_literal(ae.new_value)||'::'||adt.name, 'NULL'
                  )), ',') ||');'
          when ae.row_op = 'U' then
               'UPDATE ' || af.table_name || ' set '
               || array_to_string(array_agg(af.column_name||' = '||coalesce(
                    quote_literal(ae.new_value)||'::'||adt.name, 'NULL'
                  )), ', ') || ' where ' || afpk.column_name || ' = ' 
               || quote_literal(ae.row_pk_val) || '::' || adtpk.name || ';'
          when ae.row_op = 'D' then
               'DELETE FROM ' || af.table_name || ' where ' || afpk.column_name
               ||' = '||quote_literal(ae.row_pk_val)||'::'||adtpk.name||';'
          end)::varchar as query
     from audit_log.tb_audit_event ae
     join audit_log.tb_audit_field af using(audit_field)
     join audit_log.tb_audit_data_type adt using(audit_data_type)
     join audit_log.tb_audit_field afpk on af.table_pk = afpk.audit_field
     join audit_log.tb_audit_data_type adtpk 
       on afpk.audit_data_type = adtpk.audit_data_type
left join audit_log.tb_audit_transaction_type att using(audit_transaction_type)
 group by af.table_name, ae.row_op, afpk.column_name,
          ae.row_pk_val, adtpk.name, ae.txid, ae.recorded,
          att.label, audit_log.fn_get_email_by_audit_uid(ae.uid)
 order by ae.recorded;


-----------------------
------ TRIGGERS -------
-----------------------

-- fn_audit_event_log_trigger_updater
CREATE OR REPLACE FUNCTION audit_log.fn_audit_event_log_trigger_updater()
returns trigger as
 $_$
declare
    my_table_name   varchar;
begin
    if TG_OP = 'DELETE' then
        my_table_name := OLD.table_name;
    else
        my_table_name := NEW.table_name;
    end if;

    perform audit_log.fn_update_audit_event_log_trigger_on_table(my_table_name);
    return new;
end
 $_$
    language 'plpgsql';


drop trigger if exists tr_audit_event_log_trigger_updater
    on audit_log.tb_audit_field;

CREATE TRIGGER tr_audit_event_log_trigger_updater
    AFTER INSERT OR UPDATE OR DELETE on audit_log.tb_audit_field
    FOR EACH ROW EXECUTE PROCEDURE audit_log.fn_audit_event_log_trigger_updater();


-- fn_check_audit_field_validity
CREATE OR REPLACE FUNCTION audit_log.fn_check_audit_field_validity() 
returns trigger as
 $_$
declare
    my_pk_col           varchar;
    my_audit_data_type  integer;
begin
    if TG_OP = 'UPDATE' then
        if NEW.table_name  != OLD.table_name or
           NEW.column_name != OLD.column_name
        then
            raise exception 'Updating table_name or column_name not allowed.';
        end if;
    end if;

    my_pk_col := fn_get_table_pk_col(NEW.table_name);

    if my_pk_col is null then
        raise exception 'Cannot audit table %: No PK column found',
            NEW.table_name;
    end if;

    if my_pk_col = NEW.column_name then
        NEW.table_pk := NEW.audit_field;
    else
        select audit_log.fn_get_or_create_audit_field(NEW.table_name, my_pk_col)
          into NEW.table_pk;
    end if;

    my_audit_data_type := audit_log.fn_get_or_create_audit_data_type(
        audit_log.fn_get_column_data_type(NEW.table_name, NEW.column_name)
    );

    if my_audit_data_type is not null then
        NEW.audit_data_type := my_audit_data_type;
    else
        if TG_OP = 'INSERT' then
            raise exception 'Invalid audit field %.%', 
                NEW.table_name, NEW.column_name;
        end if;
    end if;

    return NEW;
end
 $_$
    language plpgsql;

drop trigger if exists tr_check_audit_field_validity
    on audit_log.tb_audit_field;

CREATE TRIGGER tr_check_audit_field_validity
    BEFORE INSERT OR UPDATE ON audit_log.tb_audit_field
    FOR EACH ROW EXECUTE PROCEDURE audit_log.fn_check_audit_field_validity();

select audit_log.fn_update_audit_fields();


--- COMPATIBILITY
create or replace function fn_expire_procpid_entities() returns void as
 $_$
 $_$
language sql;

create or replace function fn_set_procpid_entity
(
    in_entity   integer
)
returns integer as
 $_$
    select audit_log.fn_set_audit_uid($1);
 $_$
language sql;

create or replace function fn_get_procpid_entity() returns integer as
 $_$
    select audit_log.fn_get_audit_uid();
 $_$
language sql;


--- PERMISSIONS

grant usage on schema audit_log to public;

grant usage on all sequences in schema audit_log to public;

grant select on all tables in schema audit_log to public;

grant insert on audit_log.tb_audit_event, 
                audit_log.tb_audit_event_current,
                audit_log.tb_audit_transaction_type 
        to public;

grant update (audit_transaction_type) 
    on audit_log.tb_audit_event_current to public;

revoke select on audit_log.tb_audit_event from public;

-- create role audit_log with password 'CyanAudit';
grant select on audit_log.tb_audit_event to audit_log, postgres;

