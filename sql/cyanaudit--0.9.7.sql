------------------------
------ FUNCTIONS ------
------------------------

do language plpgsql
 $$
declare
    my_value            varchar;
    my_version          integer[];
    my_command          varchar;
begin
    my_version := regexp_matches(version(), 'PostgreSQL (\d)+\.(\d+)\.(\d+)');

    -- Verify minimum version
    if my_version < array[9,3,3]::integer[] then
        raise exception 'Cyan Audit requires PostgreSQL 9.3.3 or above';
    end if;

    -- Set default values for configuration parameters
    my_command := 'alter database ' || quote_ident(current_database()) || ' set cyanaudit.';
    execute my_command || 'enabled = 1';
    execute my_command || 'uid = -1';
    execute my_command || 'last_txid = 0';
    execute my_command || 'archive_tablespace = pg_default';
    execute my_command || 'user_table = '''' ';
    execute my_command || 'user_table_uid_col = '''' ';
    execute my_command || 'user_table_email_col = '''' ';
    execute my_command || 'user_table_username_col = '''' ';
end;
 $$;


-- fn_set_audit_uid
CREATE OR REPLACE FUNCTION @extschema@.fn_set_audit_uid
(
    in_uid   integer
)
returns integer
language sql strict
as $_$
    select (set_config('cyanaudit.uid', in_uid::varchar, false))::integer;
 $_$;


-- fn_get_audit_uid
CREATE OR REPLACE FUNCTION @extschema@.fn_get_audit_uid() 
returns integer 
language plpgsql stable
as $_$
declare
    my_uid    integer;
begin
    my_uid := coalesce( nullif( current_setting('cyanaudit.uid'), '' )::integer, -1 );

    if my_uid >= 0 then return my_uid; end if;

    select @extschema@.fn_get_audit_uid_by_username(current_user::varchar)
      into my_uid;

    return @extschema@.fn_set_audit_uid( coalesce( my_uid, 0 ) );
exception
    when undefined_object
    then return @extschema@.fn_set_audit_uid( 0 );
end
 $_$;


-- fn_set_last_audit_txid
CREATE OR REPLACE FUNCTION @extschema@.fn_set_last_audit_txid
(
    bigint default txid_current()
)
returns bigint
language sql strict
as $_$
    SELECT (set_config('cyanaudit.last_txid', $1::varchar, false))::bigint;
 $_$;


-- fn_get_last_audit_txid
CREATE OR REPLACE FUNCTION @extschema@.fn_get_last_audit_txid()
returns bigint
language sql stable
as $_$
    SELECT (nullif(current_setting('cyanaudit.last_txid'), '0'))::bigint;
 $_$;


-- fn_get_email_by_audit_uid
CREATE OR REPLACE FUNCTION @extschema@.fn_get_email_by_audit_uid
(
    in_uid  integer
)
returns varchar
language plpgsql stable strict
as $_$
declare
    my_email                varchar;
    my_query                varchar;
    my_user_table_uid_col   varchar;
    my_user_table           varchar;
    my_user_table_email_col varchar;
begin
    select current_setting('cyanaudit.user_table'),
           current_setting('cyanaudit.user_table_uid_col'),
           current_setting('cyanaudit.user_table_email_col')
      into my_user_table,
           my_user_table_uid_col,
           my_user_table_email_col;

    if my_user_table            = '' OR
       my_user_table_uid_col    = '' OR
       my_user_table_email_col  = ''
    then
        return null;
    end if;

    my_query := 'select ' || quote_ident(my_user_table_email_col)
             || '  from ' || quote_ident(my_user_table)
             || ' where ' || quote_ident(my_user_table_uid_col)
                          || ' = ' || quote_nullable(in_uid);
    execute my_query
       into my_email;

    return my_email;
exception
    when undefined_object then 
         -- settings are not defined
         return null;
    when undefined_table then 
         raise notice 'cyanaudit: Invalid user_table setting: ''%''', my_user_table;
         return null;
    when undefined_column then 
         raise notice 'cyanaudit: Invalid user_table_uid_col (''%'') or user_table_email_col (''%'')',
            my_user_table_uid_col, my_user_table_email_col;
         return null;
end
 $_$;


-- fn_get_audit_uid_by_username
CREATE OR REPLACE FUNCTION @extschema@.fn_get_audit_uid_by_username
(
    in_username varchar
)
returns integer
language plpgsql stable strict
as $_$
declare
    my_uid                      varchar;
    my_query                    varchar;
    my_user_table_uid_col       varchar;
    my_user_table               varchar;
    my_user_table_username_col  varchar;
begin
    select current_setting('cyanaudit.user_table'),
           current_setting('cyanaudit.user_table_uid_col'),
           current_setting('cyanaudit.user_table_username_col')
      into my_user_table,
           my_user_table_uid_col,
           my_user_table_username_col;

    if my_user_table                = '' OR
       my_user_table_uid_col        = '' OR
       my_user_table_username_col   = ''
    then
        return null;
    end if;

    my_query := 'select ' || quote_ident(my_user_table_uid_col)
             || '  from ' || quote_ident(my_user_table)
             || ' where ' || quote_ident(my_user_table_username_col)
                          || ' = ' || quote_nullable(in_username);
    execute my_query
       into my_uid;

    return my_uid;
exception
    when undefined_object then 
         return null;
    when undefined_table then 
         raise notice 'cyanaudit: Invalid user_table setting: ''%''', my_user_table;
         return null;
    when undefined_column then 
         raise notice 'cyanaudit: Invalid user_table_uid_col (''%'') or user_table_username_col (''%'')',
            my_user_table_uid_col, my_user_table_username_col;
         return null;
end
 $_$;


-- fn_get_table_pk_col
CREATE OR REPLACE FUNCTION @extschema@.fn_get_table_pk_cols
(
    in_table_name   varchar,
    in_table_schema varchar default 'public'
)
returns varchar[]
language sql stable strict
as $_$
    with tt_conkey as
    (
        SELECT cn.conkey,
               c.oid as relid
          from pg_class c
          join pg_namespace n
            on c.relnamespace = n.oid
          join pg_constraint cn
            on c.oid = cn.conrelid
         where cn.contype = 'p'
           and c.relname::varchar = in_table_name
           and n.nspname::varchar = in_table_schema
    ),
    tt_subscripts as
    (
        select generate_subscripts( conkey, 1 ) as i
          from tt_conkey
    )
    select array_agg( a.attname order by s.i )::varchar[]
      from tt_subscripts s
cross join tt_conkey c
      join pg_attribute a
        on c.conkey[s.i] = a.attnum
       and c.relid = a.attrelid
 $_$;



-- fn_get_or_create_audit_transaction_type
CREATE OR REPLACE FUNCTION @extschema@.fn_get_or_create_audit_transaction_type
(
    in_label    varchar
)
returns integer
language plpgsql strict
as $_$
declare
    my_audit_transaction_type   integer;
begin
    select audit_transaction_type
      into my_audit_transaction_type
      from @extschema@.tb_audit_transaction_type
     where label = in_label;

    if not found then
        insert into @extschema@.tb_audit_transaction_type
        (
            label
        )
        values
        (
            in_label
        )
        returning audit_transaction_type
        into my_audit_transaction_type;
    end if;

    return my_audit_transaction_type;
end
 $_$;

        
-- fn_label_audit_transaction
CREATE OR REPLACE FUNCTION @extschema@.fn_label_audit_transaction
(
    in_label    varchar,
    in_txid     bigint default txid_current()
)
returns bigint 
language plpgsql strict
as $_$
declare
    my_audit_transaction_type   integer;
begin
    select @extschema@.fn_get_or_create_audit_transaction_type(in_label)
      into my_audit_transaction_type;

    update @extschema@.tb_audit_event_current
       set audit_transaction_type = my_audit_transaction_type
     where txid = in_txid
       and audit_transaction_type is null;

    return in_txid;
end
 $_$;



-- fn_label_last_audit_transaction
CREATE OR REPLACE FUNCTION @extschema@.fn_label_last_audit_transaction
(
    in_label    varchar
)
returns bigint
language sql strict
as $_$
    select @extschema@.fn_label_audit_transaction
           (
                in_label, 
                @extschema@.fn_get_last_audit_txid()
           );
 $_$;

-- fn_undo_transaction
CREATE OR REPLACE FUNCTION @extschema@.fn_undo_transaction
(
    in_txid   bigint
)
returns setof varchar
language plpgsql strict
as $_$
declare
    my_statement    varchar;
begin
    for my_statement in
        select query 
          from vw_audit_transaction_statement_inverse
         where txid = in_txid
    loop
        execute my_statement;
        return next my_statement;
    end loop;

    perform @extschema@.fn_label_audit_transaction('Undo transaction');

    return;
end
 $_$;


-- fn_undo_last_transaction
CREATE OR REPLACE FUNCTION @extschema@.fn_undo_last_transaction()
returns setof varchar as
 $_$
    select @extschema@.fn_undo_transaction(@extschema@.fn_get_last_audit_txid());
 $_$
    language 'sql';



-- fn_get_or_create_audit_field
CREATE OR REPLACE FUNCTION @extschema@.fn_get_or_create_audit_field
(
    in_table_name       varchar,
    in_column_name      varchar,
    in_table_schema     varchar default 'public'
)
returns integer as
 $_$
declare
    my_audit_field   integer;
begin
    select audit_field
      into my_audit_field
      from @extschema@.tb_audit_field
     where table_schema = in_table_schema
       and table_name = in_table_name
       and column_name = in_column_name;

    if not found then
        insert into @extschema@.tb_audit_field
        (
            table_schema,
            table_name,
            column_name
        )
        values
        (
            in_table_schema,
            in_table_name, 
            in_column_name
        )
        returning audit_field
        into my_audit_field;
    end if;

    return my_audit_field;
end
 $_$
    language 'plpgsql';
        


-- fn_new_audit_event
CREATE OR REPLACE FUNCTION @extschema@.fn_new_audit_event
(
    in_audit_field      integer, -- FK into tb_audit_field
    in_pk_vals          varchar[], -- value of primary key of this row
    in_row_op           varchar, -- 'INSERT', 'UPDATE', or 'DELETE'
    in_old_value        anyelement, -- old value or null if INSERT
    in_new_value        anyelement  -- new value or null if DELETE
)
returns void
language sql as
 $_$
    insert into @extschema@.tb_audit_event
    (
        audit_field,
        pk_vals,
        row_op,
        old_value,
        new_value
    )
    values
    (
        in_audit_field,
        in_pk_vals,
        in_row_op::char(1),
        case when in_row_op = 'INSERT' then null else in_old_value end,
        case when in_row_op = 'DELETE' then null else in_new_value end
    );
 $_$;


CREATE OR REPLACE FUNCTION @extschema@.fn_log_audit_event()
RETURNS TRIGGER
language plpgsql
AS $_$
DECLARE
    my_pk_cols              varchar;
    my_column_names         varchar[];
    my_audit_fields         varchar[];
    my_column_name          varchar;
    my_audit_field          varchar;
    my_pk_vals              varchar[];
    my_pk_vals_constructor  varchar;
    my_old_row              record;
    my_new_row              record;
    my_row                  record;
    my_do_log               boolean;
BEGIN
    -- We could just munge the string, but part of me feels dirty doing that.
    my_pk_cols      := TG_ARGS[1]::varchar[];
    my_column_names := TG_ARGS[2]::varchar[];
    my_audit_fields := TG_ARGS[3]::varchar[];

    if( TG_OP != 'DELETE' ) then
        my_new_row := NEW;
    end if;

    if( TG_OP != 'INSERT' ) then
        my_old_row := OLD;
    end if;

    my_row := coalsece( NEW, OLD );

    if current_setting('cyanaudit.enabled') = '0' then
        return my_new_row;
    end if;

    perform @extschema@.fn_set_last_audit_txid();

    -- Given this varchar[]:  ARRAY[ 'column foo',bar ]
    -- Generate this varchar: '{ $1."column_foo", $1.bar }::varchar[]'
    -- And then execute it to produce this varchar[]: ARRAY[ 'val1', 'val2' ]
    select '{' || string_agg( '$1.' || quote_ident(pk_col), ',' ) || '}::varchar[]'
      into my_pk_vals_constructor
      from ( select unnest(my_pk_cols) as pk_col ) x;

    execute my_pk_vals_constructor
       into my_pk_vals
      using my_row;

    raise notice 'my_pk_vals = %', my_pk_vals;

    for my_column_name, my_audit_field in
        select unnest( my_column_names ),
               unnest( my_audit_fields )
    loop
        IF TG_OP = 'INSERT' THEN
            execute format('select $1.%I IS NOT NULL', my_column_name)
               into my_do_log
              using my_new_row;
        ELSIF TG_OP = 'UPDATE' THEN
            execute format( 'select $1.%I::text IS DISTINCT FROM $2.%I::text',
                            my_column_name, my_column_name)
               into my_do_log
              using my_new_row, my_old_row;
        ELSIF TG_OP = 'DELETE' THEN
            execute format('select $1.%I IS NOT NULL', my_column_name)
               into my_do_log
              using my_old_row;
        END IF;

        if my_do_log then
            execute '@extschema@.fn_new_audit_event( '
                 || '   $1, '
                 || '   $2, '
                 || '   $3, '
                 || '   $4.' || my_column_name || ', '
                 || '   $5.' || my_column_name || ' )'
              using my_audit_field,
                    my_pk_vals,
                    TG_OP,
                    my_old_row,
                    my_new_row;
        end if;
    end loop;

    return NEW;
EXCEPTION
    WHEN undefined_function THEN
         raise notice 'Undefined function call. Please reinstall cyanaudit.';
         return NEW;
    WHEN undefined_column THEN
         raise notice 'Undefined column. Please run fn_update_audit_fields() as superuser.';
         return NEW;
    WHEN undefined_object THEN
         raise notice 'Cyan Audit configuration invalid. Logging disabled.';
         return NEW;
END
$_$;



-- fn_get_trigger_function_declaration
CREATE OR REPLACE FUNCTION @extschema@.fn_get_trigger_function_declaration
(
    in_function_name    varchar,
    in_pk_cols          varchar[],
    in_audit_fields     varchar[],
    in_column_names     varchar[]
)
returns text
language sql immutable strict
as $_$
 select 'CREATE OR REPLACE FUNCTION @extschema@.' || in_function_name || ' returns trigger '
     || 'language plpgsql '
     || 'as $function$ '
     || 'DECLARE '
     || '    my_pk_vals          varchar[]; '
     || '    my_row              record; '
     || '    my_old_row          record; '
     || '    my_new_row          record; '
     || '    my_recorded         timestamp; '
     || 'BEGIN '
     || '    -- THIS FUNCTION AUTOMATICALLY GENERATED. DO NOT EDIT '
     || '    if( TG_OP = ''DELETE'' ) then '
     || '        my_new_row := OLD; '
     || '    else '
     || '        my_new_row := NEW; '
     || '    end if; '
     || '    if( TG_OP = ''INSERT'' ) then '
     || '        my_row := NEW; '
     || '        my_old_row := NEW; '
     || '    else '
     || '        my_old_row := OLD; '
     || '        my_row := OLD; '
     || '    end if;  '
     || '    ' || ' '
     || '    if current_setting(''cyanaudit.enabled'') = ''0'' then '
     || '        return my_new_row; '
     || '    end if; '
     || ' '
     || '    perform @extschema@.fn_set_last_audit_txid(); '
     || ' '
     || '    my_recorded := clock_timestamp(); '
     || ' '
     || '    my_pk_vals := ARRAY[ ' || (select string_agg( ',', quote_ident(unnest(in_pk_cols)) )) || ' ];  '
     || '    ' 
     || ( select string_agg( E'\n\n', ' '
     || '    IF (TG_OP = ''INSERT'' AND '
     || '        my_new_row.${column_name} IS NOT NULL) OR '
     || '       (TG_OP = ''UPDATE'' AND '
     || '        my_new_row.${column_name}::text IS DISTINCT FROM '
     || '        my_old_row.${column_name}::text) OR '
     || '       (TG_OP = ''DELETE'' AND '
     || '        my_old_row.${column_name} IS NOT NULL) '
     || '    THEN '
     || '        perform @extschema@.fn_new_audit_event( '
     || '                    ' || quote_ident(unnest(in_audit_fields)) || ', '
     || '                    my_pk_vals, '
     || '                    my_recorded, '
     || '                    TG_OP, '
     || '                    my_old_row.' || quote_ident(unnest(in_column_names)) || ', '
     || '                    my_new_row.' || quote_ident(unnest(in_column_names)) || ' '
     || '                ); '
     || '    END IF; ' ))
     || ' '
     || '    return NEW; '
     || 'EXCEPTION '
     || '    WHEN undefined_function THEN '
     || '         raise notice ''Undefined function call. Please reinstall cyanaudit.''; '
     || '         return NEW; '
     || '    WHEN undefined_column THEN '
     || '         raise notice ''Undefined column. Please run fn_update_audit_fields() as superuser.''; '
     || '         return NEW; '
     || '    WHEN undefined_object THEN '
     || '         raise notice ''Cyan Audit configuration invalid. Logging disabled.''; '
     || '         return NEW; '
     || 'END '
     || ' $function$ ';
$_$;





-- fn_setup_table_audit_trigger
CREATE OR REPLACE FUNCTION @extschema@.fn_setup_table_audit_trigger
(
    in_table_name   varchar,
    in_table_schema varchar default 'public'
)
returns void 
language plpgsql strict
as $_$
declare
    my_audit_fields     varchar[];
    my_column_names     varchar[];
    my_pk_colnames      varchar[];
    my_trigger_name     varchar;
    my_function_name    varchar;
begin
    my_trigger_name := 'fn_log_audit_event_ ' || md5(in_table_schema||'.'||in_table_name);
    my_function_name := 'tr_log_audit_event_ ' || md5(in_table_schema||'.'||in_table_name);

    -- if in_table_schema = '@extschema@' then
    --     raise exception 'cyanaudit: Refusing to audit Cyan Audit table %.%',
    --         in_table_name, in_table_schema;
    -- end if;

    perform *
       from pg_class c
       join pg_namespace n
         on c.relnamespace = n.oid
      where c.relname = in_table_name
        and n.nspname = in_table_schema;

    -- If table exists, update trigger on the table.

    if not found then
        raise exception 'cyanaudit: Cannot audit nonexistent table %.%',
            in_table_name, in_table_schema;
    end if;

    my_pk_colnames := @extschema@.fn_get_table_pk_cols( in_table_name, in_table_schema );
        
    if( array_length( my_pk_colnames, 1 ) = 0 ) then
        execute 'CREATE OR REPLACE FUNCTION @extschema@.' || my_function_name
             || ' returns trigger language plpgsql as '
             || '$$ begin /*BOGUS*/ return NEW; end; $$';
        return;
    end if;

    select array_agg(audit_field),
           array_agg(column_names)
      into my_audit_fields,
           my_column_names
      from @extschema@.tb_audit_field
     where active
       and table_schema = in_table_schema
       and table_name = in_table_name;

    execute @extschema@.fn_get_trigger_function_declaration (
        my_function_name,
        my_pk_colnames,
        my_audit_fields,
        my_column_names
    );

    perform @extschema@.fn_create_audit_trigger_on_table( 
        quote_nullable( in_table_schema ), 
        quote_nullable( in_table_name ) 
    );

    -- TODO: Drop trigger if no longer needed
    return;
end
 $_$;


-- XXX don't need?
-- fn_drop_audit_event_log_trigger
CREATE OR REPLACE FUNCTION @extschema@.fn_drop_audit_event_log_trigger 
(
    in_table_name   varchar,
    in_table_schema varchar default 'public'
)
returns void as
 $_$
declare
    my_function_name    varchar;
begin
    my_function_name := 'fn_log_audit_event_' || md5(in_table_schema||'.'||in_table_name);

    set client_min_messages to warning;

    perform p.proname
       from pg_catalog.pg_depend d
       join pg_catalog.pg_proc p
         on d.classid = 'pg_proc'::regclass::oid
        and d.objid = p.oid
       join pg_catalog.pg_extension e
         on d.refclassid = 'pg_extension'::regclass::oid
        and d.refobjid = e.oid
      where e.extname = 'cyanaudit'
        and p.proname = my_function_name;

    if found then
        execute 'alter extension cyanaudit drop function '
             || '@extschema@.' || my_function_name|| '()';
    end if;

    perform p.proname
       from pg_proc p
       join pg_namespace n
         on p.pronamespace = n.oid
        and n.nspname = '@extschema@'
      where p.proname = my_function_name;

    if found then
        execute 'drop function '
             || '@extschema@.'||my_function_name||'() cascade';
    end if;

    set client_min_messages to notice;
end
 $_$
    language 'plpgsql';




-- fn_create_audit_trigger_on_table
CREATE OR REPLACE FUNCTION @extschema@.fn_create_audit_trigger_on_table
(
    in_table_schema varchar,
    in_table_name   varchar
)
returns void as 
 $_$
declare
    my_trigger_name varchar;
    my_function_name varchar;
    my_trigger_row  record;
begin
    my_trigger_name  := 'tr_log_audit_event_' || md5( in_table_schema||'.'||in_table_name );
    my_function_name := 'fn_log_audit_event_' || md5( in_table_schema||'.'||in_table_name );

    -- Check if the trigger exists already
    perform *
       from pg_trigger t
       join pg_class c
         on t.tgrelid = c.oid
       join pg_namespace n
         on c.relnamespace = c.relnamespace
      where t.tgname = my_trigger_name
        and n.nspname = in_table_schema
        and c.relname = in_table_name;

    if found then return; end if;

    execute 'drop trigger if exists $1' using my_trigger_name;

    execute 'CREATE TRIGGER $1 '
         || '   after insert or update or delete on $2.$3 for each row '
         || '   execute procedure @extschema@.$4 '
      using my_trigger_name,
            in_table_schema,
            in_table_name,
            my_function_name;
    
    execute 'ALTER EXTENSION cyanaudit ADD FUNCTION @extschema@.$1' using my_function_name;
end
 $_$
    language plpgsql strict;


-- fn_update_audit_fields
-- Create or update audit_fields for all columns in the passed-in schema.
-- If passed-in schema is null, create or update for all already-known schemas.
CREATE OR REPLACE FUNCTION @extschema@.fn_update_audit_fields
(
    in_schemas           varchar[] default null,
    in_allow_deactivate  boolean default true
) 
returns void as
 $_$
declare
    my_deactivated_field varchar;
    my_schemas           varchar[];
    my_stale_field_count integer;
begin
    -- Keep two of these functions from running at the same time
    -- perform pg_advisory_xact_lock('@extschema@.tb_audit_field'::regclass::bigint);

    my_schemas = in_schemas;
    
    if my_schemas is null or array_length(in_schemas, 1) = 0 then
        select array_agg( distinct table_schema )::varchar[]
          into my_schemas
          from @extschema@.tb_audit_field;
    end if;

    for my_deactivated_field in
        -- Make list of all fields in passed-in schemas
        with tt_all_fields as
        (
            select c.relname::varchar as table_name,
                   a.attname::varchar as column_name,
                   n.nspname::varchar as table_schema
              from pg_attribute a
              join pg_class c
                on a.attrelid = c.oid
              join pg_namespace n
                on c.relnamespace = n.oid
             where n.nspname::varchar = any(my_schemas)
        ),
        tt_new_fields as
        (
           -- Create entries in tb_audit_field for all existing fields
           -- active flags will be set to default values
            select @extschema@.fn_get_or_create_audit_field(
                        ttaf.table_name,
                        ttaf.column_name,
                        ttaf.table_schema
                   )
              from tt_all_fields ttaf
         left join @extschema@.tb_audit_field af
                on ttaf.table_schema = af.table_schema
               and ttaf.table_name = af.table_name
               and ttaf.column_name = af.column_name
             where af.audit_field is null
        ),
        -- Get list of audit_fields in passed-in schemas that are stale
        tt_stale_fields as
        (
            select af.audit_field,
                   af.table_schema,
                   af.table_name,
                   af.column_name
              from @extschema@.tb_audit_field af
         left join tt_all_fields ttaf
                on ttaf.table_schema = af.table_schema
               and ttaf.table_name = af.table_name
               and ttaf.column_name = af.column_name
             where ttaf.column_name is null
               and af.table_schema = any(my_schemas)
               and af.active
               and in_allow_deactivate
          order by af.table_schema,
                   af.table_name,
                   af.column_name
        )
        update @extschema@.tb_audit_field af
           set active = false
          from tt_stale_fields ttsf
         where ttsf.audit_field = af.audit_field
     returning 'Disabled stale field: ' || af.table_schema || '.' || af.table_name || '.' || af.column_name
    loop
        raise notice '%', my_deactivated_field;
    end loop;

    with tt_pk_tables as
    (
        select c.relname::varchar as table_name,
               n.nspname::varchar as table_schema,
               'fn_log_audit_event_' || md5(n.nspname::varchar || '.' || c.relname::varchar) as proname
          from pg_class c
          join pg_namespace n
            on c.relnamespace = n.oid
          join pg_constraint cn
            on cn.conrelid = c.oid
           and cn.contype = 'p'
         where n.nspname::varchar = any(my_schemas)
    ),
    tt_bogus_function_tables as
    (
        select table_schema,
               table_name
          from tt_pk_tables tt
          join pg_proc p
            on tt.proname = p.proname
          join pg_namespace n
            on p.pronamespace = n.oid
         where n.nspname = '@extschema@'
           and prosrc like '%-- BOGUS%'
    )
    update @extschema@.tb_audit_field af
       set active = true
      from tt_bogus_function_tables tt
     where af.active = true
       and tt.table_name = af.table_name
       and tt.table_schema = af.table_schema;

    return;
end;
 $_$
    language 'plpgsql';


--------- Audit event archiving -----------

create or replace function @extschema@.fn_redirect_audit_events() 
returns trigger as
 $_$
begin
    insert into @extschema@.tb_audit_event_current select NEW.*;
    return null;
end
 $_$
    language 'plpgsql';



------------------
----- TABLES -----
------------------

-- tb_audit_field
create sequence @extschema@.sq_pk_audit_field;

CREATE TABLE IF NOT EXISTS @extschema@.tb_audit_field
(
    audit_field     integer primary key default nextval('@extschema@.sq_pk_audit_field'),
    table_schema    varchar not null default 'public',
    table_name      varchar not null,
    column_name     varchar not null,
    active          boolean not null default true,
    CONSTRAINT tb_audit_field_table_column_key 
        UNIQUE( table_schema, table_name, column_name ),
    CONSTRAINT tb_audit_field_tb_audit_event_not_allowed 
        CHECK( table_name not like 'tb_audit_event%' )
);

alter sequence @extschema@.sq_pk_audit_field
    owned by @extschema@.tb_audit_field.audit_field;

SELECT pg_catalog.pg_extension_config_dump('@extschema@.tb_audit_field','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.sq_pk_audit_field','');



-- tb_audit_transaction_type
CREATE SEQUENCE @extschema@.sq_pk_audit_transaction_type;

CREATE TABLE IF NOT EXISTS @extschema@.tb_audit_transaction_type
(
    audit_transaction_type  integer primary key
                            default nextval('@extschema@.sq_pk_audit_transaction_type'),
    label                   varchar unique
);

ALTER SEQUENCE sq_pk_audit_transaction_type
    owned by @extschema@.tb_audit_transaction_type.audit_transaction_type;

CREATE SEQUENCE @extschema@.sq_pk_audit_event MAXVALUE 2147483647 CYCLE;

-- tb_audit_event
CREATE TABLE IF NOT EXISTS @extschema@.tb_audit_event
(
    audit_field             integer not null references @extschema@.tb_audit_field,
    pk_vals                 varchar[] not null,
    recorded                timestamp not null default clock_timestamp(),
    uid                     integer not null default @extschema@.fn_get_audit_uid(),
    row_op                  char(1) not null CHECK (row_op in ('I','U','D')),
    txid                    bigint not null default txid_current(),
    audit_transaction_type  integer references @extschema@.tb_audit_transaction_type,
    old_value               text,
    new_value               text
);

ALTER TABLE tb_audit_event
    ADD CONSTRAINT tb_audit_event_consistency_chk
        CHECK( case row_op when 'I' then old_value is null when 'D' then new_value is null 
                           when 'U' then old_value is distinct from new_value end );

-- tb_audit_event_current
CREATE TABLE IF NOT EXISTS @extschema@.tb_audit_event_current()
    inherits ( @extschema@.tb_audit_event );

drop index if exists @extschema@.tb_audit_event_current_txid_idx;
drop index if exists @extschema@.tb_audit_event_current_recorded_idx;
drop index if exists @extschema@.tb_audit_event_current_audit_field_idx;

create index tb_audit_event_current_txid_idx
    on @extschema@.tb_audit_event_current(txid);
create index tb_audit_event_current_recorded_idx
    on @extschema@.tb_audit_event_current(recorded);
create index tb_audit_event_current_audit_field_idx
    on @extschema@.tb_audit_event_current(audit_field);

drop trigger if exists tr_redirect_audit_events on @extschema@.tb_audit_event;
create trigger tr_redirect_audit_events 
    before insert on @extschema@.tb_audit_event
    for each row execute procedure @extschema@.fn_redirect_audit_events();



--------------------
------ VIEWS -------
--------------------

-- log view
CREATE OR REPLACE VIEW @extschema@.vw_audit_log as
   select ae.recorded, 
          ae.uid, 
          @extschema@.fn_get_email_by_audit_uid(ae.uid) as user_email,
          ae.txid, 
          att.label as description,
          case when af.table_schema = any(current_schemas(true))
               then af.table_name
               else af.table_schema || '.' || af.table_name
          end as table_name,
          af.column_name,
          ae.pk_vals as pk_vals,
          ae.row_op as op,
          ae.old_value,
          ae.new_value
     from @extschema@.tb_audit_event ae
     join @extschema@.tb_audit_field af using(audit_field)
left join @extschema@.tb_audit_transaction_type att using(audit_transaction_type)
 order by ae.recorded desc, af.table_name, af.column_name;

-- vw_audit_transaction_statement
CREATE OR REPLACE VIEW @extschema@.vw_audit_transaction_statement as
   select ae.txid, 
          ae.recorded,
          @extschema@.fn_get_email_by_audit_uid(ae.uid) as user_email,
          att.label as description, 
          (case ae.row_op
          when 'I' then
               'INSERT INTO ' || af.table_schema || '.' || af.table_name || ' ('
               || array_to_string(array_agg('"'||af.column_name||'"'), ',') 
               || ') VALUES ('
               || array_to_string(array_agg(coalesce(
                    quote_literal(ae.new_value), 'NULL'
                  )), ',') ||');'
          when 'U' then
               'UPDATE ' || af.table_schema || '.' || af.table_name || ' SET '
               || array_to_string(array_agg(af.column_name||' = '||coalesce(
                    quote_literal(ae.new_value), 'NULL'
                  )), ', ') 
               || ' WHERE ' 
               || quote_literal(fn_get_table_pk_cols(af.table_name, af.table_schema)) || ' = ' 
               || quote_literal(ae.pk_vals) || ';'
          when 'D' then
               'DELETE FROM ' || af.table_schema || '.' || af.table_name 
               || ' WHERE ' 
               || quote_literal(fn_get_table_pk_cols(af.table_name, af.table_schema)) || ' = ' 
               || quote_literal(ae.pk_vals) || ';'
          end)::varchar as query
     from @extschema@.tb_audit_event ae
     join @extschema@.tb_audit_field af using(audit_field)
left join @extschema@.tb_audit_transaction_type att using(audit_transaction_type)
 group by af.table_schema, af.table_name, ae.row_op, 
          ae.pk_vals, ae.txid, ae.recorded, att.label, 
          @extschema@.fn_get_email_by_audit_uid(ae.uid),
          fn_get_table_pk_cols(af.table_name, af.table_schema)
 order by ae.recorded;


-- vw_audit_transaction_statement_inverse
CREATE OR REPLACE VIEW @extschema@.vw_audit_transaction_statement_inverse AS
   select ae.txid,
          (case ae.row_op
           when 'D' then 
                'INSERT INTO ' || af.table_schema || '.' || af.table_name || ' ('
                || array_to_string(
                     array_agg('"'||af.column_name||'"'),
                   ',') || ') values ('
                || array_to_string(
                     array_agg(coalesce(
                         quote_literal(ae.old_value), 'NULL'
                     )),
                   ',') ||')'
          when 'U' then
               'UPDATE ' || af.table_schema || '.' || af.table_name || ' set '
               || array_to_string(array_agg(
                    af.column_name||' = '|| coalesce(
                        quote_literal(ae.old_value), 'NULL'
                    )
                  ), ', ')
               || ' WHERE ' 
               || quote_literal(fn_get_table_pk_cols(af.table_name, af.table_schema)) || ' = ' 
               || quote_literal(ae.pk_vals) || ';'
          when 'I' then
               'DELETE FROM ' || af.table_schema || '.' || af.table_name
               || ' WHERE ' 
               || quote_literal(fn_get_table_pk_cols(af.table_name, af.table_schema)) || ' = ' 
               || quote_literal(ae.pk_vals) || ';'
          end)::varchar as query
     from @extschema@.tb_audit_event ae
     join @extschema@.tb_audit_field af using(audit_field)
 group by af.table_schema, af.table_name, ae.row_op, 
          ae.pk_vals, ae.recorded, ae.txid,
          fn_get_table_pk_cols(af.table_name, af.table_schema)
 order by ae.recorded desc;



-----------------------
------ TRIGGERS -------
-----------------------

-- fn_before_audit_field_change
CREATE OR REPLACE FUNCTION @extschema@.fn_before_audit_field_change()
returns trigger as
 $_$
declare
    my_pk_colname       varchar;
begin
    if TG_OP = 'DELETE' then
        raise exception 'cyanaudit: Deletion from this table is not allowed.';
    end if;

    if TG_OP = 'UPDATE' then
        if NEW.table_schema IS DISTINCT FROM OLD.table_schema OR
           NEW.table_name  IS DISTINCT FROM OLD.table_name OR
           NEW.column_name IS DISTINCT FROM OLD.column_name
        then
            raise exception 'Updating table_schema, table_name or column_name not allowed.';
        end if;
    end if;
    
    -- Got to double check our value if it's true
    if NEW.active = true then
        -- active if the column is currently real in the db, inactive otherwise
       perform *
          from pg_attribute a
          join pg_class c
            on a.attrelid = c.oid
          join pg_namespace n
            on c.relnamespace = n.oid
         where n.nspname::varchar = NEW.table_schema
           and table_name::varchar = NEW.table_name
           and column_name::varchar = NEW.column_name;

        NEW.active := found;
    elsif NEW.active is null then
        -- Sensible default value for "active" is important to avoid freaking people out:

        -- If any column on same table is active, then true.
        -- Else If we know of fields on this table but all are inactive, then false.
        -- Else If we know of no fields in this table, then:
            -- If any field in same schema is active, then true.
            -- Else If we know of fields in this schema but all are inactive, then false.
            -- Else If we know of no columns in this schema, then:
                -- If any column in the database is active, then true.
                -- Else If we know of fields in this database but all are inactive, then false.
                -- Else, true.

        select active
          into NEW.active
          from tb_audit_field
      order by active desc, -- Sort active fields to the top
               (table_schema = NEW.table_schema) desc, -- Sort active fields within schema to top of that
               (table_name = NEW.table_name) desc; -- Sort active fields over table to top of that

        -- If we got here, we found no fields in the db. Activate logging by default.
        if NEW.active is null then
            NEW.active = true;
        end if;
    end if;

    return NEW;
end
 $_$
    language plpgsql;


CREATE TRIGGER tr_check_audit_field_validity
    BEFORE INSERT OR UPDATE ON @extschema@.tb_audit_field
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_before_audit_field_change();



-- fn_after_audit_field_change
CREATE OR REPLACE FUNCTION @extschema@.fn_after_audit_field_change()
returns trigger 
language plpgsql
as $_$
begin
    perform @extschema@.fn_setup_table_audit_trigger(NEW.table_name, NEW.table_schema);
    return NEW;
end
 $_$
    language 'plpgsql';


CREATE TRIGGER tr_audit_event_log_trigger_updater
    AFTER INSERT OR UPDATE on @extschema@.tb_audit_field
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_after_audit_field_change();



-- EVENT TRIGGER


CREATE OR REPLACE FUNCTION @extschema@.fn_update_audit_fields_event_trigger_drop()
returns event_trigger
language plpgsql as
   $function$
begin
    perform @extschema@.fn_update_audit_fields( null, true );
exception
     when insufficient_privilege
     then return;
end
   $function$;

CREATE OR REPLACE FUNCTION @extschema@.fn_update_audit_fields_event_trigger()
returns event_trigger
language plpgsql as
   $function$
begin
    perform @extschema@.fn_update_audit_fields( null, false );
exception
     when insufficient_privilege
     then return;
end
   $function$;

CREATE EVENT TRIGGER tr_update_audit_fields ON ddl_command_end
    WHEN TAG IN ('ALTER TABLE', 'CREATE TABLE')
    EXECUTE PROCEDURE @extschema@.fn_update_audit_fields_event_trigger();

CREATE EVENT TRIGGER tr_update_audit_fields_delete ON sql_drop
    EXECUTE PROCEDURE @extschema@.fn_update_audit_fields_event_trigger_drop();
    



--- PERMISSIONS

grant usage on schema @extschema@ to public;

grant usage on all sequences in schema @extschema@ to public;

grant select on all tables in schema @extschema@ to public;
revoke select on @extschema@.tb_audit_event from public;
grant select on @extschema@.tb_audit_event to postgres;

grant select (audit_transaction_type, txid) 
   on @extschema@.tb_audit_event_current to public;
grant update (audit_transaction_type) 
   on @extschema@.tb_audit_event_current to public;

grant insert on @extschema@.tb_audit_event, 
                @extschema@.tb_audit_event_current,
                @extschema@.tb_audit_transaction_type
        to public;
