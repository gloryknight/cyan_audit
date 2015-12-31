/* TODO: 
 - transaction-specific GUC for transaction labels in real-time
 - function to set and unset real-time transaction label
*/
   
----- INITIAL SETUP -----
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
    execute my_command || 'archive_tablespace = pg_default';
    execute my_command || 'user_table = '''' ';
    execute my_command || 'user_table_uid_col = '''' ';
    execute my_command || 'user_table_email_col = '''' ';
    execute my_command || 'user_table_username_col = '''' ';
    execute my_command || '_audit_transaction_type = '''' ';
    execute my_command || '_uid = -1';
    execute my_command || '_last_txid = 0';
end;
 $$;




------------------------
------ FUNCTIONS ------
------------------------

----- User/Application Functions ----

-- fn_set_audit_uid
CREATE OR REPLACE FUNCTION @extschema@.fn_set_audit_uid
(
    in_uid   integer
)
returns integer
language sql strict
as $_$
    select (set_config('cyanaudit._uid', in_uid::varchar, false))::integer;
 $_$;


-- fn_get_audit_uid
CREATE OR REPLACE FUNCTION @extschema@.fn_get_audit_uid() 
returns integer 
language plpgsql stable
as $_$
declare
    my_uid    integer;
begin
    my_uid := coalesce( nullif( current_setting('cyanaudit._uid'), '' )::integer, -1 );

    if my_uid >= 0 then return my_uid; end if;

    select @extschema@.fn_get_audit_uid_by_username(current_user::varchar)
      into my_uid;

    return @extschema@.fn_set_audit_uid( coalesce( my_uid, 0 ) );
exception
    when undefined_object
    then return @extschema@.fn_set_audit_uid( 0 );
end
 $_$;


-- fn_get_last_audit_txid
CREATE OR REPLACE FUNCTION @extschema@.fn_get_last_audit_txid()
returns bigint
language sql stable
as $_$
    SELECT (nullif(current_setting('cyanaudit._last_txid'), '0'))::bigint;
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

    update @extschema@.tb_audit_event
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



-- fn_update_audit_fields
-- Create or update audit_fields for all columns in the passed-in schema.
-- If passed-in schema is null, create or update for all already-known schemas.
CREATE OR REPLACE FUNCTION @extschema@.fn_update_audit_fields
(
    in_schema            varchar default null
) 
returns void as
 $_$
declare
    my_schemas           varchar[];
begin
    if pg_trigger_depth() > 0 then
        return;
    end if;

    -- Add only those tables in the passed-in schemas. 
    -- If no schemas are passed in, use only those we already know about.
    -- This way, we will never log any schema that has not been explicitly
    -- requested to be logged.
    select case when in_schema is not null
                then ARRAY[ in_schema ]
                else array_agg( distinct table_schema )
           end
      into my_schemas
      from @extschema@.tb_audit_field;

    with tt_audit_fields as
    (
        select coalesce
               (
                    af.audit_field,
                    @extschema@.fn_get_or_create_audit_field
                    ( 
                        n.nspname::varchar,
                        c.relname::varchar,
                        a.attname::varchar
                    )
               ) as audit_field,
               (a.attnum is null and af.loggable) as stale
          from (
                    pg_class c
               join pg_attribute a
                 on a.attrelid = c.oid
                and a.attnum > 0
                and a.attisdropped is false
               join pg_namespace n
                 on c.relnamespace = n.oid
                and n.nspname::varchar = any( my_schemas )
               join pg_constraint cn
                 on conrelid = c.oid
                and cn.contype = 'p'
               ) 
     full join @extschema@.tb_audit_field af
            on af.table_schema = n.nspname::varchar
           and af.table_name   = c.relname::varchar
           and af.column_name  = a.attname::varchar
           and af.loggable is true
    )
    update @extschema@.tb_audit_field af
       set loggable = false
      from tt_audit_fields ttaf
     where af.audit_field = ttaf.audit_field
       and af.loggable
       and ttaf.stale;

    return;
end;
 $_$
    language 'plpgsql';





---- INTERNAL UTILITY FUNCTIONS ----

-- fn_set_last_audit_txid
CREATE OR REPLACE FUNCTION @extschema@.fn_set_last_audit_txid
(
    bigint default txid_current()
)
returns bigint
language sql strict
as $_$
    SELECT (set_config('cyanaudit._last_txid', $1::varchar, false))::bigint;
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


-- fn_get_table_pk_cols
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

        
-- fn_get_or_create_audit_field
CREATE OR REPLACE FUNCTION @extschema@.fn_get_or_create_audit_field
(
    in_table_schema     varchar,
    in_table_name       varchar,
    in_column_name      varchar
)
returns integer as
 $_$
declare
    my_audit_field  integer;
    my_loggable     boolean;
begin
    select audit_field,
           loggable
      into my_audit_field,
           my_loggable
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
    elsif my_loggable is false then
        update @extschema@.tb_audit_field
           set loggable = loggable -- trigger will set correct value
         where audit_field = my_audit_field;
    end if;

    return my_audit_field;
end
 $_$
    language 'plpgsql';
        

-- fn_log_audit_event
CREATE OR REPLACE FUNCTION @extschema@.fn_log_audit_event()
 RETURNS trigger
 LANGUAGE plpgsql
AS $_$
DECLARE
    my_audit_fields         varchar[];
    my_audit_field          integer;
    my_column_names         varchar[];
    my_column_name          varchar;
    my_new_row              record;
    my_old_row              record;
    my_pk_cols              varchar;
    my_pk_vals_constructor  varchar;
    my_pk_vals              varchar[];
    my_old_value            text;
    my_new_value            text;
BEGIN
    my_pk_cols      := TG_ARGV[0]::varchar[];
    my_audit_fields := TG_ARGV[1]::varchar[];
    my_column_names := TG_ARGV[2]::varchar[];

    if( TG_OP = 'INSERT' ) then
        my_new_row := NEW;
        my_old_row := NEW;
    elsif( TG_OP = 'UPDATE' ) then
        my_new_row := NEW;
        my_old_row := OLD;
    elsif( TG_OP = 'DELETE' ) then
        my_new_row := OLD;
        my_old_row := OLD;
    end if;

    if current_setting('cyanaudit.enabled') = '0' then
        return my_new_row;
    end if;

    perform @extschema@.fn_set_last_audit_txid();

    -- Given:  my_pk_cols::varchar[]           = ARRAY[ 'column foo',bar ]
    -- Result: my_pk_vals_constructor::varchar = 'select ARRAY[ $1."column foo", $1.bar ]::varchar[]'
    select 'select ARRAY[' || string_agg( '$1.' || quote_ident(pk_col), ',' ) || ']::varchar[]'
      into my_pk_vals_constructor
      from ( select unnest(my_pk_cols::varchar[]) as pk_col ) x;

    -- Execute the result using my_new_row in $1 to produce the following result:
    -- my_pk_vals::varchar[] = ARRAY[ 'val1', 'val2' ]
    EXECUTE my_pk_vals_constructor
       into my_pk_vals
      using my_new_row;

    FOR my_column_name, my_audit_field in
        select unnest( my_column_names::varchar[] ),
               unnest( my_audit_fields::varchar[] )
    LOOP
        IF TG_OP = 'INSERT' THEN
            EXECUTE format('select null::text, $1.%I::text', my_column_name)
               INTO my_old_value, my_new_value
              USING my_new_row;

            CONTINUE when my_new_value is null;

        ELSIF TG_OP = 'UPDATE' THEN
            EXECUTE format( 'select $1.%1$I::text, $2.%1$I::text', my_column_name)
               INTO my_old_value, my_new_value
              USING my_old_row, my_new_row;

            CONTINUE when my_old_value is not distinct from my_new_value;

        ELSIF TG_OP = 'DELETE' THEN
            EXECUTE format('select $1.%I::text, null::text', my_column_name)
               INTO my_old_value, my_new_value
              USING my_old_row;

            CONTINUE when my_old_value is null;

        END IF;

        EXECUTE format( 'INSERT INTO @extschema@.tb_audit_event '
                     || '( audit_field, pk_vals, row_op, old_value, new_value ) '
                     || 'VALUES(  $1, $2, $3::char(1), $4, $5 ) ',
                        my_column_name
                      )
          USING my_audit_field, my_pk_vals, TG_OP, my_old_value, my_new_value;
    END LOOP;

    RETURN NEW;
EXCEPTION
    WHEN undefined_function THEN
         raise notice 'cyanaudit: Missing internal function. Please reinstall.';
         return NEW;
    WHEN undefined_column THEN
         raise notice 'cyanaudit: Attempt to log deleted column. Please run fn_update_audit_fields() as superuser.';
         return NEW;
    WHEN undefined_object THEN
         raise notice 'cyanaudit: Invalid global configuration. Logging disabled.';
         return NEW;
END
$_$;







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
    enabled         boolean not null,
    loggable        boolean not null,
    CONSTRAINT tb_audit_field_table_column_key 
        UNIQUE( table_schema, table_name, column_name ),
    CONSTRAINT tb_audit_field_tb_audit_event_not_allowed 
        CHECK( table_schema != '@extschema@' )
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

SELECT pg_catalog.pg_extension_config_dump('@extschema@.tb_audit_transaction_type','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.sq_pk_audit_transaction_type','');



-- tb_audit_event
CREATE SEQUENCE @extschema@.sq_pk_audit_event MAXVALUE 2147483647 CYCLE;

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

ALTER TABLE @extschema@.tb_audit_event
    ADD CONSTRAINT tb_audit_event_consistency_chk
        CHECK( case row_op when 'I' then old_value is null when 'D' then new_value is null 
                           when 'U' then old_value is distinct from new_value end );

-- These are empty "template" indexes to clone when creating child tables.
CREATE INDEX tb_audit_event_txid_idx        on @extschema@.tb_audit_event(txid);
CREATE INDEX tb_audit_event_recorded_idx    on @extschema@.tb_audit_event(recorded);
CREATE INDEX tb_audit_event_audit_field_idx on @extschema@.tb_audit_event(audit_field);





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
    IF TG_OP = 'INSERT' THEN
        if NEW.table_schema = '@extschema@' then return NULL; end if;
    ELSIF TG_OP = 'DELETE' then
        raise exception 'cyanaudit: Deletion from this table is not allowed.';
    ELSIf TG_OP = 'UPDATE' then
        if NEW.table_schema != OLD.table_schema OR
           NEW.table_name  != OLD.table_name OR
           NEW.column_name != OLD.column_name
        then
            raise exception 'Updating table_schema, table_name or column_name not allowed.';
        end if;
    end if;
    
   perform *
      from pg_attribute a
      join pg_class c
        on a.attrelid = c.oid
      join pg_namespace n
        on c.relnamespace = n.oid
      join pg_constraint cn
        on conrelid = c.oid
     where n.nspname::varchar = NEW.table_schema
       and c.relname::varchar = NEW.table_name
       and a.attname::varchar = NEW.column_name
       and cn.contype = 'p'
       and a.attnum > 0
       and a.attisdropped is false;

    NEW.loggable := found;

    -- Got to double check our value if it's true
    if NEW.enabled is null then
        -- Sensible default value for "enabled" is important to avoid freaking people out:

        -- If any column on same table is enabled, then true.
        -- Else If we know of fields on this table but all are inactive, then false.
        -- Else If we know of no fields in this table, then:
            -- If any field in same schema is enabled, then true.
            -- Else If we know of fields in this schema but all are inactive, then false.
            -- Else If we know of no columns in this schema, then:
                -- If any column in the database is enabled, then true.
                -- Else If we know of fields in this database but all are inactive, then false.
                -- Else, true:
        select enabled
          into NEW.enabled
          from @extschema@.tb_audit_field
      order by (table_name = NEW.table_name) desc, -- Sort enabled fields over table to top of that
               (table_schema = NEW.table_schema) desc, -- Sort enabled fields within schema to top of that
               enabled desc; -- Sort any remaining enabled fields to the top

        -- If we got here, we found no fields in the db. Activate logging by default.
        if NEW.enabled is null then
            NEW.enabled = true;
        end if;
    end if;

    return NEW;
end
 $_$
    language plpgsql;


CREATE TRIGGER tr_before_audit_field_change
    BEFORE INSERT OR UPDATE ON @extschema@.tb_audit_field
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_before_audit_field_change();


-- fn_after_audit_field_change
CREATE OR REPLACE FUNCTION @extschema@.fn_after_audit_field_change()
returns trigger 
language plpgsql
as $_$
declare
    my_pk_colnames      varchar[];
    my_function_name    varchar;
    my_audit_fields     varchar[];
    my_column_names     varchar[];
begin
    if TG_OP = 'UPDATE' and OLD.enabled = NEW.enabled and OLD.loggable = NEW.loggable THEN
        return NEW;
    end if;

    perform @extschema@.fn_verify_partition_config();

    perform *
       from pg_trigger t
       join pg_class c
         on t.tgrelid = c.oid
       join pg_namespace n
         on c.relnamespace = n.oid
      where n.nspname::varchar = NEW.table_schema
        and c.relname::varchar = NEW.table_name
        and tgname = 'tr_log_audit_event';

    IF FOUND THEN
        execute format( 'DROP TRIGGER tr_log_audit_event ON %I.%I',
                        NEW.table_schema, NEW.table_name );
    END IF;

    -- Get a list of audit fields and column names for this table
    select array_agg(audit_field),
           array_agg(column_name)
      into my_audit_fields,
           my_column_names
      from @extschema@.tb_audit_field
     where enabled
       and loggable
       and table_schema = NEW.table_schema
       and table_name = NEW.table_name;

    IF array_length(my_audit_fields, 1) > 0 THEN
        my_pk_colnames := @extschema@.fn_get_table_pk_cols( NEW.table_name, NEW.table_schema );
            
        -- Create the table trigger (if it doesn't exist) to call the function
        execute format( 'CREATE TRIGGER tr_log_audit_event '
                     || 'AFTER INSERT OR UPDATE OR DELETE ON %I.%I FOR EACH ROW '
                     || 'EXECUTE PROCEDURE @extschema@.fn_log_audit_event(%L,%L,%L)',
                        NEW.table_schema,
                        NEW.table_name,
                        my_pk_colnames,
                        my_audit_fields,
                        my_column_names
                      );
    END IF;

    return NEW;
end
 $_$;

-- Function to install the event trigger explicitly after pg_restore completes,
-- because we don't want it firing during pg_restore.
CREATE TRIGGER tr_after_audit_field_change
    AFTER INSERT OR UPDATE on @extschema@.tb_audit_field
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_after_audit_field_change();



--------- Partitioning -----------

CREATE OR REPLACE FUNCTION @extschema@.fn_parse_tgargs
(
    in_tgargs   BYTEA
)
RETURNS VARCHAR[] AS
 $_$
    SELECT string_to_array(
               regexp_replace(
                   encode(
                       in_tgargs,
                       'escape'
                   )::VARCHAR,
                   '\\000$',
                   ''
               ),
               '\000'
           )::VARCHAR[];
 $_$
    LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION @extschema@.fn_redirect_audit_events() 
returns trigger as
 $_$
declare
    my_table_name   varchar;
begin
    my_table_name := TG_ARGV[0];

    execute format( 'insert into @extschema@.%I select $1.*', my_table_name )
      using NEW;

    return null;
end
 $_$
    language 'plpgsql';


CREATE OR REPLACE FUNCTION @extschema@.fn_get_active_partition_name()
returns varchar as
 $_$
    select (@extschema@.fn_parse_tgargs( tgargs ))[1]
      from pg_trigger t
      join pg_class c
        on t.tgrelid = c.oid
      join pg_namespace n
        on c.relnamespace = n.oid
     where n.nspname = '@extschema@'
       and c.relname = 'tb_audit_event'
       and t.tgname = 'tr_redirect_audit_events';
 $_$
    language sql;

CREATE OR REPLACE FUNCTION @extschema@.fn_activate_partition
(
    in_partition_name   varchar
)
returns void as
 $_$
declare
    my_active_partition_name   varchar;
begin
    my_active_partition_name := @extschema@.fn_get_active_partition_name();

    if my_active_partition_name = in_partition_name then
        -- already configured correctly
        return;
    end if;

    if my_active_partition_name is not null then
        DROP TRIGGER tr_redirect_audit_events on @extschema@.tb_audit_event;
    end if;

    execute format( 'CREATE TRIGGER tr_redirect_audit_events '
                 || 'before insert on @extschema@.tb_audit_event for each row '
                 || 'execute procedure @extschema@.fn_redirect_audit_events( %L ) ',
                    in_partition_name );
    return;
end
 $_$
    language plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.fn_setup_partition_range_constraint
(
    in_table_name   varchar
)
returns void as
 $_$
declare
    my_min_recorded     timestamp;
    my_max_recorded     timestamp;
    my_min_txid         bigint;
    my_max_txid         bigint;
    my_constraint_name  varchar;
begin
    my_constraint_name := 'partition_range_chk';

    perform *
       from pg_constraint cn
       join pg_class c
         on cn.conrelid = c.oid
       join pg_namespace n
         on c.relnamespace = n.oid
      where n.nspname = 'cyanaudit'
        and cn.conname = my_constraint_name;

    if found then
        execute format( 'alter table @extschema@.%I drop constraint %I', 
                        in_table_name, my_constraint_name );
    end if;

    execute format( 'select min(recorded), max(recorded), min(txid), max(txid) from @extschema@.%I',
                    in_table_name )
       into my_min_recorded, my_max_recorded, my_min_txid, my_max_txid;

    if in_table_name = @extschema@.fn_get_active_partition_name() then
        execute format( 'ALTER TABLE @extschema@.%I add constraint %I '
                     || ' CHECK( recorded > %L )', 
                        in_table_name, my_constraint_name, coalesce( my_min_recorded, now() ) );
    elsif my_min_recorded is not null then
        execute format( 'ALTER TABLE @extschema@.%I add constraint %I '
                    || ' CHECK( recorded between %L and %L and txid between %L and %L )',
                       in_table_name, my_constraint_name,
                       my_min_recorded, my_max_recorded, my_min_txid, my_max_txid );
    end if;
end
 $_$
    language plpgsql;


-- fn_create_new_partition
CREATE OR REPLACE FUNCTION @extschema@.fn_create_new_partition
(
    in_new_table_name varchar default 'tb_audit_event_' || to_char(now(), 'YYYYMMDD_HH24MI')
)
returns varchar as
 $_$
begin
    if in_new_table_name !~ '^tb_audit_event_\d{8}_\d{4}$' then
        raise exception 'Table name must conform to format "tb_audit_event_########_####"';
    end if;

    SET LOCAL client_min_messages to WARNING;

    execute format( 'CREATE TABLE @extschema@.%I '
                 || '( LIKE @extschema@.tb_audit_event INCLUDING STORAGE INCLUDING INDEXES ) '
                 || 'INHERITS ( @extschema@.tb_audit_event )',
                    in_new_table_name );

    execute format( 'ALTER EXTENSION cyanaudit ADD TABLE @extschema@.%I', in_new_table_name );

    return in_new_table_name;
end
 $_$
    language plpgsql;


-- fn_verify_partition_config()
CREATE OR REPLACE FUNCTION @extschema@.fn_verify_partition_config()
returns varchar as
 $_$
declare
    my_partition_name   varchar;
begin
    my_partition_name := @extschema@.fn_get_active_partition_name();

    if my_partition_name is null then
        my_partition_name := @extschema@.fn_create_new_partition();
        perform @extschema@.fn_activate_partition( my_partition_name );
        perform @extschema@.fn_setup_partition_range_constraint( my_partition_name );
    end if;

    return my_partition_name;
end
 $_$
    language plpgsql;




-------- Partition Archiving ---------

CREATE OR REPLACE FUNCTION @extschema@.fn_archive_partition
(
    in_partition_name   varchar
)
returns void as
 $_$
declare
    my_archive_tablespace   varchar;
    my_index_name           varchar;
begin
    my_archive_tablespace := current_setting( 'cyanaudit.archive_tablespace' );

    execute format( 'alter table @extschema@.%I set tablespace %I',
                    in_partition_name, my_archive_tablespace );

    for my_index_name in
        select i.indname 
          from pg_index i
          join pg_class c
            on i.indrelid = c.oid
           and c.relname = in_partition_name
          join pg_namespace n
            on c.relnamespace = n.oid
           and n.nspname = '@extschema@'
         where c.relname = in_partition_name
    loop
        execute format( 'alter index @extschema@.%I set tablespace %I',
                        my_index_name, my_archive_tablespace );
    end loop;
exception
    when undefined_object then
        raise exception 'cyanaudit: Missing setting for cyanaudit.archive_tablespace. Aborting.';
end
 $_$
    language plpgsql strict;


-- Returns names of tables dropped
CREATE OR REPLACE FUNCTION @extschema@.fn_prune_archive
(
    in_keep_interval    interval
)
returns setof varchar as 
 $_$
declare
    my_table_name           varchar;
    my_min_keep_table_name  varchar;
begin
    select 'tb_audit_event_' || to_char( now() - in_keep_interval, 'YYYYMMDD_HH24MI' )
      into my_min_keep_table_name;

    for my_table_name in
        select c.relname
          from pg_class c
          join pg_namespace n
            on c.relnamespace = n.oid
           and n.nspname = '@extschema@'
         where c.relkind = 'r'
           and c.relname ~ '^tb_audit_event_\d{8}_\d{4}$'
           and c.relname < my_min_keep_table_name
         order by c.relname desc
        offset 1
    loop
        execute 'alter extension cyanaudit drop table @extschema@.'||quote_ident(my_table_name);
        execute 'drop table @extschema@.'||quote_ident(my_table_name);
        return next my_table_name;
    end loop;

    return;
end
 $_$
    language plpgsql strict;
    



-- EVENT TRIGGER
-- fn_update_audit_fields_event_trigger()
CREATE OR REPLACE FUNCTION @extschema@.fn_update_audit_fields_event_trigger()
returns event_trigger
language plpgsql as
   $function$
begin
    perform @extschema@.fn_update_audit_fields();
exception
     when insufficient_privilege
     then return;
end
   $function$;



CREATE OR REPLACE FUNCTION @extschema@.fn_create_event_trigger()
RETURNS void
LANGUAGE plpgsql
AS $_$
begin
    PERFORM *
       from pg_event_trigger
      where evtname = 'tr_update_audit_fields';

    IF NOT FOUND THEN
        CREATE EVENT TRIGGER tr_update_audit_fields ON ddl_command_end
            WHEN TAG IN ('ALTER TABLE', 'CREATE TABLE', 'DROP TABLE')
            EXECUTE PROCEDURE @extschema@.fn_update_audit_fields_event_trigger();

        ALTER EXTENSION cyanaudit ADD EVENT TRIGGER tr_update_audit_fields;
    END IF;
end;
 $_$;




--- PERMISSIONS

grant  usage
       on schema @extschema@                      to public;

grant  usage
       on all sequences in schema @extschema@     to public;

grant  insert, select 
       on @extschema@.tb_audit_transaction_type   to public;

grant  insert, 
       select (audit_transaction_type, txid), 
       update (audit_transaction_type) 
       on @extschema@.tb_audit_event              to public;


