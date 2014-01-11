-- fn_set_audit_uid
CREATE OR REPLACE FUNCTION @extschema@.fn_set_audit_uid
(
    in_uid   integer
)
returns integer as
 $_$
begin
    return set_config('cyanaudit.uid', in_uid::varchar, false);
end;
 $_$
    language plpgsql strict;


-- fn_get_audit_uid
CREATE OR REPLACE FUNCTION @extschema@.fn_get_audit_uid() returns integer as
 $_$
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
 $_$
    language plpgsql stable;


drop function if exists public.fn_set_audit_uid(integer);
drop function if exists public.fn_get_audit_uid();

-- fn_get_audit_uid
CREATE OR REPLACE FUNCTION @extschema@.fn_get_audit_uid() returns integer as
 $_$
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
 $_$
    language plpgsql stable;


-- fn_get_email_by_audit_uid
CREATE OR REPLACE FUNCTION @extschema@.fn_get_email_by_audit_uid
(
    in_uid  integer
)
returns varchar as
 $_$
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

    my_query := 'select ' || my_user_table_email_col
             || '  from ' || my_user_table
             || ' where ' || my_user_table_uid_col
                          || ' = ' || in_uid;
    execute my_query
       into my_email;

    return my_email;
exception
    when undefined_object then
         return null;
    when undefined_table then
         raise notice 'Cyan Audit: Invalid user_table';
         return null;
    when undefined_column then
         raise notice 'Cyan Audit: Invalid user_table_uid_col or user_table_email_col';
         return null;
end
 $_$
    language plpgsql stable strict;


-- fn_get_audit_uid_by_username
CREATE OR REPLACE FUNCTION @extschema@.fn_get_audit_uid_by_username
(
    in_username varchar
)
returns integer as
 $_$
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

    my_query := 'select ' || my_user_table_uid_col
             || '  from ' || my_user_table
             || ' where ' || my_user_table_username_col
                          || ' = ''' || in_username || '''';
    execute my_query
       into my_uid;

    return my_uid;
exception
    when undefined_object then
         return null;
    when undefined_table then
         raise notice 'Cyan Audit: Invalid user_table';
         return null;
    when undefined_column then
         raise notice 'Cyan Audit: Invalid user_table_uid_col or user_table_username_col';
end
 $_$
    language plpgsql stable strict;

do language plpgsql
 $$
declare
    my_command  varchar;
begin
    my_command := 'alter database ' || current_database() 
               || '  set cyanaudit.enabled = '''
               || current_setting('cyanaudit.enabled') || '''';
    execute my_command;
    my_command := 'alter database ' || current_database() 
               || '  set cyanaudit.uid = '''
               || current_setting('cyanaudit.uid') || '''';
    execute my_command;
    my_command := 'alter database ' || current_database() 
               || '  set cyanaudit.last_txid = '''
               || current_setting('cyanaudit.last_txid') || '''';
    execute my_command;
    my_command := 'alter database ' || current_database() 
               || '  set cyanaudit.archive_tablespace = '''
               || current_setting('cyanaudit.archive_tablespace') || '''';
    execute my_command;
    my_command := 'alter database ' || current_database() 
               || '  set cyanaudit.user_table = '''
               || current_setting('cyanaudit.user_table') || '''';
    execute my_command;
    my_command := 'alter database ' || current_database() 
               || '  set cyanaudit.user_table_uid_col = '''
               || current_setting('cyanaudit.user_table_uid_col') || '''';
    execute my_command;
    my_command := 'alter database ' || current_database() 
               || '  set cyanaudit.user_table_email_col = '''
               || current_setting('cyanaudit.user_table_email_col') || '''';
    execute my_command;
    my_command := 'alter database ' || current_database() 
               || '  set cyanaudit.user_table_username_col = '''
               || current_setting('cyanaudit.user_table_username_col') || '''';
    execute my_command;
end;
 $$;

