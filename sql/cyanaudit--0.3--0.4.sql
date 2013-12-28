--- RENAME EXTENSION
update pg_extension
   set extname = 'cyanaudit'
 where extname = 'auditlog';

drop function fn_rotate_audit_events();
drop function fn_get_or_create_audit_field(varchar, varchar);

alter sequence @extschema@.sq_pk_audit_event MAXVALUE 2147483647 CYCLE;

-- fn_get_audit_uid
CREATE OR REPLACE FUNCTION fn_get_audit_uid() returns integer as
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
    my_email    varchar;
    my_query    varchar;
begin
    my_query := 'select ' || current_setting('cyanaudit.user_table_email_col')
             || '  from ' || current_setting('cyanaudit.user_table')
             || ' where ' || current_setting('cyanaudit.user_table_uid_col')
                          || ' = ' || in_uid;
    execute my_query
       into my_email;

    return my_email;
exception
    when undefined_object
    then return null;
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
    my_uid      varchar;
    my_query    varchar;
begin
    my_query := 'select ' || current_setting('cyanaudit.user_table_uid_col')
             || '  from ' || current_setting('cyanaudit.user_table')
             || ' where ' || current_setting('cyanaudit.user_table_username_col')
                          || ' = ''' || in_username || '''';
    execute my_query
       into my_uid;

    return my_uid;
exception
    when undefined_object
    then return null;
end
 $_$
    language plpgsql stable strict;

-- fn_get_column_data_type
CREATE OR REPLACE FUNCTION @extschema@.fn_get_column_data_type
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

-- fn_get_table_pk_col
CREATE OR REPLACE FUNCTION @extschema@.fn_get_table_pk_col
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


-- fn_label_last_audit_transaction
CREATE OR REPLACE FUNCTION @extschema@.fn_label_last_audit_transaction
(
    in_label    varchar
)
returns bigint as
 $_$
begin
    return @extschema@.fn_label_audit_transaction
           (
                in_label,
                @extschema@.fn_get_last_audit_txid()
           );
end
 $_$
    language 'plpgsql' strict;


-- fn_get_or_create_audit_field
CREATE OR REPLACE FUNCTION @extschema@.fn_get_or_create_audit_field
(
    in_table_name       varchar,
    in_column_name      varchar,
    in_audit_data_type  integer default null
)
returns integer as
 $_$
declare
    my_audit_field   integer;
    my_active        boolean;
begin
    select audit_field
      into my_audit_field
      from @extschema@.tb_audit_field
     where table_name = in_table_name
       and column_name = in_column_name;

    if not found then
        perform *
           from @extschema@.tb_audit_field
          where table_name = in_table_name
          limit 1;

        if found then
            perform *
               from @extschema@.tb_audit_field
              where table_name = in_table_name
                and active = true
              limit 1;

            if found then
                my_active = true;
            else
                my_active = false;
            end if;
        else
            perform *
               from information_schema.columns
              where table_schema = 'public'
                and table_name = in_table_name
                and column_name = in_column_name;

            if found then
                my_active = true;
            else
                my_active = false;
            end if;
        end if;

        insert into @extschema@.tb_audit_field
        (
            table_name,
            column_name,
            active,
            audit_data_type
        )
        values
        (
            in_table_name,
            in_column_name,
            my_active,
            in_audit_data_type
        )
        returning audit_field
        into my_audit_field;
    end if;

    return my_audit_field;
end
 $_$
    language 'plpgsql';


-- fn_drop_audit_event_log_trigger
CREATE OR REPLACE FUNCTION @extschema@.fn_drop_audit_event_log_trigger
(
    in_table_name   varchar
)
returns void as
 $_$
declare
    my_function_name    varchar;
begin
    my_function_name := 'fn_log_audit_event_'||in_table_name;

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



-- fn_update_audit_event_log_trigger_on_table
CREATE OR REPLACE FUNCTION @extschema@.fn_update_audit_event_log_trigger_on_table
(
    in_table_name   varchar
)
returns void as
 $_$
use strict;

my $table_name = $_[0];

return if $table_name =~ /tb_audit_.*/;

my $table_q = "select relname "
            . "  from pg_class c "
            . "  join pg_namespace n "
            . "    on c.relnamespace = n.oid "
            . " where n.nspname = 'public' "
            . "   and c.relname = '$table_name' ";

my $table_rv = spi_exec_query($table_q);

if( $table_rv->{'processed'} == 0 )
{
    elog(NOTICE, "Cannot audit invalid table '$table_name'");
    return;
}

my $colnames_q = "select audit_field, column_name "
               . "  from @extschema@.tb_audit_field "
               . " where table_name = '$table_name' "
               . "   and active = true ";

my $colnames_rv = spi_exec_query($colnames_q);

if( $colnames_rv->{'processed'} == 0 )
{
    my $q = "select @extschema@.fn_drop_audit_event_log_trigger('$table_name')";
    eval{ spi_exec_query($q) };
    elog(ERROR, "fn_drop_audit_event_log_trigger: $@") if($@);
    return;
}

my $pk_q = "select @extschema@.fn_get_table_pk_col('$table_name') as pk_col ";

my $pk_rv = spi_exec_query($pk_q);

my $pk_col = $pk_rv->{'rows'}[0]{'pk_col'};

unless( $pk_col )
{
    my $pk2_q = "select column_name as pk_col "
              . "  from @extschema@.tb_audit_field "
              . " where table_pk = audit_field "
              . "   and table_name = '$table_name'";

    my $pk2_rv = spi_exec_query($pk2_q);

    $pk_col = $pk2_rv->{'rows'}[0]{'pk_col'};

    unless( $pk_col )
    {
        elog(NOTICE, "pk_col is null");
        return;
    }
}

my $fn_q = <<EOF;
CREATE OR REPLACE FUNCTION @extschema@.fn_log_audit_event_$table_name()
returns trigger as
 \$_\$
-- THIS FUNCTION AUTOMATICALLY GENERATED. DO NOT EDIT
DECLARE
    my_row_pk_val       integer;
    my_old_row          record;
    my_new_row          record;
    my_recorded         timestamp;
BEGIN
    if( TG_OP = 'INSERT' ) then
        my_row_pk_val := NEW.$pk_col;
    else
        my_row_pk_val := OLD.$pk_col;
    end if;

    if( TG_OP = 'DELETE' ) then
        my_new_row := OLD;
    else
        my_new_row := NEW;
    end if;
    if( TG_OP = 'INSERT' ) then
        my_old_row := NEW;
    else
        my_old_row := OLD;
    end if;

    if current_setting('cyanaudit.enabled') = '0' then
        return my_new_row;
    end if;

    perform @extschema@.fn_set_last_audit_txid();

    my_recorded := clock_timestamp();

EOF

foreach my $row (@{$colnames_rv->{'rows'}})
{
    my $column_name = $row->{'column_name'};
    my $audit_field = $row->{'audit_field'};

    $fn_q .= <<EOF;
    IF (TG_OP = 'INSERT' AND
        my_new_row.$column_name IS NOT NULL) OR
       (TG_OP = 'UPDATE' AND
        my_new_row.${column_name}::text IS DISTINCT FROM
        my_old_row.${column_name}::text) OR
       (TG_OP = 'DELETE')
    THEN
        perform @extschema@.fn_new_audit_event(
                    $audit_field,
                    my_row_pk_val,
                    my_recorded,
                    TG_OP,
                    my_old_row.$column_name,
                    my_new_row.$column_name
                );
    END IF;

EOF
}

$fn_q .= <<EOF;
    return NEW;
EXCEPTION
    WHEN undefined_function THEN
         raise notice 'Undefined function call. Please reinstall cyanaudit.';
         return NEW;
    WHEN undefined_column THEN
         raise notice 'Undefined column. Please run fn_update_audit_fields().';
         return NEW;
END
 \$_\$
    language 'plpgsql';
EOF

eval { spi_exec_query($fn_q) };
elog(ERROR, $@) if $@;

my $tg_q = "CREATE TRIGGER tr_log_audit_event_$table_name "
         . "   after insert or update or delete on $table_name for each row "
         . "   execute procedure @extschema@.fn_log_audit_event_$table_name()";
eval { spi_exec_query($tg_q) };

my $ext_q = "ALTER EXTENSION cyanaudit ADD FUNCTION @extschema@.fn_log_audit_event_$table_name()";

eval { spi_exec_query($ext_q) };
 $_$
    language 'plperl';




-- fn_check_audit_field_validity
CREATE OR REPLACE FUNCTION @extschema@.fn_check_audit_field_validity()
returns trigger as
 $_$
declare
    my_pk_col           varchar;
begin
    if TG_OP = 'UPDATE' then
        if NEW.table_name  != OLD.table_name or
           NEW.column_name != OLD.column_name
        then
            raise exception 'Updating table_name or column_name not allowed.';
        end if;
    end if;

    NEW.audit_data_type := coalesce(
        NEW.audit_data_type,
        @extschema@.fn_get_or_create_audit_data_type(
            @extschema@.fn_get_column_data_type(NEW.table_name, NEW.column_name)
        ),
        0
    );

    if NEW.table_pk is null then
        my_pk_col := @extschema@.fn_get_table_pk_col(NEW.table_name);

        if my_pk_col is null then
            NEW.table_pk := 0;
        else
            if my_pk_col = NEW.column_name then
                NEW.table_pk := NEW.audit_field;
            else
                NEW.table_pk := @extschema@.fn_get_or_create_audit_field (
                                    NEW.table_name,
                                    my_pk_col
                                );
            end if;
        end if;
    end if;

    if NEW.active and NEW.table_pk = 0 then
        raise exception 'Cannot audit table %: No PK column found',
            NEW.table_name;
    end if;

    return NEW;
end
 $_$
    language plpgsql;


do language plpgsql
 $$
declare
    my_version  integer[];
    my_cmd      text;
begin
    -- If on PostgreSQL 9.3 or above, add a DDL trigger to run
    -- fn_update_audit_fields() automatically. Use EXECUTE to avoid syntax
    -- errors during installation on older versions.

    my_version := regexp_matches(version(), 'PostgreSQL (\d)+\.(\d+)\.(\d+)');

    if my_version >= array[9,3,0]::integer[] then
        my_cmd := 'CREATE OR REPLACE FUNCTION fn_update_audit_fields_event_trigger() '
               || 'returns event_trigger '
               || 'language plpgsql as '
               || '   $function$ '
               || 'begin '
               || '     perform fn_update_audit_fields(); '
               || 'end '
               || '   $function$; ';

        execute my_cmd;

        my_cmd := 'CREATE EVENT TRIGGER tr_update_audit_fields ON ddl_command_end '
               || '    WHEN TAG IN (''ALTER TABLE'', ''CREATE TABLE'', ''DROP TABLE'') '
               || '    EXECUTE PROCEDURE fn_update_audit_fields_event_trigger(); ';

        execute my_cmd;
    end if;
end;
 $$;

