drop function fn_rotate_audit_events();

alter sequence @extschema@.sq_pk_audit_event MAXVALUE 2147483647 CYCLE;

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

    if current_setting('@extschema@.enabled') = '0' then
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
         raise notice 'Undefined function call. Please reinstall auditlog.';
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

my $ext_q = "ALTER EXTENSION auditlog ADD FUNCTION @extschema@.fn_log_audit_event_$table_name()";

eval { spi_exec_query($ext_q) };
 $_$
    language 'plperl';



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
      where e.extname = 'auditlog'
        and p.proname = my_function_name;

    if found then
        execute 'alter extension auditlog drop function '
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




--- RENAME EXTENSION
update pg_extension
   set extname = 'cyanaudit'
 where extname = 'auditlog';

