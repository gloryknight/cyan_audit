-- vw_audit_transaction_statement
CREATE OR REPLACE VIEW @extschema@.vw_audit_transaction_statement as
   select ae.txid,
          ae.recorded,
          @extschema@.fn_get_email_by_audit_uid(ae.uid) as user_email,
          att.label as description,
          (case
          when ae.row_op = 'I' then
               'INSERT INTO ' || af.table_name || ' ('
               || array_to_string(array_agg('"'||af.column_name||'"'), ',')
               || ') VALUES ('
               || array_to_string(array_agg(coalesce(
                    quote_literal(ae.new_value)||'::'||adt.name, 'NULL'
                  )), ',') ||');'
          when ae.row_op = 'U' then
               'UPDATE ' || af.table_name || ' SET '
               || array_to_string(array_agg(af.column_name||' = '||coalesce(
                    quote_literal(ae.new_value)||'::'||adt.name, 'NULL'
                  )), ', ') || ' WHERE ' || afpk.column_name || ' = '
               || quote_literal(ae.row_pk_val) || '::' || adtpk.name || ';'
          when ae.row_op = 'D' then
               'DELETE FROM ' || af.table_name || ' WHERE ' || afpk.column_name
               ||' = '||quote_literal(ae.row_pk_val)||'::'||adtpk.name||';'
          end)::varchar as query
     from @extschema@.tb_audit_event ae
     join @extschema@.tb_audit_field af using(audit_field)
     join @extschema@.tb_audit_data_type adt using(audit_data_type)
     join @extschema@.tb_audit_field afpk on af.table_pk = afpk.audit_field
     join @extschema@.tb_audit_data_type adtpk
       on afpk.audit_data_type = adtpk.audit_data_type
left join @extschema@.tb_audit_transaction_type att using(audit_transaction_type)
 group by af.table_name, ae.row_op, afpk.column_name,
          ae.row_pk_val, adtpk.name, ae.txid, ae.recorded,
          att.label, @extschema@.fn_get_email_by_audit_uid(ae.uid)
 order by ae.recorded;


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
    WHEN undefined_object THEN
         raise notice 'Cyan Audit configuration invalid. Logging disabled.';
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


with tt_tables as
(
    select distinct table_name
      from @extschema@.tb_audit_field
     where active
)
select fn_update_audit_event_log_trigger_on_table( table_name )
  from tt_tables;



