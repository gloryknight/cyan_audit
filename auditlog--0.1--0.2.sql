SELECT pg_catalog.pg_extension_config_dump('@extschema@.tb_audit_field', '');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.tb_audit_data_type', '');

do 
 $$ 
declare 
    my_statement text; 
begin 
    for my_function in
        select p.proname 
          from pg_extension e 
          join pg_depend d 
            on d.refobjid = e.oid 
          join pg_proc p 
            on d.objid = p.oid 
         where e.extname = 'auditlog' 
           and p.proname like 'fn_log_audit_event_%'
    loop
        execute 'alter extension auditlog drop function '||my_function||'()'; 
    end loop;
end
 $$;

CREATE OR REPLACE FUNCTION @extschema@.fn_update_audit_event_log_trigger_on_table
(
    in_table_name   varchar
)
returns void as
 $_$
use strict;

my $table_name = $_[0];

return if $table_name =~ 'tb_audit_.*';

my $colnames_q = "select audit_field, column_name "
               . "  from @extschema@.tb_audit_field "
               . " where table_name = '$table_name' "
               . "   and active = true ";

my $colnames_rv = spi_exec_query($colnames_q);

if( $colnames_rv->{'processed'} == 0 )
{
    my $q = "select @extschema@.fn_drop_audit_event_log_trigger('$table_name')";
    eval{ spi_exec_query($q) };
    elog(ERROR, $@) if $@;
    return;
}

my $pk_q = "select @extschema@.fn_get_table_pk_col('$table_name') as pk_col ";

my $pk_rv = spi_exec_query($pk_q);

my $pk_col = $pk_rv->{'rows'}[0]{'pk_col'};

unless( $pk_col )
{
    elog(NOTICE, 'pk_col is null');
    return;
}

my $fn_q = "CREATE OR REPLACE FUNCTION "
         . "    @extschema@.fn_log_audit_event_$table_name()\n"
         . "returns trigger as \n"
         . " \$_\$\n"
         . "-- THIS FUNCTION AUTOMATICALLY GENERATED. DO NOT EDIT\n"
         . "DECLARE\n"
         . "    my_row_pk_val       integer;\n"
         . "    my_old_row          record;\n"
         . "    my_new_row          record;\n"
         . "    my_recorded         timestamp;\n"
         . "BEGIN\n"
         . "    if current_setting('@extschema@.enabled') = '0' then\n"
         . "        return my_new_row;\n"
         . "    end if;\n"
         . "    \n"
         . "    perform @extschema@.fn_set_last_audit_txid();\n"
         . "    \n"
         . "    my_recorded := clock_timestamp();\n"
         . "    \n"
         . "    if( TG_OP = 'INSERT' ) then\n"
         . "        my_row_pk_val := NEW.$pk_col;\n"
         . "    else\n"
         . "        my_row_pk_val := OLD.$pk_col;\n"
         . "    end if;\n"
         . "    \n"
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
          .  "        perform @extschema@.fn_new_audit_event(\n "
          .  "                    $audit_field,\n"
          .  "                    my_row_pk_val,\n"
          .  "                    my_recorded,\n"
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

eval { spi_exec_query($fn_q) };
elog(ERROR, $@) if $@;

my $tg_q = "CREATE TRIGGER tr_log_audit_event_$table_name "
         . "   after insert or update or delete on $table_name for each row "
         . "   execute procedure @extschema@.fn_log_audit_event_$table_name()";
eval { spi_exec_query($tg_q) };
 $_$
    language 'plperl';

