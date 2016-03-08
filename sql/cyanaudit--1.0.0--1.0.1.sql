-- fn_setup_partition_constraints
CREATE OR REPLACE FUNCTION @extschema@.fn_setup_partition_constraints
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
    if in_table_name is null then
        raise exception 'Table name cannot be null';
    end if;

    my_constraint_name := 'partition_range_chk';

    perform *
       from pg_constraint cn
       join pg_class c
         on cn.conrelid = c.oid
       join pg_namespace n
         on c.relnamespace = n.oid
      where n.nspname = '@extschema@'
        and cn.conname = my_constraint_name
        and c.relname = in_table_name;

    if found then
        execute format( 'alter table @extschema@.%I drop constraint %I',
                        in_table_name, my_constraint_name );
    end if;

    execute format( 'select min(recorded), max(recorded), min(txid), max(txid) from @extschema@.%I',
                    in_table_name )
       into my_min_recorded, my_max_recorded, my_min_txid, my_max_txid;

    if in_table_name = @extschema@.fn_get_active_partition_name() then
        execute format( 'ALTER TABLE @extschema@.%I add constraint %I '
                     || ' CHECK( recorded >= %L )',
                        in_table_name, my_constraint_name, coalesce( my_min_recorded, now() ) );
    elsif my_min_recorded is not null then
        execute format( 'ALTER TABLE @extschema@.%I add constraint %I '
                    || ' CHECK( recorded between %L and %L and txid between %L and %L )',
                       in_table_name, my_constraint_name,
                       my_min_recorded, my_max_recorded, my_min_txid, my_max_txid );
    end if;

    -- Install FK to tb_audit_field if not present
    perform *
       from pg_constraint cn
       join pg_class c
         on cn.conrelid = c.oid
       join pg_namespace n
         on c.relnamespace = n.oid
       join pg_class cf
         on cn.confrelid = cf.oid
       join pg_namespace nf
         on cf.relnamespace = nf.oid
      where n.nspname = '@extschema@'
        and c.relname = in_table_name
        and nf.nspname = '@extschema@'
        and cf.relname = 'tb_audit_field';

    if not found then
        execute format( 'ALTER TABLE @extschema@.%I '
                     || '  ADD FOREIGN KEY ( audit_field ) '
                     || '      references @extschema@.tb_audit_field',
                        in_table_name );
    end if;

    -- Install FK to tb_audit_transaction_type if not present
    perform *
       from pg_constraint cn
       join pg_class c
         on cn.conrelid = c.oid
       join pg_namespace n
         on c.relnamespace = n.oid
       join pg_class cf
         on cn.confrelid = cf.oid
       join pg_namespace nf
         on cf.relnamespace = nf.oid
      where n.nspname = '@extschema@'
        and c.relname = in_table_name
        and nf.nspname = '@extschema@'
        and cf.relname = 'tb_audit_transaction_type';

    if not found then
        execute format( 'ALTER TABLE @extschema@.%I '
                     || '  ADD FOREIGN KEY ( audit_transaction_type ) '
                     || '      references @extschema@.tb_audit_transaction_type',
                        in_table_name );
    end if;
end
 $_$
    language plpgsql;




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
    my_clock_timestamp      timestamp;
BEGIN
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

    if @extschema@.fn_get_config('enabled') = '0' then
        return my_new_row;
    end if;

    my_pk_cols          := TG_ARGV[0]::varchar[];
    my_audit_fields     := TG_ARGV[1]::varchar[];
    my_column_names     := TG_ARGV[2]::varchar[];

    my_clock_timestamp  := clock_timestamp(); -- same for all entries from this invocation

    perform @extschema@.fn_set_last_txid();

    -- Given:  my_pk_cols::varchar[]           = ARRAY[ 'column foo',bar ]
    -- Result: my_pk_vals_constructor::varchar = 'select ARRAY[ $1."column foo", $1.bar ]::varchar[]'
    select 'select ARRAY[' || string_agg( '$1.' || quote_ident(pk_col), ',' ) || ']::varchar[]'
      into my_pk_vals_constructor
      from ( select unnest(my_pk_cols::varchar[]) as pk_col ) x;

    -- Execute the result using my_new_row in $1 to produce the following result:
    -- my_pk_vals::varchar[] = ARRAY[ 'val1', 'val2' ]
    EXECUTE my_pk_vals_constructor
       into my_pk_vals
      using my_new_row; -- To allow undoing updates to pk columns, logged pk_vals are post-update.

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
                     || '( audit_field, recorded, pk_vals, uid, row_op, old_value, new_value ) '
                     || 'VALUES(  $1, $2, $3, $4, $5::char(1), $6, $7 ) ',
                        my_column_name
                      )
          USING my_audit_field,
                my_clock_timestamp,
                my_pk_vals,
                @extschema@.fn_get_current_uid(),
                TG_OP,
                my_old_value,
                my_new_value;
    END LOOP;

    RETURN NEW;
EXCEPTION
    WHEN undefined_function THEN
         raise notice 'cyanaudit: Missing internal function. Please reinstall.';
         return NEW;
    WHEN undefined_column THEN
         raise notice 'cyanaudit: Attempt to log deleted column. Please run @extschema@.fn_update_audit_fields() as superuser.';
         return NEW;
    WHEN insufficient_privilege THEN
         raise notice 'cyanaudit: Incorrect permissions. Operation not logged';
         return NEW;
    WHEN others THEN
         raise notice 'cyanaudit: Unknown exception. Operation not logged';
         return NEW;
END
$_$;

COMMENT ON FUNCTION @extschema@.fn_log_audit_event()
    IS 'Trigger function installed on all tables logged by the cyanaudit extension.';

