-- fn_create_partition_indexes
CREATE OR REPLACE FUNCTION @extschema@.fn_create_partition_indexes
(
    in_table_name   varchar
)
returns void as
 $_$
declare
    my_index_columns        varchar[];
    my_index_column         varchar;
    my_index_name           varchar;
    my_tablespace_clause    varchar;
begin
    my_index_columns := array[ 'recorded', 'txid', 'audit_field' ];

    foreach my_index_column in array my_index_columns
    loop
        my_index_name := format( 'ix_%s_%s', right( in_table_name, -3 ), my_index_column );

        perform *
           from pg_index i
           join pg_class ci
             on i.indexrelid = ci.oid
           join pg_class c
             on i.indrelid = c.oid
           join pg_namespace n
             on c.relnamespace = n.oid
          where n.nspname = '@extschema@'
            and c.relname = in_table_name
            and ci.relname = my_index_name;

        if not found then
            -- Use tablespace of in_table_name. If default, this will be empty.
            select format( 'TABLESPACE %I', t.spcname )
              into my_tablespace_clause
              from pg_class c
              join pg_tablespace t
                on c.reltablespace = t.oid
              join pg_namespace n
                on c.relnamespace = n.oid
               and n.nspname = '@extschema@'
             where c.relname = in_table_name;

            execute format( 'CREATE INDEX %I on @extschema@.%I ( %I ) %s',
                            my_index_name,
                            in_table_name,
                            my_index_column,
                            coalesce( my_tablespace_clause, '' )
                          );
        end if;
    end loop;
exception
    when undefined_object then
        raise exception 'cyanaudit: Missing setting for cyanaudit.archive_tablespace. Aborting.';
end
 $_$
    language plpgsql;



-- Returns names of tables dropped
CREATE OR REPLACE FUNCTION @extschema@.fn_prune_archive
(
    in_keep_qty     integer
)
returns setof varchar as
 $_$
declare
    my_table_name           varchar;
begin
    if in_keep_qty < 0 then
        raise exception 'in_keep_qty may not be negative.';
    end if;

    for my_table_name in
        select c.relname
          from pg_class c
          join pg_namespace n
            on c.relnamespace = n.oid
           and n.nspname = '@extschema@'
         where c.relkind = 'r'
           and c.relname ~ '^tb_audit_event_\d{8}_\d{4}$'
         order by c.relname desc
        offset in_keep_qty + 1
    loop
        execute format( 'ALTER EXTENSION cyanaudit DROP TABLE @extschema@.%I', my_table_name );
        execute format( 'DROP TABLE @extschema@.%I', my_table_name );
        return next my_table_name;
    end loop;

    return;
end
 $_$
    language plpgsql strict;


-- fn_create_new_partition
CREATE OR REPLACE FUNCTION @extschema@.fn_create_new_partition
(
    in_new_table_name varchar default 'tb_audit_event_' || to_char(now(), 'YYYYMMDD_HH24MI')
)
returns varchar as
 $_$
begin
    if in_new_table_name is null then
        raise exception 'in_new_table_name cannot be null';
    end if;

    if in_new_table_name !~ '^tb_audit_event_\d{8}_\d{4}$' then
        raise exception 'Table name must conform to format "tb_audit_event_########_####"';
    end if;

    perform *
       from pg_class c
       join pg_namespace n
         on c.relnamespace = n.oid
      where c.relname = in_new_table_name
        and n.nspname = '@extschema@';

    if found then
        return null;
    end if;

    SET LOCAL client_min_messages to WARNING;

    execute format( 'CREATE TABLE @extschema@.%I '
                 || '( '
                 || '  LIKE @extschema@.tb_audit_event INCLUDING STORAGE INCLUDING CONSTRAINTS '
                 || ') ',
                    in_new_table_name );

    execute format( 'GRANT insert, '
                 || '      select (audit_transaction_type, txid), '
                 || '      update (audit_transaction_type) '
                 || '   ON @extschema@.%I '
                 || '   TO public ',
                    in_new_table_name );

    execute format( 'ALTER EXTENSION cyanaudit ADD TABLE @extschema@.%I', in_new_table_name );

    SET LOCAL client_min_messages to NOTICE;

    return in_new_table_name;
end
 $_$
    language plpgsql;



-- fn_setup_partition_inheritance
CREATE OR REPLACE FUNCTION @extschema@.fn_setup_partition_inheritance
(
    in_partition_name   varchar
)
returns void as
 $_$
begin
    if in_partition_name is null then
        raise exception 'in_partition_name must not be null.';
    end if;

    -- See if inheritance is already set up for this table
    perform *
       from pg_inherits i
       join pg_class c_child
         on i.inhrelid = c_child.oid
       join pg_namespace n_child
         on c_child.relnamespace = n_child.oid
        and n_child.nspname = '@extschema@'
       join pg_class c_parent
         on i.inhparent = c_parent.oid
       join pg_namespace n_parent
         on c_parent.relnamespace = n_parent.oid
        and n_parent.nspname = '@extschema@'
      where c_child.relname = in_partition_name
        and c_parent.relname = 'tb_audit_event';

    -- If not, then set it up!
    if not found then
        execute format( 'ALTER TABLE @extschema@.%I INHERIT @extschema@.tb_audit_event',
                        in_partition_name );
    end if;
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
        perform @extschema@.fn_create_partition_indexes( my_partition_name );
        perform @extschema@.fn_activate_partition( my_partition_name );
        perform @extschema@.fn_setup_partition_constraints( my_partition_name );
        perform @extschema@.fn_setup_partition_inheritance( my_partition_name );
    end if;

    return my_partition_name;
end
 $_$
    language plpgsql;



