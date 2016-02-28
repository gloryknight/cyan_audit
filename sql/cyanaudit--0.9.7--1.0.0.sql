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

