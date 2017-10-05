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
    my_archive_tablespace := @extschema@.fn_get_config( 'archive_tablespace' );

    for my_index_name in
        select ci.relname
          from pg_index i
          join pg_class ci
            on i.indexrelid = ci.oid
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

    execute format( 'alter table @extschema@.%I set tablespace %I',
                    in_partition_name, my_archive_tablespace );
exception
    when undefined_object then
        raise exception 'cyanaudit: Missing setting for cyanaudit.archive_tablespace. Aborting.';
end
 $_$
    language plpgsql strict;

