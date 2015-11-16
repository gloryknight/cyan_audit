-- This is a script for migrating cyan audit 0.9.4 to 0.9.7 by uninstalling &
-- reinstalling while keeping the audit data intact.

CREATE OR REPLACE FUNCTION fn_migrate_cyanaudit()
returns void
language plpgsql
as $_$
declare
    my_object_description        text;
    my_cyanaudit_oid            oid;
    my_cyanaudit_schema_name    varchar;
    my_cyanaudit_schema_oid     oid;
begin
    -- Verify presence of extension
    SELECT e.oid,
           n.oid,
           n.nspname
      INTO my_cyanaudit_oid,
           my_cyanaudit_schema_oid,
           my_cyanaudit_schema_name
      FROM pg_catalog.pg_extension e
      JOIN pg_namespace n
        ON e.extnamespace = n.oid
     WHERE e.extname = 'cyanaudit';

    execute format( 'SET search_path = %I', my_cyanaudit_schema_name );

    if not found then
        raise exception 'cyanaudit extension not found in database.';
    end if;

    -- Remove dependency of audit tables on extension
    for my_object_description in
        SELECT pg_catalog.pg_describe_object(classid, objid, 0)
          FROM pg_catalog.pg_depend
         WHERE refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass 
           AND refobjid = my_cyanaudit_oid
           AND deptype = 'e'
           AND ( pg_catalog.pg_describe_object(classid, objid, 0) ~ 'table tb_audit_(field|event.*)$'
              OR pg_catalog.pg_describe_object(classid, objid, 0) ~ 'sequence sq_pk_audit_(field|transaction_type)$'
               )
         ORDER BY 1
    loop
        raise notice 'dropping % from cyanaudit extension', my_object_description;
        execute 'alter extension cyanaudit drop ' || my_object_description;
    end loop;

    drop extension cyanaudit cascade;

    -- Create tb_audit_field.table_schema if not already present
    perform *
       from information_schema.columns
      where table_schema = my_cyanaudit_schema_name
        and table_name   = 'tb_audit_field'
        and column_name  = 'table_schema';

    if not found then
        alter table tb_audit_field add column table_schema varchar not null default 'public';
        alter table tb_audit_field alter column table_schema drop default;
    end if;

    -- Rename tb_audit_field.active (if found) to enabled
    perform *
       from information_schema.columns
      where table_schema = my_cyanaudit_schema_name
        and table_name   = 'tb_audit_field'
        and column_name  = 'active';

    if found then
        alter table tb_audit_field rename column active to enabled;
    end if;

    -- Create tb_audit_field.loggable if not already present
    perform *
       from information_schema.columns
      where table_schema = my_cyanaudit_schema_name
        and table_name   = 'tb_audit_field'
        and column_name  = 'loggable';

    if not found then
        alter table tb_audit_field add column loggable boolean not null default true; 
        alter table tb_audit_field alter column loggable drop default;
    end if;
    
    -- Rename tb_audit_event.row_pk_val (if found) to pk_val
    perform *
       from information_schema.columns
      where table_schema = my_cyanaudit_schema_name
        and table_name   = 'tb_audit_event'
        and column_name  = 'row_pk_val';

    if found then
        alter table tb_audit_event
            rename column row_pk_val to pk_vals;
    end if;

    if my_cyanaudit_schema_name != 'cyanaudit' then
        execute format( 'alter schema %I rename to cyanaudit', my_cyanaudit_schema_name );
    end if;

--    create extension cyanaudit;
--
--    select fn_update_audit_fields();
end;
$_$;


    
