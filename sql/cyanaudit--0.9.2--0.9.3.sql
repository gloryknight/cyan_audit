do language plpgsql
 $$
declare
    my_version  integer[];
    my_cmd      text;
begin
    -- If on PostgreSQL 9.3.3 or above, add a DDL trigger to run
    -- fn_update_audit_fields() automatically. Use EXECUTE to avoid syntax
    -- errors during installation on older versions.

    my_version := regexp_matches(version(), 'PostgreSQL (\d)+\.(\d+)\.(\d+)');

    if my_version >= array[9,3,3]::integer[] then
        my_cmd := E'CREATE OR REPLACE FUNCTION @extschema@.fn_update_audit_fields_event_trigger() \n'
               || E'returns event_trigger \n'
               || E'language plpgsql as \n'
               || E'   $function$ \n'
               || E'begin \n'
               || E'     perform * \n'
               || E'        from @extschema@.tb_audit_field \n'
               || E'       limit 1 \n'
               || E'         for update; \n'
               || E'\n'
               || E'     if found then \n'
               || E'         perform @extschema@.fn_update_audit_fields(); \n'
               || E'     end if; \n'
               || E'exception \n'
               || E'     when insufficient_privilege \n'
               || E'     then return; \n'
               || E'end \n'
               || E'   $function$; \n';

        execute my_cmd;
    end if;
end;
 $$;


-- fn_update_audit_fields
CREATE OR REPLACE FUNCTION @extschema@.fn_update_audit_fields() returns void as
 $_$
begin
    perform *
       from @extschema@.tb_audit_field
      where audit_field = 0;
        for update;

    if not found then
        insert into @extschema@.tb_audit_data_type
             values (0, '[unknown]');
        insert into @extschema@.tb_audit_field
             values (0, '[unknown]','[unknown]', 0, 0, false);
    end if;

    with tt_audit_fields as
    (
        select coalesce(
                   af.audit_field,
                   @extschema@.fn_get_or_create_audit_field(
                       a.attrelid::regclass::varchar,
                       a.attname::varchar
                   )
               ) as audit_field,
               (a.attrelid is null and af.active) as stale
          from pg_attribute a
          join pg_constraint cn
            on cn.conrelid = a.attrelid
           and cn.contype = 'p'
           and array_length(cn.conkey, 1) = 1
           and a.attnum > 0
           and a.attisdropped is false
          join pg_namespace n
            on cn.connamespace = n.oid
           and n.nspname::varchar = 'public'
     full join @extschema@.tb_audit_field af
            on a.attrelid::regclass::varchar = af.table_name
           and a.attname::varchar = af.column_name
    )
    update @extschema@.tb_audit_field af
       set active = false
      from tt_audit_fields afs
     where afs.stale
       and afs.audit_field = af.audit_field;
end
 $_$
    language 'plpgsql';

