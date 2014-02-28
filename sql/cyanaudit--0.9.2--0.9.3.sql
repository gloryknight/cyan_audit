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

