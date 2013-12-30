do language plpgsql
 $$
declare
    my_version  integer[];
    my_cmd      text;
begin
    my_version := regexp_matches(version(), 'PostgreSQL (\d)+\.(\d+)\.(\d+)');

    if my_version between array[9,3,0]::integer[] and array[9,3,2] then
        execute 'drop event trigger if exists tr_update_audit_fields';
    end if;
end
 $$;
