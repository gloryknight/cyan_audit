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
         return null;
end
 $_$
    language plpgsql stable strict;

