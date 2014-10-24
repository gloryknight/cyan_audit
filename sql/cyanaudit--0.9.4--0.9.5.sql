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



-- fn_get_or_create_audit_field
CREATE OR REPLACE FUNCTION @extschema@.fn_get_or_create_audit_field
(
    in_table_name       varchar,
    in_column_name      varchar,
    in_audit_data_type  integer default null
)
returns integer as
 $_$
declare
    my_audit_field   integer;
    my_active        boolean;
begin
    select audit_field
      into my_audit_field
      from @extschema@.tb_audit_field
     where table_name = in_table_name
       and column_name = in_column_name;

    -- if we do not yet know about the field
    if not found then
        -- set it active if another field in this table is already active
        select active
          into my_active
          from @extschema@.tb_audit_field
         where table_name = in_table_name
         order by active desc
         limit 1;

        -- If no columns of this table are known, 
        -- or this table is currently being logged,
        if my_active is distinct from false then
            -- set it active if the column is currently real in the db
            select count(*)::integer::boolean
              into my_active
              from information_schema.columns
             where table_schema = 'public'
               and table_name = in_table_name
               and column_name = in_column_name;
        end if;

        insert into @extschema@.tb_audit_field
        (
            table_name,
            column_name,
            active,
            audit_data_type
        )
        values
        (
