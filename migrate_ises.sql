\set ON_ERROR_STOP
\set ECHO all

BEGIN;

-- This is for on dev where this doesn't exist
do language plpgsql
 $_$
begin
    if (select count(*) from pg_namespace where nspname = 'audit_log') = 0 then
        create schema audit_log;
    end if;
end;
 $_$;

-- Create schema and extension for new audit log
create schema auditlog;
create extension auditlog schema auditlog;


-- Save old audit field states and deactivate them all
create table tt_audit_field as
select *
  from tb_audit_field;

update public.tb_audit_field set active = false;

-- Now that nothing else is creating audit events, rotate.
select public.fn_rotate_audit_events();

-- Drop views so we can change the columns used by them, below
drop view public.vw_audit_log;
drop view public.vw_audit_transaction_statement;
drop view public.vw_audit_transaction_statement_cet;

-- Modify existing tb_audit_event so child tables are modified
alter table public.tb_audit_event rename column entity to uid;
alter table public.tb_audit_event rename column transaction_id to txid;
alter table public.tb_audit_event rename column process_id to pid;
alter table public.tb_audit_event rename column op_sequence to audit_event;
alter table public.tb_audit_event 
    add column audit_transaction_type integer;

-- This needs to be set before we start logging to the new table
select setval('auditlog.sq_pk_audit_event', nextval('sq_op_sequence'));

-- Populate audit data types
insert into auditlog.tb_audit_data_type
select audit_data_type, name 
  from public.tb_audit_data_type;

select setval('auditlog.sq_pk_audit_data_type', max(audit_data_type))
  from auditlog.tb_audit_data_type;

-- Populate audit transaction types
insert into auditlog.tb_audit_transaction_type
select * from public.tb_audit_transaction_type;

select setval('auditlog.sq_pk_audit_transaction_type', max(audit_transaction_type))
  from auditlog.tb_audit_transaction_type;

-- Populate tb_audit_field, which will install logging triggers
alter table auditlog.tb_audit_field
    disable trigger tr_check_audit_field_validity;

insert into auditlog.tb_audit_field
select * from public.tb_audit_field;

select setval('auditlog.sq_pk_audit_field', max(audit_field))
  from auditlog.tb_audit_field;

alter table auditlog.tb_audit_field
    enable trigger tr_check_audit_field_validity;

-- Now let's be sure we didn't miss anything
select auditlog.fn_update_audit_fields();

-- Turn it on!
update auditlog.tb_audit_field af
   set active = tt.active
  from tt_audit_field tt
 where tt.audit_field = af.audit_field
   and tt.active;


-- These columns were using fn_get_procpid_entity()
-- alter table tb_program    alter column creator  set default auditlog.fn_get_audit_uid();
-- alter table tb_program    alter column modifier set default auditlog.fn_get_audit_uid();
-- alter table tb_project    alter column creator  set default auditlog.fn_get_audit_uid();
-- alter table tb_project    alter column modifier set default auditlog.fn_get_audit_uid();
-- alter table tb_reset      alter column creator  set default auditlog.fn_get_audit_uid();
-- alter table tb_reset      alter column modifier set default auditlog.fn_get_audit_uid();
-- alter table tb_task       alter column creator  set default auditlog.fn_get_audit_uid();
-- alter table tb_task       alter column modifier set default auditlog.fn_get_audit_uid();
-- alter table tb_reset_task alter column creator  set default auditlog.fn_get_audit_uid();
-- alter table tb_reset_task alter column modifier set default auditlog.fn_get_audit_uid();

create or replace function public.fn_get_procpid_entity ()
returns integer as 
 $_$
    select current_setting('auditlog.uid')::integer;
 $_$
    language sql;
    
create or replace function public.fn_set_procpid_entity
(
    in_entity integer
) returns integer as
$_$
    select auditlog.fn_set_audit_uid($1);
$_$
    language sql;

do language plpgsql 
 $_$
declare
    my_table    varchar;
begin
    for my_table in
        select relname 
          from pg_class c 
          join pg_namespace n 
            on c.relnamespace = n.oid 
         where n.nspname = 'audit_log' 
           and c.relkind = 'r'
           and c.relname like 'tb_audit_event_%'
    loop
        execute 'alter table audit_log.' || my_table 
             || ' set schema auditlog';

        execute 'alter table auditlog.' || my_table
             || ' no inherit public.tb_audit_event';

        execute 'alter table auditlog.' || my_table
             || ' inherit auditlog.tb_audit_event';

        execute 'alter extension auditlog add table auditlog.' || my_table;

    end loop;
end;
 $_$;

alter table tb_system_audit_field 
    drop constraint tb_system_audit_field_audit_field_fkey;

alter table tb_system_audit_field
    add foreign key (audit_field) references auditlog.tb_audit_field;

-- Drop functions
drop function public.fn_archive_audit_events(interval);
drop function public.fn_audit_event_log_trigger_updater() cascade;
drop function public.fn_check_audit_field_validity() cascade;
drop function public.fn_drop_audit_event_log_trigger(varchar);
drop function public.fn_get_or_create_audit_data_type(varchar);
drop function public.fn_get_or_create_audit_field(varchar, varchar, bool);
drop function public.fn_get_or_create_audit_transaction_type(varchar);
drop function public.fn_get_or_create_system_audit_field(int, int);
drop function public.fn_label_audit_transaction(varchar, bigint);
drop function public.fn_label_last_audit_transaction(varchar);
drop function public.fn_new_audit_event(int, int, varchar, int, bigint, anyelement, anyelement);
drop function public.fn_new_system_audit_field(int, int);
drop function public.fn_redirect_audit_events() cascade;
drop function public.fn_rotate_audit_events();
drop function public.fn_update_audit_event_log_trigger_on_table(varchar);
drop function public.fn_update_audit_fields();
drop function public.fn_expire_procpid_entities();
drop function public.fn_get_my_last_transaction_id();
drop function public.fn_set_my_last_transaction_id(bigint);
drop function public.fn_undo_my_last_transaction();
drop function public.fn_undo_transaction(bigint);
drop function public.fn_get_all_table_columns();


-- Drop tables
drop table public.tb_audit_event_current;
drop table public.tb_audit_event;
drop table public.tb_audit_field;
drop table public.tb_audit_data_type;
drop table public.tb_procpid_entity;

-- Redefine CET statement view

CREATE OR REPLACE VIEW auditlog.vw_audit_transaction_statement_cet as
   select ae.txid, ae.recorded,
          (case
          when ae.row_op = 'I' then
               'INSERT INTO ' || af.table_name || ' ('
               || array_to_string(array_agg('['||af.column_name||']'), ',')
               || ') values ('
               || array_to_string(array_agg(coalesce(
                    fn_quote_literal_simple(
                       case when adt.name = 'interval'
                            then extract(days from ae.new_value::interval)::text
                            when adt.name like 'timestamp%'
                            then date_trunc('ms', ae.new_value::timestamp)::text
                            else ae.new_value
                       end
                    ),'NULL')), ',') ||');'
          when ae.row_op = 'U' then
               'UPDATE ' || af.table_name || ' set '
               || array_to_string( array_agg('['||af.column_name||'] = '
               || coalesce(fn_quote_literal_simple(
                      case when adt.name = 'interval'
                           then extract(days from ae.new_value::interval)::text
                           when adt.name like 'timestamp%'
                           then date_trunc('ms', ae.new_value::timestamp)::text
                           else ae.new_value::text
                      end
                  ),'NULL')), ', ')
               || ' where [' || afpk.column_name || '] = '
               || fn_quote_literal_simple(ae.row_pk_val) || ';'
          when ae.row_op = 'D' then
               'DELETE FROM ' || af.table_name || ' where [' || afpk.column_name
               || '] = ''' || ae.row_pk_val || ''';'
          end)::varchar as query
     from auditlog.tb_audit_event ae
     join auditlog.tb_audit_field af using(audit_field)
     join auditlog.tb_audit_data_type adt using(audit_data_type)
     join auditlog.tb_audit_field afpk on af.table_pk = afpk.audit_field
     join public.tb_system_audit_field saf
       on af.audit_field = saf.audit_field and saf.system = 4 -- CET
 group by af.table_name, ae.row_op, ae.audit_event, afpk.column_name,
          ae.row_pk_val, ae.txid, ae.recorded
 order by ae.recorded, ae.audit_event;

alter database ises set search_path to public, auditlog;

COMMIT;

----------------------------------------------------------------

BEGIN;

do language plpgsql 
 $_$
declare
    my_table    varchar;
begin
    for my_table in
        select relname 
          from pg_class c 
          join pg_namespace n 
            on c.relnamespace = n.oid 
         where n.nspname = 'auditlog' 
           and c.relkind = 'r'
           and c.relname like 'tb_audit_event_%'
    loop
        execute 'alter table auditlog.' || my_table
             || ' add foreign key(audit_field)'
             || ' references auditlog.tb_audit_field';

        execute 'alter table auditlog.' || my_table
             || ' add foreign key(audit_transaction_type)'
             || ' references auditlog.tb_audit_transaction_type';
    end loop;
end;
 $_$;


update auditlog.tb_audit_event ae
   set audit_transaction_type = at.audit_transaction_type
  from public.tb_audit_transaction at
 where at.transaction_id = ae.txid;

do language plpgsql
 $_$
begin
    if (
         select count(*)
           from pg_class c
           join pg_namespace n
             on c.relnamespace = n.oid
          where c.relname = 'tb_audit_transaction_archive'
            and n.nspname = 'audit_log'
       ) > 0
    then
        update auditlog.tb_audit_event ae
           set audit_transaction_type = at.audit_transaction_type
          from audit_log.tb_audit_transaction_archive at
         where at.transaction_id = ae.txid;

        drop table audit_log.tb_audit_transaction_archive;
    end if;
end;
 $_$;
 

drop table public.tb_audit_transaction;
drop table public.tb_audit_transaction_type;
drop sequence if exists sq_pk_audit_transaction;

-- Drop schema
drop schema audit_log;

COMMIT;
