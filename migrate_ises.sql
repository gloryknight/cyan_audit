-- Create schema and extension for new audit log
create schema auditlog;
create extension auditlog schema auditlog;


-- Save old audit field states and deactivate them all
create table tt_audit_field as
select *
  from tb_audit_field
 where active;

update public.tb_audit_field set active = false;

-- Now that nothing else is creating audit events, rotate.
select public.fn_rotate_audit_events();

-- Modify existing tb_audit_event so child tables are modified
alter table public.tb_audit_event rename column entity to uid;
alter table public.tb_audit_event rename column transaction_id to txid;
alter table public.tb_audit_event rename column process_id to pid;
alter table public.tb_audit_event rename column op_sequence to audit_event;

-- This needs to be set before we start logging to the new table
select setval('auditlog.sq_pk_audit_event', max(audit_event))
  from public.tb_audit_event;

-- Populate audit data types
insert into auditlog.tb_audit_data_type
select audit_data_type, name 
  from public.tb_audit_data_type

select setval('auditlog.sq_pk_audit_data_type', max(audit_data_type))
  from auditlog.tb_audit_data_type;

-- Populate tb_audit_field, which will install logging triggers
alter table auditlog.tb_audit_field
    disable trigger tr_check_audit_field_validity;

insert into auditlog.tb_audit_field
select * from tt_audit_field;

alter table auditlog.tb_audit_field
    enable trigger tr_check_audit_field_validity;

select setval('auditlog.sq_pk_audit_field', max(audit_field))
  from auditlog.tb_audit_field;

-- Now let's be sure we didn't miss anything
select auditlog.fn_update_audit_fields();


-- These columns were using fn_get_procpid_entity()
alter table tb_program    alter column creator  set default fn_get_audit_uid();
alter table tb_program    alter column modifier set default fn_get_audit_uid();
alter table tb_project    alter column creator  set default fn_get_audit_uid();
alter table tb_project    alter column modifier set default fn_get_audit_uid();
alter table tb_reset      alter column creator  set default fn_get_audit_uid();
alter table tb_reset      alter column modifier set default fn_get_audit_uid();
alter table tb_task       alter column creator  set default fn_get_audit_uid();
alter table tb_task       alter column modifier set default fn_get_audit_uid();
alter table tb_reset_task alter column creator  set default fn_get_audit_uid();
alter table tb_reset_task alter column modifier set default fn_get_audit_uid();

create or replace function fn_set_procpid_entity(int) returns void as
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
           and c.relname like 'tb_audit_event_%';
    loop
        execute 'alter table audit_log.' || my_table 
             || ' set schema auditlog';

        execute 'alter table auditlog.' || my_table
             || ' no inherit public.tb_audit_event';

        execute 'alter table auditlog.' || my_table
             || ' inherit auditlog.tb_audit_event';

        execute 'alter extension auditlog add table ' || my_table;

    end loop;
end;
 $_$;

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
drop function public.fn_set_procpid_entity(int);
drop function public.fn_get_procpid_entity();
drop function public.fn_expire_procpid_entities();


-- Drop tables
drop sequence if exists sq_pk_audit_transaction;
drop view vw_audit_log;
drop view vw_audit_transaction_statement;
drop table audit_log.tb_audit_transaction_archive;
drop table public.tb_audit_transaction;
drop table public.tb_audit_event_current;
drop table public.tb_audit_event;
drop table public.tb_audit_field;
drop table public.tb_audit_event_type;
drop table public.tb_procpid_entity;

/* RUN THESE LATER

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
           and c.relname like 'tb_audit_event_%';
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
 where at.transaction_id = ae.transaction_id;

update auditlog.tb_audit_event ae
   set audit_transaction_type = at.audit_transaction_type
  from public.tb_audit_transaction_archive at
 where at.transaction_id = ae.transaction_id;

*/
