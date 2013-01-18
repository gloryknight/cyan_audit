create table tt_active_audit_fields as
select * from tb_audit_field where active;

update tb_audit_field set active = false;

drop function if exists public.fn_archive_audit_events(interval);
drop function if exists public.fn_audit_event_log_trigger_updater() cascade;
drop function if exists public.fn_check_audit_field_validity() cascade;
drop function if exists public.fn_drop_audit_event_log_trigger(varchar);
drop function if exists public.fn_get_audit_uid();
drop function if exists public.fn_get_or_create_audit_data_type(varchar);
drop function if exists public.fn_get_or_create_audit_field(varchar, varchar, boolean);
drop function if exists public.fn_label_audit_transaction(varchar, bigint);
drop function if exists public.fn_label_last_audit_transaction(varchar);
drop function if exists public.fn_new_audit_event(int, int, varchar, int, bigint, anyelement, anyelement);
drop function if exists public.fn_redirect_audit_events() cascade;
drop function if exists public.fn_rotate_audit_events();
drop function if exists public.fn_set_audit_uid(integer);
drop function if exists public.fn_update_audit_event_log_trigger_on_table(varchar);
drop function if exists public.fn_update_audit_fields();

alter table tb_audit_data_type          set schema audit_log;
alter table tb_audit_field              set schema audit_log;
alter table tb_audit_transaction        set schema audit_log;
alter table tb_audit_transaction_type   set schema audit_log;
alter table tb_audit_event              set schema audit_log;
alter table tb_audit_event_current      set schema audit_log;
drop view vw_audit_log;
drop view vw_audit_transaction_statement;

-- alter table audit_log.tb_audit_event drop column audit_event;

alter table audit_log.tb_audit_event rename column transaction_id to txid;
alter table audit_log.tb_audit_transaction rename column transaction_id to txid;
alter table audit_log.tb_audit_event rename column entity to uid;
alter table audit_log.tb_audit_event rename column process_id to pid;
alter sequence sq_op_sequence set schema audit_log;
alter table audit_log.tb_audit_event 
    alter column op_sequence set default nextval('audit_log.sq_op_sequence'),
    alter column txid set default txid_current(),
    alter column pid set default pg_backend_pid();
alter table audit_log.tb_audit_transaction
    drop column audit_transaction;
-- alter table audit_log.tb_audit_transaction add primary key(txid);


drop index if exists audit_log.tb_audit_event_transaction_id_idx;
drop index if exists audit_log.tb_audit_event_recorded_idx;
drop index if exists audit_log.tb_audit_event_audit_field_idx;
drop index if exists audit_log.tb_audit_event_current_transaction_id_idx;
drop index if exists audit_log.tb_audit_event_current_recorded_idx;
drop index if exists audit_log.tb_audit_event_current_audit_field_idx;


drop sequence sq_pk_audit_event;
drop sequence sq_pk_audit_transaction;

alter sequence sq_pk_audit_data_type set schema audit_log;
alter sequence sq_pk_audit_field set schema audit_log;
alter sequence sq_pk_audit_transaction_type set schema audit_log;


alter sequence sq_pk_audit_data_type 
    owned by audit_log.tb_audit_data_type.audit_data_type;
alter sequence sq_pk_audit_field
    owned by audit_log.tb_audit_field.audit_field;
alter sequence sq_pk_audit_transaction_type
    owned by audit_log.tb_audit_transaction_type.audit_transaction_type;
