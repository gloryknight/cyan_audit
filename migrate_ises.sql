create table tt_active_audit_fields as
select * from tb_audit_field where active;

update public.tb_audit_field set active = false;

-- should exist:
drop function if exists public.fn_archive_audit_events(in_age interval);
drop function if exists public.fn_audit_event_log_trigger_updater() cascade;
drop function if exists public.fn_check_audit_field_validity() cascade;
drop function if exists public.fn_drop_audit_event_log_trigger(in_table_name
character varying);
drop function if exists public.fn_get_or_create_audit_data_type(in_type_name
character varying);
drop function if exists public.fn_get_or_create_audit_field(in_table_name
character varying, in_column_name character varying, in_active boolean);
drop function if exists public.fn_get_or_create_audit_transaction_type(in_label
character varying);
drop function if exists public.fn_get_or_create_system_audit_field(in_system
integer, in_audit_field integer);
drop function if exists public.fn_label_audit_transaction(in_label character varying, in_transaction_id bigint);
drop function if exists public.fn_label_last_audit_transaction(in_label character varying);
drop function if exists public.fn_new_audit_event(in_audit_field integer, in_row_pk_val integer, in_row_op character varying, in_op_sequence integer, in_transaction_id bigint, in_old_value anyelement, in_new_value anyelement);
drop function if exists public.fn_new_system_audit_field(in_system integer, in_audit_field integer);
drop function if exists public.fn_redirect_audit_events() cascade;
drop function if exists public.fn_rotate_audit_events();
drop function if exists
public.fn_update_audit_event_log_trigger_on_table(in_table_name character
varying);
drop function if exists public.fn_update_audit_fields();
drop function if exists public.fn_set_procpid_entity(int);
drop function if exists public.fn_get_procpid_entity();

-- may not actually exist:
drop function if exists public.fn_get_audit_uid();
drop function if exists public.fn_set_audit_uid(integer);

------------
-- TABLES -- 
------------
--alter table tb_audit_data_type
--    set schema audit_log;
alter table tb_audit_data_type
    drop column type_oid;

--alter table tb_audit_event
--    set schema audit_log;
alter table tb_audit_event rename column entity to uid;
alter table tb_audit_event rename column transaction_id to txid;
alter table tb_audit_event rename column process_id to pid;
alter table tb_audit_event 
    alter column op_sequence set default nextval('sq_op_sequence'),
    alter column txid set default txid_current(),
    alter column pid set default pg_backend_pid();

-- alter table tb_audit_event_current          set schema audit_log;
-- alter table tb_audit_field                  set schema audit_log;
-- alter table tb_audit_transaction            set schema audit_log;
-- alter table tb_audit_transaction_archive    set schema audit_log;
-- alter table tb_audit_transaction_type       set schema audit_log;
drop view vw_audit_log;
drop view vw_audit_transaction_statement;

alter table tb_audit_transaction 
    rename column transaction_id to txid;
alter table tb_audit_transaction
    drop column audit_transaction;


drop index if exists tb_audit_event_transaction_id_idx;
drop index if exists tb_audit_event_recorded_idx;
drop index if exists tb_audit_event_audit_field_idx;
drop index if exists tb_audit_event_current_transaction_id_idx;
drop index if exists tb_audit_event_current_recorded_idx;
drop index if exists tb_audit_event_current_audit_field_idx;



---------------
-- SEQUENCES --
---------------
drop sequence if exists sq_pk_audit_event;
drop sequence if exists sq_pk_audit_transaction;

-- alter sequence sq_op_sequence set schema audit_log;

-- alter sequence sq_pk_audit_data_type set schema audit_log;
-- alter sequence sq_pk_audit_field set schema audit_log;
-- alter sequence sq_pk_audit_transaction_type set schema audit_log;


-- alter sequence sq_pk_audit_data_type 
--     owned by audit_log.tb_audit_data_type.audit_data_type;
-- alter sequence sq_pk_audit_field
--     owned by audit_log.tb_audit_field.audit_field;
-- alter sequence sq_pk_audit_transaction_type
--     owned by audit_log.tb_audit_transaction_type.audit_transaction_type;
