ALTER TABLE @extschema@.tb_audit_field
    DROP COLUMN audit_data_type;

DROP TABLE @extschema@.tb_audit_data_type;

DROP FUNCTION @extschema@.fn_get_or_create_audit_data_type(varchar);

ALTER TABLE ONLY tb_audit_event DROP COLUMN pid;
ALTER TABLE ONLY tb_audit_event_current DROP COLUMN pid;


-- vw_audit_transaction_statement
CREATE OR REPLACE VIEW @extschema@.vw_audit_transaction_statement as
   select ae.txid,
          ae.recorded,
          @extschema@.fn_get_email_by_audit_uid(ae.uid) as user_email,
          att.label as description,
          (case
          when ae.row_op = 'I' then
               'INSERT INTO ' || af.table_name || ' ('
               || array_to_string(array_agg('"'||af.column_name||'"'), ',')
               || ') VALUES ('
               || array_to_string(array_agg(coalesce(
                    quote_literal(ae.new_value), 'NULL'
                  )), ',') ||');'
          when ae.row_op = 'U' then
               'UPDATE ' || af.table_name || ' SET '
               || array_to_string(array_agg(af.column_name||' = '||coalesce(
                    quote_literal(ae.new_value), 'NULL'
                  )), ', ') || ' WHERE ' || afpk.column_name || ' = '
               || quote_literal(ae.row_pk_val) || ';'
          when ae.row_op = 'D' then
               'DELETE FROM ' || af.table_name || ' WHERE ' || afpk.column_name
               ||' = '||quote_literal(ae.row_pk_val) || ';'
          end)::varchar as query
     from @extschema@.tb_audit_event ae
     join @extschema@.tb_audit_field af using(audit_field)
     join @extschema@.tb_audit_field afpk on af.table_pk = afpk.audit_field
left join @extschema@.tb_audit_transaction_type att using(audit_transaction_type)
 group by af.table_name, ae.row_op, afpk.column_name,
          ae.row_pk_val, ae.txid, ae.recorded,
          att.label, @extschema@.fn_get_email_by_audit_uid(ae.uid)
 order by ae.recorded;




-- vw_audit_transaction_statement_inverse
CREATE OR REPLACE VIEW @extschema@.vw_audit_transaction_statement_inverse AS
   select ae.txid,
          (case ae.row_op
           when 'D' then
                'INSERT INTO ' || af.table_name || ' ('
                || array_to_string(
                     array_agg('"'||af.column_name||'"'),
                   ',') || ') values ('
                || array_to_string(
                     array_agg(coalesce(
                         quote_literal(ae.old_value), 'NULL'
                     )),
                   ',') ||')'
          when 'U' then
               'UPDATE ' || af.table_name || ' set '
               || array_to_string(array_agg(
                    af.column_name||' = '||coalesce(
                        quote_literal(ae.old_value), 'NULL'
                    )
                  ), ', ') || ' where ' || afpk.column_name || ' = '
               || quote_literal(ae.row_pk_val)
          when 'I' then
               'DELETE FROM ' || af.table_name || ' where '
               || afpk.column_name ||' = '||quote_literal(ae.row_pk_val)
          end)::varchar as query
     from @extschema@.tb_audit_event ae
     join @extschema@.tb_audit_field af using(audit_field)
     join @extschema@.tb_audit_field afpk on af.table_pk = afpk.audit_field
    where ae.txid = in_txid
 group by af.table_name, ae.row_op, afpk.column_name,
          ae.row_pk_val, ae.recorded
 order by ae.recorded desc;

