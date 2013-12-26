0.1 -> 0.2
----------
fn_update_audit_log_trigger_on_table()
Config tables tb_audit_field and tb_audit_data_type

0.2 -> 0.3
----------
Lots of fixes for pg_dump/pg_restore and schema qualification
Added sequences sq_pk_audit_field and sq_pk_audit_data_type as config data

0.3 -> 0.4
----------
Changed extension name to "cyanaudit" and fixed branding throughout.
Added check for PostgreSQL 9.1.7 
Added support for archiving and restoring audit data to/from files.
Better error checking throughout and during installation.
Moved log rotation function to an external Perl script to avoid race condition
Fixed bug with audit_event sequence going out of range
Added DDL trigger for Postgres 9.3 and above

