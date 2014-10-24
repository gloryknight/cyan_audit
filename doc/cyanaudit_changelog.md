0.9.4 -> 0.9.5
--------------
- Fixed incorrect behavior when cyanaudit.user_table_username_col was not
  correctly set.
- Fixed bug when specifying -c option to cyanaudit_dump.pl, such that some files
  would be overwritten when they already existed, and some would not be written
  even if they didn't yet exist.
- Fixed a couple of minor aesthetic issues with cyanaudit_dump.pl
- Fixed auto-activate logic for new rows in tb_audit_field

0.9.3 -> 0.9.4
--------------
- Fixed error where pg_restore was trying to create triggers pointing to
  functions that did not exist

0.9.2 -> 0.9.3
--------------
- Fixed error where event trigger threw an exception when running as
  unprivileged user.
- Tables restored with cyanaudit_restore.pl are now placed into the correct
  tablespace and also altered to be owned by the cyanaudit extension.
- More intelligent naming of tables created by cyanaudit_restore.pl. Name of
  table will be taken from filename if it looks reasonable, otherwise it will be
  named dynamically according to the last recorded event in the table.
- Fixed concurrency problem when restoring a cyanaudit-enabled database with
  pg_restore -j ##.

0.9.1 -> 0.9.2
--------------
- Fixed cyanaudit_log_rotate.pl to correctly move archived table to
  archive_tablespace and create new table in pg_default tablespace.
- Fixed two functions that were being created in public schema instead of
  extensions's schema.
- Configuration parameters are now stored on the database instead of in
  postgresql.conf. After upgrade, you may remove the cyanaudit.* config
  parameters from postgresql.conf. However, for PostgreSQL 9.1, you must retain
  the `custom_variable_classes = cyanaudit` setting.

0.9.0 -> 0.9.1
--------------
- Do not install event trigger on PostgreSQL 9.3.2 or below, as it is not
  handled properly by pg_dump.

0.4 -> 0.9.0
------------
- Made Cyan Audit ready for PGXN
- Changed to semantic version number

0.3 -> 0.4
----------
- Changed extension name to "cyanaudit" and fixed branding throughout.
- Added check for PostgreSQL 9.1.7 
- Added support for archiving and restoring audit data to/from files.
- Better error checking throughout and during installation.
- Moved log rotation function to an external Perl script to avoid race condition
- Fixed bug with audit_event sequence going out of range
- Added DDL trigger for Postgres 9.3 and above

0.2 -> 0.3
----------
- Lots of fixes for pg_dump/pg_restore and schema qualification
- Added sequences sq_pk_audit_field and sq_pk_audit_data_type as config data

0.1 -> 0.2
----------
- fn_update_audit_log_trigger_on_table()
- Config tables tb_audit_field and tb_audit_data_type
