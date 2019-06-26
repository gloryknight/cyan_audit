2.2.0 -> 2.2.1
--------------
- Fix exception when trying to log an operation after fn_set_transaction_label()
  is called followed by RESET ALL or DISCARD ALL.
- Now installs on Windows thanks to patch from Benjamin Hayes!
- Other minor install script fixes

2.1 -> 2.2.0
------------
- Change back to major.minor.revision versioning for pgxn compatibility
- Fix various problems restoring cyanaudit schema during pg_restore
- Completely update documentation

2.0 -> 2.1
----------
- Code cleanup including removal of a lot of stale extension-related code
- Made cyanaudit_log_rotate.pl more robust and unintrusive re. lock contention
- Removed make-based install and added install.pl for installation and upgrades.
- Increased robustness of config verification functions
- Improved exception handling in fn_log_audit_event()
- cyanaudit_log_rotate.pl can now rotate without pruning (-n/-s/-a now optional)

1.0.2 -> 2.0
------------
- Cyan Audit is no longer an extension. Just run the .sql script to install. All
  code is installed to cyanaudit schema and is not relocatable. This is to
  support Amazon RDS.
- Removed the need for database-level GUCs (for compatibility with Amazon RDS).
- Removed various deprecated functions
- cyanaudit_log_rotate.pl now has -p flag to prune without rotating.
- cyanaudit_log_rotate.pl now offers rotation by size and age as well as number.
- cyanaudit_log_rotate.pl now waits for in-flight transactions to finish before
  modifying the constraints or table spaces of archived partitions, so as to
  allow transactions to be labeled before locking the partition in which the
  transaction data resides.
- Added function fn_set_transaction_label() to set the label of a transaction
  for future actions in the transaction. This avoids the need to call
  fn_label_transaction() or fn_label_last_transaction() after DML.

1.0.1 -> 1.0.2
--------------
- Order of operations in fn_archive_partition changed to reduce duration of
  exclusive lock on tb_audit_event, which could cause lockups during archival.

1.0.0 -> 1.0.1
--------------
- Logging function now catches permission and other exceptions to prevent
  bringing down the system when events cannot be logged.
- cyanaudit_log_rotate.pl now exits cleanly if there are no events to rotate.
- Instructions now correctly reflect that package is a .zip not .tar.gz
- Installation package now includes all necessary files
- Partition range constraints use >= instead of > now.

0.9.7 -> 1.0.0
--------------
- cyanaudit_restore.pl now sets up constraints on restored partition before
  setting up inheritance, to avoid locking parent table while constraints are
  validated.
- cyanaudit_log_rotate.pl now correctly recreates constraints on archived table.
- fn_create_partition_indexes() now works correctly with non-default tablespace.
- fn_prune_archive(), called by cyanaudit_log_rotate.pl, now correctly drops the
  extension dependency before dropping the table.
- cyanaudit_restore.pl now correctly archives the partition before restoring.


0.9.6 -> 0.9.7
--------------
- Can now log tables with multi-column PKs.
- Specific trigger function no longer created for each table.  Instead, a
  generic logging function is called by customized triggers.
- cyanaudit_{dump/restore}.pl use md5 checksums to validate file integrity.
- cyanaudit_dump.pl checks currency of output file based on mtime and last
  recorded timestamp of the partition (log table) being backed up.
- cyanaudit_dump.pl is no longer responsible for dropping old tables.
  cyanaudit_log_rotate.pl is now responsible for archiving and dropping tables.
- Log partition tables are now named according to when they start rather than
  end, and there is no more tb_audit_current (so that it doesn't have to be
  renamed upon rotate, and can be backed up and restored correctly).
- DROP EXTENSION without CASCADE now silently drops all triggers and log tables.
- CREATE EXTENSION cyanaudit; now does not require schema to be specified.
  Extension will automatically create and install into the 'cyanaudit' schema.
- Lots and lots (and lots) of code cleanup.

0.9.5 -> 0.9.6
--------------
- Added support for logging tables in schemas other than 'public'. Yay!
- Fixed event trigger, which was inserting schema-qualified table names into
  tb_audit_field, which as a result disabled logging on all existing fields.
- Quieted notices regarding truncated trigger and function names
- Created vw_audit_transaction_statement_inverse for pulling the inverse of a
  transaction's effective statements (now used by fn_undo_transaction()).
- Simplified code by now requiring at least PostgreSQL version 9.3.3.
- Dropped tb_audit_event.pid, which was not used.
- Dropped unneeded tb_audit_field.audit_data_type and tb_audit_data_type.

0.9.4 -> 0.9.5
--------------
- Fixed incorrect behavior when cyanaudit.user_table_username_col was not
  correctly set.
- Fixed bug when specifying -c option to cyanaudit_dump.pl, such that some files
  would be overwritten when they already existed, and some would not be written
  even if they didn't yet exist.
- Fixed a couple of minor aesthetic issues with cyanaudit_dump.pl
- Fixed auto-activate logic for new rows in tb_audit_field
- Fixed malfunction when database name had characters that needed to be quoted
- Fixed behavior with cyanaudit_restore.pl restoring archives not in current dir

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
