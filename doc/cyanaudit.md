![Cyan Audit Logo](https://db.tt/IrZ3wQpr)

Overview
========

Where did that unexpected value in the database came from?  Do you have to fix
your code? Or do you have to fix the user? Which user?

Don't waste time adding logging hooks to your application; Cyan Audit logs every
INSERT, UPDATE and DELETE, and gives you an easy-to-use view for querying the log.


Basic Usage
===========

Install Cyan Audit:

    $ su
    # tar zxvf cyanaudit-X.X.X.tar.gz
    # cd cyanaudit-X.X.X
    # make install
    # psql -U postgres -h /tmp -d app_db
    app_db# CREATE EXTENSION cyanaudit;
    app_db# select cyanaudit.fn_create_event_trigger();

Turn on logging for schemas `public` and `app_schema`:

    app_db# select cyanaudit.fn_update_audit_fields('public');
    app_db# select cyanaudit.fn_update_audit_fields('app_schema');

Search the logs by querying the view `vw_audit_log`:

    app_db=# select recorded, txid, table_name, column_name, pk_vals, op, old_value, new_value
    app_db-#   from cyanaudit.vw_audit_log
    app_db-#  where recorded > now() - interval '5 min' -- less than 5 min ago
    app_db-#    and pk_vals[1] = '7' -- the modified row's pk value is '7'
    app_db-#    and table_name = 'employees'
    app_db-#    and column_name = 'family_name';
             recorded          |  txid   | table_name | column_name | pk_vals | op | old_value | new_value
    ---------------------------+---------+------------+-------------+---------+----+-----------+-----------
     2016-02-06 16:07:32.63177 | 1901268 | employees  | family_name | {7}     | U  | Riley     | Chase
    (1 row)

The audit log view looks like this:

    | Column      | Description  
    |-------------|---------------------------------------------------------------  
    | recorded    | clock_timestamp of each logged operation.  
    | uid         | UID of application user, set with SELECT fn_set_current_uid(uid). 
    | user_email  | Derived from uid (see GUC SETTINGS below for configuring).  
    | txid        | Indexed for easy lookup.  
    | table_name  | Affected table (schema-qualified if not in search_path)
    | column_name | Column whose values are given in old_value and new_value.
    | pk_vals[]   | affected row's pk values (after update) cast as text.
    | op          | operation ('I', 'U', or 'D')  
    | old_value   | NULL on 'I'. Never NULL on 'D'. IS DISTINCT FROM old_value.
    | new_value   | NULL on 'D'. Never NULL on 'I'. IS DISTINCT FROM new_value.

With `\pset format wrapped`, these columns fit comfortably across the screen.

Toggle logging on a column-by-column basis:
    
    UPDATE cyanaudit.tb_audit_field
       SET enabled = false
     WHERE table_schema = 'app_schema'
       AND table_name = 'customers'
       AND column_name = 'last_modified';

Disable logging for the current session:

    SET cyanaudit.enabled = 0;

Play back the inverse of your last transaction:
    
    SELECT cyanaudit.fn_undo_transaction( cyanaudit.fn_get_last_txid() );

Shorthand for above:
    
    SELECT cyanaudit.fn_undo_last_transaction();



Application Hooks
=================

Set uid value to 42 for all subsequent activity in the current session:

    SELECT cyanaudit.fn_set_current_uid( 42 );

Label all unlabled, completed operations in this transaction:

    SELECT cyanaudit.fn_label_transaction( 'User disabled due to inactivity' );

Label all unlabled operations in the most-recently-committed transaction:

    SELECT cyanaudit.fn_label_transaction( 'User enabled', cyanaudit.fn_get_last_txid() );

Shorthand for above:

    SELECT cyanaudit.fn_label_last_transaction( 'User enabled' );

Un-set uid and last txid, turn logging back on:
    
    DISCARD ALL;



Final Configuration Steps
=========================

Tell Cyan Audit how to populate `vw_audit_log.user_email` based on logged uids:

    ALTER DATABASE mydb SET cyanaudit.user_table                = 'users';
    ALTER DATABASE mydb SET cyanaudit.user_table_uid_col        = 'user_id';
    ALTER DATABASE mydb SET cyanaudit.user_table_email_col      = 'email_address';

Enable setting uid automatically when `current_user` matches `users.username`: 

    ALTER DATABASE mydb SET cyanaudit.user_table_username_col   = 'username';

Set the tablespace to which rotated logs will be moved:

    ALTER DATABASE mydb SET cyanaudit.archive_tablespace        = 'big_n_slow';

Cause all sessions to reload settings by forcing reconnect (optional):

    SELECT pg_terminate_backend(pid) 
      FROM pg_stat_activity 
     WHERE pid != pg_backend_pid()
       AND datname = current_database();

Re-scan for schema changes in all tracked schemas:

    SELECT cyanaudit.fn_update_audit_fields();

Set up `:logwhere` alias by customizing this line & adding to your `.psqlrc`:

    \set logwhere 'select recorded, uid, user_email, txid, description, table_schema, table_name, column_name, pk_vals, op, old_value, new_value from cyanaudit.vw_audit_log where' 

Use the `:logwhere` alias (previous step) to see all activity from the last 5 minutes:
    
    app_db# :logwhere recorded > now() - interval '5 min';



Log Maintenance
===============

Cyan Audit's logs are divided (sharded) into partitions, which are created every
time you run `cyanaudit_log_rotate.pl`. If you ran it at 2016-01-10 09:00, it
would create a new partition called `cyanaudit.tb_audit_event_20160110_0900`.

Cron to rotate logs weekly, dropping archives after 10 weeks:

    0 0 * * 0  /usr/pgsql-9.3/bin/cyanaudit_log_rotate.pl -U postgres -d app_db -n 10

Cron to back up logs nightly (skips tables already having current backup):

    5 0 * * *  /usr/pgsql-9.3/bin/cyanaudit_dump.pl -U postgres -d app_db /mnt/backups/cyanaudit/app_db

Restore all backup files to an existing Cyan Audit installation:

    # /usr/pgsql/9.3/bin/cyanaudit_restore.pl -U postgres -d app_db /mnt/backups/cyanaudit/app_db/*.gz



Important Notes
===============
* Requires PostgreSQL 9.3.3 or above.

* Not compatible with multithreaded `pg_restore` (`-j 2+`).  `pg_restore`
  sometimes neglects to install the logging triggers on system tables, even
  though they are present in the dump. No error is emitted by `pg_restore`.

* `DROP EXTENSION cyanaudit` will require `CASCADE` in order to drop the
  logging triggers on your system tables. This is because PostgreSQL does not
  currently support extensions owning triggers. Setting up the ownership in
  `pg_depend` manually does allow the omission of `CASCADE`, but it causes
  postgres to refuse to drop a table that is being logged, because it cannot
  drop the logging trigger without dropping the extension.

* When using with pgbouncer or other connnection poolers, you must use
  session-level pooling (not statement-level or transaction-level) for
  `fn_set_current_uid()` and `fn_label_last_txid()` to have any effect.
  Additionally, you must have the pooler issue a `DISCARD ALL` command to reset
  the persistent server connection after a client disconnects.

* `fn_update_audit_fields()` will hold an exclusive lock on all of your tables
  until the function returns. On a test database with about 2500 columns, this
  took 20 seconds. Please make sure you run this at a time when it is
  acceptable for your tables to be locked for up to a minute.

* When Cyan Audit finds a new column (e.g. during `fn_update_audit_fields()`),
  it will decide the default value for `enabled` as follows:

        If any column on same table is enabled, then true.
        Else If we know of fields on this table but all are inactive, then false.
        Else If we know of no fields in this table, then:
            If any field in same schema is enabled, then true.
            Else If we know of fields in this schema but all are inactive, then false.
            Else If we know of no columns in this schema, then:
                If any column in the database is enabled, then true.
                Else If we know of fields in this database but all are inactive, then false.
                Else, true

* When querying `vw_audit_log`, being as specific as possible about your
  `table_name` and `column_name` will greatly speed up search results.



About
=====

* Cyan Audit is released under the __PostgreSQL license__. Please see the accompanying
  LICENSE file for more details.

* Cyan Audit is written and maintained by Moshe Jacobson -- <moshe@neadwerx.com>

* Development sponsored by Nead Werx, Inc. -- <http://www.neadwerx.com>

