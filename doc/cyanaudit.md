![Cyan Audit Logo](https://bitbucket.org/neadwerx/cyanaudit/raw/master/doc/cyanaudit_logo.png)

Overview
========

Cyan Audit provides an easy-to-use SQL-searchable log of who changed your data
and when. It is a stable, powerful and mature DML logging extension for
PostgreSQL 9.6+. 

Cyan Audit is written entirely in pl/pgsql and is trigger-based, so it does not
require admin privileges at the cluster or system levels.


Basic Usage
===========

Install Cyan Audit:

    $ tar zxvf cyanaudit-X.X.zip
    $ cd cyanaudit-X.X
    $ ./install.pl -d dbname [ -h dbhost -p dbport -U dbuser ]

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
    | user_email  | Derived from uid (see Final Configuration Steps below).  
    | txid        | Indexed for easy lookup.  
    | table_name  | Affected table (schema-qualified if not in search_path)
    | column_name | Column whose values are given in old_value and new_value.
    | pk_vals[]   | affected row's pk values (after update) cast as text.
    | op          | operation ('I', 'U', or 'D')  
    | old_value   | NULL on 'I'. Never NULL on 'D'. IS DISTINCT FROM new_value.
    | new_value   | NULL on 'D'. Never NULL on 'I'. IS DISTINCT FROM old_value.

With `\pset format wrapped`, these columns fit nicely  across a ~200 col display

Disable logging for a particularly noisy column:
    
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

Set the description for all future DML in this transaction:

    SELECT cyanaudit.fn_set_transaction_label( 'Change last name' );

Set the description for all past, unlabled DML in this transaction:

    SELECT cyanaudit.fn_label_transaction( 'User disabled due to inactivity' );

Set the label for all unlabled DML in this session's last logged transaction:

    SELECT cyanaudit.fn_label_last_transaction( 'User enabled' );

Un-set uid and last txid, turn logging back on:
    
    DISCARD ALL;



Final Configuration Steps
=========================

Tell Cyan Audit how to populate `vw_audit_log.user_email` based on logged uids:

    UPDATE cyanaudit.tb_config SET value = 'users'          where name = 'user_table'
    UPDATE cyanaudit.tb_config SET value = 'user'           where name = 'user_table_uid_col'
    UPDATE cyanaudit.tb_config SET value = 'email_address'  where name = 'user_table_email_col''

Enable setting uid automatically when `current_user` matches `users.username`: 

    UPDATE cyanaudit.tb_config SET value = 'username' WHERE name = 'user_table_username_col';

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

Cron to rotate logs weekly, dropping archive tables over 10 in quantity, over
20GB in size, and over 30 days in age:

    0 0 * * 0  /usr/pgsql-X.X/bin/cyanaudit_log_rotate.pl -U postgres -d app_db -n 10 -s 20 -a 30

Cron to back up logs nightly (skips tables already having current backup):

    5 0 * * *  /usr/pgsql-X.X/bin/cyanaudit_dump.pl -U postgres -d app_db /mnt/backups/cyanaudit/app_db/

Restore a couple backup files to an existing Cyan Audit installation:

    # /usr/pgsql/9.3/bin/cyanaudit_restore.pl -U postgres -d app_db \
        /mnt/backups/cyanaudit/app_db/tb_audit_event_20180101_1200.gz \
        /mnt/backups/cyanaudit/app_db/tb_audit_event_20180102_1200.gz


Reinstalling or Upgrading Cyan Audit In Place
=============================================

If you wish to reinstall Cyan Audit without dropping it, simply re-run the
install script and it will automatically re-install the same version you
currently have installed:

    ./install.pl -d app_db -U postgres

If you'd like to upgrade an existing installation, simply use the -V flag with
the version you'd like to install:
    
    ./install.pl -d app_db -U postgres -V 2.2


Uninstalling Cyan Audit
=======================

Cyan Audit lives entirely in the cyanaudit schema, and can be dropped as follows:

    psql> DROP SCHEMA cyanaudit CASCADE

Cyan Audit's scripts can be removed as follows:

    # rm /var/lib/psql-X.X/bin/[cC]yanaudit*


Removing & Reinstalling Cyan Audit
==================================

Sometimes a completely fresh install might be needed (please report the bug if
so). In this case, it will involve a little bit of downtime, but you can back up
all of your logs, remove, reinstall and restore the logs, as follows:

1.  Rotate logs using `cyanaudit_log_rotate.pl`
2.  Back up all logs using `cyanaudit_dump.pl`
3.  Shut down database access (e.g. by turning off pgbouncer)
4.  Run `cyanaudit_dump.pl` again to catch the last bit of logs
5.  Install new cyanaudit scripts using `./install.pl` from cyanaudit directory
6.  Create backup of `cyanaudit.tb_audit_field` (for the enabled values) and
    `cyanaudit.tb_config` for the new installation:  
    `CREATE TABLE public.tb_audit_field_backup AS SELECT * FROM cyanaudit.tb_audit_field;`  
    `CREATE TABLE public.tb_cyanaudit_config_backup AS SELECT * FROM cyanaudit.tb_config;` 
7.  `DROP SCHEMA cyanaudit CASCADE;`
8.  `./install.pl -d app_db -h localhost`
9.  `select fn_update_audit_fields('public')` # (Also run this for any other schema being logged)
10. Restore the configs from your backups of tb_audit_field and tb_config:  
    ```
    UPDATE cyanaudit.tb_audit_field af   
       SET enabled = afb.enabled   
      FROM public.tb_audit_field_backup afb   
     WHERE afb.table_schema = af.table_schema   
       AND afb.table_name = af.table_name   
       AND afb.column_name = af.column_name;  
    UPDATE cyanaudit.tb_config c  
       SET value = ccb.value  
      FROM public.tb_cyanaudit_config_backup ccb  
     WHERE ccb.name = c.name;  
    ```
11. Re-enable database access (e.g. restart pgbouncer)
12. TEST THE SYSTEM. Log in. See if things are being logged.
13. Restore old logs using `cyanaudit_restore.pl`, remembering to restore only
    as much logs as are normally kept on the server (not the whole history!)


Important Notes
===============

* When using with pgbouncer or other connnection poolers, you must use
  session-level pooling (not statement-level or transaction-level) for
  `fn_set_current_uid()` and `fn_label_last_txid()` to have any effect.
  Additionally, you must have the pooler issue a `DISCARD ALL` command to reset
  the persistent server connection after a client disconnects.

* `fn_update_audit_fields()` will hold an exclusive lock on all of your tables
  until the function returns. On a test database with about 3500 columns, this
  took 12 seconds. Please make sure you run this at a time when it is
  acceptable for your tables to be locked for up to a minute.

* When Cyan Audit finds a new column (e.g. during `fn_update_audit_fields()`),
  it will decide the default value for `enabled` as follows:

        If any column on same table is enabled, then true.
        Else If we know of fields on this table but all are inactive, then false.
        Else If we know of no fields in this table, then:
            If any field in same schema is enabled, then true.
            Else If we know of fields in this schema but all are inactive, then false.
            Else true

* When querying `vw_audit_log`, being as specific as possible about your
  `table_name` and `column_name` will greatly speed up search results.



About
=====

* Cyan Audit is released under the __PostgreSQL license__. Please see the accompanying
  LICENSE file for more details.

* Cyan Audit is written and maintained by Moshe Jacobson -- <moshe@neadwerx.com>

* Development sponsored by Nead Werx, Inc. -- <http://www.neadwerx.com>
