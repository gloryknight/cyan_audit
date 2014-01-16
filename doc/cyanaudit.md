![Cyan Audit Logo](https://db.tt/IrZ3wQpr)

Synopsis
========

Cyan Audit is a powerful PostgreSQL extension for in-database logging of DDL.


Introduction
============

How do you keep track of who modified the contents of your database?

Most of the time, such logging is implemented in the application layer, meaning
that every action your application takes has to have extra code just to log the
action. Therefore, if you forget to add the code to log the action, it will
never be logged! This is a big headache for the application developer who just
wants to get the code written.

Cyan Audit aims to solve this problem by providing an easy and powerful logging
system that requires minimal modification to your application and is installed
easily and cleanly as a PostgreSQL extension.

Cyan Audit can selectively log DDL on a column-by-column basis, and you can
select which tables/columns to log using a simple UPDATE command.

You can also turn off logging entirely for just your session if you'd like to
perform bulk administrative actions without clogging up your log table.

The contents of the log are available through a view which can be queried easily
based on recorded timestamp, table/column, userid who performed the action, PK
value of the affected row, and more.

One of the handiest features, however, is the ability to "undo" a transaction. A
simple function call will issue SQL statements to reverse every data
modification logged for the given transaction ID.

Does that sound interesting? Good, let's get started.


Requirements & Limitations
==========================

* PostgreSQL 9.1.7 or above is required for basic functionality.
* PostgreSQL 9.3.3 or above is required for auto-detection of DDL and automatic
  modification of configuration.
* Requires languages `plpgsql` and `plperl`.
* Currently only tables in schema `public` can be logged.
* Currently only tables having a single-column PK of type integer can be logged.


Installation
============

1. Unpack the source files into a directory in your file system:

        # tar zxvf cyanaudit-X.X.X.tar.gz

2. Now go into the directory and simply run `make install`. 

   Note: You will have to have `pg_config` in your path in order for this to
   work.  You can see if it is in your path by issuing the command `which
   pg_config`. If it is not found in your search path, you may need to add it by
   modifying your login script (e.g. .bashrc) to add your `/usr/pgsql-9.3/bin` (or
   equivalent) directory to your `$PATH`.

3. Configure custom_variable_classes in `postgresql.conf` (Only for PostgreSQL 9.1):

        custom_variable_classes = 'cyanaudit'

4. Log into your database as user `postgres` and create the schema and extension:

        mydb=# CREATE SCHEMA cyanaudit;
        mydb=# CREATE EXTENSION cyanaudit SCHEMA cyanaudit;

5. Configure database-specific settings (optional):

        alter database mydb set cyanaudit.archive_tablespace = 'big_slow_drive';
        alter database mydb set cyanaudit.user_table = 'tb_entity';
        alter database mydb set cyanaudit.user_table_uid_col = 'entity';
        alter database mydb set cyanaudit.user_table_email_col = 'email_address';
        alter database mydb set cyanaudit.user_table_username_col = 'username';

   The `archive_tablespace` setting is the name of the tablespace to which you
   would like your logs to be archived when they are rotated. This allows you to
   put your older logs on cheaper, slower media than that of your main database.

   The `user_table`, `user_table_uid_col` and `user_table_email_col` settings
   allow Cyan Audit to display the userid and email of your users when you view
   the logs.

   If the `user_table_username_col` is set, Cyan Audit will be able to match a
   userid from your application to a user in your database cluster. For this to
   work, your database cluster usernames must match those of your application.

6. Force all sessions to reconnect and pick up the database config settings (optional):

        select pg_terminate_backend(pid) 
          from pg_stat_activity 
         where pid != pg_backend_pid();

7. Instruct Cyan Audit to catalog all tables & columns in your database, and
   install the logging trigger onto all tables:

        mydb=# SELECT cyanaudit.fn_update_audit_fields();

   **WARNING**: This function will hold an exclusive lock on all of your tables
   until the function returns. On a test database with about 2500 columns, this
   took 20 seconds. Please make sure you run this at a time when it is
   acceptable for your tables to be locked for up to a minute.

8. (Optional) Add the Cyan Audit schema to your search path:

        mydb=# ALTER DATABASE mydb SET search_path = public, cyanaudit;

   This will keep you from having to preceed every relation and function in this
   extension with `cyanaudit.`.

At this point, logging should be turned on for all supported tables. Perform
some DML (INSERT, UPDATE, DELETE) and then do `select * from
cyanaudit.vw_audit_log` and see if your changes have been logged.


Configuring Your Application
============================

Passing the User ID
-------------------
The only way for Cyan Audit to know the application-level userids of the people
modifying the database is for your application to convey that information using
the `fn_set_audit_uid()` function.

This function must be called before any modifications take place, so it is
normally called immediately after your application obtains a database handle:

    SELECT fn_set_audit_uid( userid );

Of course, you will need to bind the userid where you see the `userid` parameter
above.

The userid you set here will persist for the remainder of the session. If you
want to un-set it at any point, you can set the userid to -1 or use the `DISCARD
ALL` SQL command to discard all session-specific settings.

Labeling Transactions
---------------------
Cyan Audit has the ability to attach textual labels to your transactions, so
that your application can provide easily understood descriptions of what was
happening in that transaction, for example to indicate the module of code that
made the modification, or to allow the layperson to understand the logs a little
bit more easily.

To use this feature, your application must call
`fn_last_label_audit_transaction()` immediately after any transaction you wish
to label:

    SELECT fn_label_last_audit_transaction('User Logged In');

This will apply the given label to all log entries associated with the last
logged transaction.

If you are wanting to label a transaction from within that transaction, e.g. in
a PL/pgSQL function, you can call the `fn_label_audit_transaction()` function:

    SELECT fn_label_audit_transaction('User Added');

This will label all actions performed within the current transaction up until
the point that this function is called. If future modifications are made after
the function is called, they will not be labeled until the function is caalled
again.

The text string you specify here is automatically added to a distinct list of
labels that can be used over and over without copying the actual text onto each
transaction, so it is very efficient from the perspective of disk usage.


Database Objects
================

Following is a list of Cyan Audit's views, tables and functions of interest to
the user.

For the sake of simplicity, I will assume for the rest of this document that you
have added the cyanaudit schema to your search path (See Installation Step 8).


Views
-----

* `vw_audit_log`

  This view is the primary interface into the log. It has the following columns:

  * `recorded` - timestamp of action. This column is indexed.
  * `uid` - userid of the user performing the action
  * `user_email` - email address of user performing the action. 
  * `txid` - database transaction ID of this operation. This column is indexed.
  * `description` - Textual description of transaction. See "Labeling
                    Transactions" above for more information.
  * `table_name` - Name of table of action. Indexed together with column_name.
  * `column_name` - Name of column of action. Indexed together with table_name.
  * `pk_val` - Integer PK value of the row that was modified
  * `op` - Type of operation ('I', 'U' or 'D' for INSERT, UPDATE or DELETE)
  * `old_value` - Value of column before DELETE or UPDATE
  * `new_value` - Value of column after INSERT or UPDATE

  It is most efficient to query the audit log based on the indexed columns.
  Therefore, restricting by `recorded` , `txid` or `table_name` + `column_name`
  will return results very quickly, whereas using the other columns for your
  searches will be quite slow unless you have already well restricted the output
  set using the indexed columns.

* `vw_audit_transaction_statement`

  This view constructs SQL statements from the data recorded in the log. The
  reconstructed statements for a transaction are not necessarily the same as the
  original statements used to effect that transaction's changes, but they
  produce the same result if executed.

  The view has the following columns:

  * `txid` - Transaction ID. This column is indexed.
  * `recorded` - When the changes of this statement were originally made
  * `user_email` - Email of user who made the changes in this transaction
  * `description` - Textual description of transaction. See "Labeling
                    Transactions" above for more information.
  * `query` - The re-constructed query


Tables
------

* `tb_audit_field`

  This table controls the tables & columns that Cyan Audit logs. It is updated
  automatically whenever you call `fn_update_audit_fields()` (which happens
  automatically in 9.3.3 and above whenever any DDL such as a CREATE TABLE is
  executed).

  This table has the following columns:

  * `audit_field` - This is the PK column of the tb_audit_field table.
  * `table_name` - table of column being logged
  * `column_name` - column being logged
  * `audit_data_type` - Data type of the column.
  * `table_pk` - audit_field row for this table's PK column.
  * `active` - boolean indicating whether this column is enabled for logging.

  The only column that has any use to you as the administrator is the `active`
  boolean.  You can turn on or off logging for a particular column by updating
  this field. As an example, the following command disables logging for table
  'foo', column 'bar':

        UPDATE tb_audit_field 
           SET active = false 
         WHERE table_name = 'foo' 
           AND column_name = 'bar';

  When Cyan Audit discovers a new column in your database, it will automatically
  set `active` to `true` (i.e. it will automatically log the column) unless the
  following conditions are met:

  1. There is at least one more column of this table already in tb_audit_field
  2. There is no column of this table in tb_audit_field that has `active = true`


Functions
---------

* `fn_label_audit_transaction( label )`

  Please see the section "Labeling Transactions" under "Configuring Your
  Application" for details on this function.

* `fn_set_audit_uid()`

  Sets the userid for the current session.

  Please see "Passing the User ID" under "Cnofiguring Your Application for more
  details on this function.

* `fn_get_audit_uid()`

  Returns the userid previously set with `fn_set_audit_uid()`. If none was set,
  returns 0.

* `fn_get_last_audit_txid()`

  This function returns the txid of the last transaction logged for the current
  user as indicated by fn_get_audit_uid()

* `fn_undo_transaction( txid )`

  Pass a transaction ID into this function, and it will issue commands to
  reverse the modifications that were logged for this transaction.

* `fn_undo_last_transaction()`
  
  A shortcut for `fn_undo_transaction( fn_get_last_audit_txid() )`

* `cyanaudit.enabled`

  This configuration parameter can be set on a session-by-session basis to
  disable logging for the current session only:

        SET cyanaudit.enabled = 0;

  Logging can be re-enabled by setting this back to 1 or issuing the `DISCARD
  ALL` command.


Log Maintenance
===============

Log Storage Overview
--------------------

In order to understand the functionality of the Cyan Audit log maintenance
scripts, it is first necessary to understand the way the log tables are managed.

When an event is logged, it is inserted into `tb_audit_event`, which is actually
just an empty parent table. Inheriting this table is another table called
`tb_audit_event_current`, to which all of the log events are redirected. 

`tb_audit_event_current` lives in your default tablespace, which is usually your
fastest media, which will not have enough room to let this table grow
indefinitely. When this table becomes too large, it needs to be re-located into
your archive tablespace (specified by the confiration parameter
`cyanaudit.archive_tablespace`), which is usually your larger, slower and
cheaper media. `cyanaudit_log_rotate.pl` is used to perform this log rotation.

When the current events are rotated into the archive, the table
`tb_audit_event_current` is renamed to e.g. `tb_audit_event_20131229_0401`,
where the table name reflects the time the table was rotated. A new
`tb_audit_event_current` in your default tablespae is then created to receive
subsequent events.

Under a typical server load, you will want to rotate your audit events on a
weekly basis. This will eventually create a large number of partitions, one per
week, dating back as far as you're willing or able to store on your server.

At a certain point, however, you will want to begin deleting old logs to make
room on your server. The `cyanaudit_dump.pl` script allows you to back up and
remove logs past a certain age. The backup files are compressed as they are
created, and they are generally quite small. 

If you want to restore archived logs, for example to do forensics on long lost
data, or in the case that you've added storage to your server and want to make
more logs available online, then you can use the `cyanaudit_restore.pl` script
to import them back from the file into the database.

Below are more detailed descriptions of the three log maintenance scripts, which
are automatically installed to your PostgreSQL bin/ directory.


Log Maintenance Scripts
-----------------------

* `cyanaudit_log_rotate.pl`

  This script rotates the current audit events into the archive. It takes only
  the following parameters, which are optional if the standard PG environment
  variables are set:

        Usage: cyanaudit_log_rotate.pl [ options ... ]
        Options:
          -d db      Connect to given database
          -h host    Connect to given host
          -p port    Connect on given port
          -U user    Connect as given user

  To run this script every Sunday at midnight, you would create the following
  entry in your crontab (change values and paths as appropriate):

        0 0 * * 0   /usr/pgsql-9.3/bin/cyanaudit_log_rotate.pl -U postgres -d mydb

* `cyanaudit_dump.pl`

  This script has two functions. The first is to export archived logs to files.
  The second is to remove archived logs older than a certain age from your
  database.

  Normally you will use both functions simultaneously for the purposes of
  exporting old logs and removing them from the database. However, it can also
  be used to back up the logs you have not yet exported, even if you are not
  ready to delete them. This will allow you to restore these tables in the event
  of a database crash, since your regular `pg_dump` backups will not catch these
  tables as they are owned by the extension.

        Usage: cyanaudit_dump.pl -m months_to_keep [ options ... ]
        Options:
          -d db      Connect to database by given Name
          -U user    Connect to database as given User
          -h host    Connect to database on given Host
          -p port    Connect to database on given Port
          -a         Back up All audit tables
          -c         Clobber (overwrite) existing files. Default is to skip these.
          -r         Remove table from database once it has been archived
          -z         gzip output file
          -o dir     Output directory (default current directory)

  If you'd like to use the script to ensure that all of your tables are backed
  up into files in `/var/lib/pgsql/backups`, as well as to remove logs over 6
  months old, you can run it every day with a crontab entry as follows:
  
        0 0 * * *   /usr/pgsql-9.3/bin/cyanaudit_dump.pl -U postgres -d mydb -a -r -m 6 -z -o /var/lib/pgsql/backups

  It is highly recommended to use the -z option in all cases, as the output is
  extremely large if uncompressed, and also compresses quite well.

* `cyanaudit_restore.pl`

  This script takes a file created by `cyanaudit_dump.pl` and restores it back
  into the database. 

        Usage: cyanaudit_restore.pl [ options ] file [...]
        Options:
          -d db      Connect to given database
          -h host    Connect to given host
          -p port    Connect on given port
          -U user    Connect as given user

  Due to the nature of the way the compresed archive files are stored, it is not
  possible to give a percentage-based progress indicator when restoring from a
  compressed archive.


About
=====

License
-------

Cyan Audit is released under the PostgreSQL license. Please see the accompanying
LICENSE file for more details.


Author
------

Cyan Audit is written and maintained by Moshe Jacobson -- <moshe@neadwerx.com>

Development sponsored by Nead Werx, Inc. -- <http://www.neadwerx.com>
