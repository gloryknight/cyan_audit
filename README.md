Cyan Audit
==========

Cyan Audit is a PostgreSQL extension providing comprehensive and
easily-searchable logs of DML (INSERT/UPDATE/DELETE) activity in your database.

With Cyan Audit you can:

* Log any table with a PK, regardless of schema.
* Search logs by querying a simple view.
* Toggle logging on a column-by-column basis using an easy config table.
* Attribute every operation to a specific application user.
* Label any operation with a human-readable description.
* Back up and restore logs with confidence using supplied Perl scripts.
* Rotate & drop old logs automatically using a supplied Perl script.
* Keep years of logs online comfortably with automatic archival to your cheap tablespace.
* Effectively "undo" any recorded transaction by playing its operations in reverse.
* Save time with a design focused on ease of setup and maintenance.

Cyan Audit:

* is written entirely in SQL and PL/pgSQL (except Perl cron scripts).
* is Trigger-based.
* supports PostgreSQL 9.6 and newer.
* has been production tested since 2012.

For installation and usage instructions please see doc/cyanaudit.md.

