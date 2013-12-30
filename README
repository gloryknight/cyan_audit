Cyan Audit
==========

Cyan Audit is a PostgreSQL extension for in-database audit logging of DML that
occurs in your database. It allows for forensics to be performed on past data
modifications, for the purposes of finding who was responsible for making a
particular change, or when exactly it occurred.

Some features of Cyan Audit:
* Enabling or disabling of logging on a column-by-column basis.
* Correlation of transactions with the application-level userid
  that performed them (requires a modification to your application).
* Ability to store years of logs in an efficiently accessible manner.
* Backup and restore of logs to and from compressed files.
* Customizable log retention period with automated archival.
* Support for custom textual descriptions to be attached to any transaction
  in a space-efficient manner, allowing the layman to better understand the
  logs, or differentiating two similar modifications based on where they
  happened in the application code (requires application changes).
* Ability to "undo" any transaction by reversing the recorded changes.

For installation instructions and further documentation, please see
the documentation for the cyanaudit module in doc/cyanaudit.md.

