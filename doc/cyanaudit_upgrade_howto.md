Upgrading Cyan Audit
====================

To upgrade Cyan Audit between versions with no upgrade sql script, please follow
these directions precisely:

1. Make sure you've backed up the enabled state of your audit_fields

        CREATE TABLE tb_audit_field_backup AS
        SELECT * FROM cyanaudit.tb_audit_field;

2. Run the old version of `cyanaudit_log_rotate.pl`

3. Run the new version of `cyanaudit_dump.pl` and let it complete

4. As user `postgres`, block all other connections to database: 

        ALTER DATABASE <yourdb> CONNECTION LIMIT 0;

        SELECT pg_terminate_backend(pid)
          FROM pg_stat_activity
         WHERE usename != 'postgres'
           AND datname = '<yourdb>';

   Any changes made by connected superusers will not be logged.

5. Run the old version of `cyanaudit_log_rotate.pl` AGAIN

6. Run the new version of `cyanaudit_dump.pl` AGAIN 

7. Install the files for the new version of Cyan Audit using `make install` from
   the base directory of the extracted tar.gz file.

8. As superuser, drop the old extension & schema:

        DROP EXTENSION cyanaudit CASCADE;
        DROP SCHEMA cyanaudit CASCADE;

10. Install the cyanaudit extension in your database:
    
        CREATE EXTENSION cyanaudit;
        SELECT cyanaudit.fn_update_audit_fields('public'); --run on all logged schemas
        SELECT cyanaudit.fn_create_event_trigger();

11. Set up cyanaudit.archive_tablespace GUC:

        ALTER DATABASE <yourdb> SET cyanaudit.archive_tablespace = 'big_slow';

12. Reenable connections to your database:

        ALTER DATABASE <yourdb> CONNECTION LIMIT -1;

13. Restore audit_field states:

        UPDATE cyanaudit.tb_audit_field af
           SET enabled = afb.enabled
          FROM tb_audit_field_backup afb
         WHERE afb.table_schema = af.table_schema
           AND afb.table_name = af.table_name
           AND afb.column_name = af.column_name;

14. Restore old backups using the new `cyanaudit_restore.pl`

15. Finish configuring other cyanaudit GUCs
    

