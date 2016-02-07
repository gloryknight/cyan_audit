Upgrading Cyan Audit
====================

To upgrade Cyan Audit between versions with no upgrade sql script, please follow
these directions precisely:

1. Run the old version of `cyanaudit_log_rotate.pl`

2. Run the new version of `cyanaudit_dump.pl` and let it complete

3. As user `postgres`, block all other connections to database: 

         ALTER DATABASE <yourdb> CONNECTION LIMIT 0;
         SELECT pg_terminate_backend(pid)
           FROM pg_stat_activity
          WHERE datname = '<yourdb>'
            AND usename != 'postgres';

   Any changes made by connected superusers will not be logged.

4. Run the old version of `cyanaudit_log_rotate.pl` AGAIN

5. Run the new version of `cyanaudit_dump.pl` AGAIN 

6. Install the files for the new version of Cyan Audit using `make install` from
   the base directory of the extracted tar.gz file.

7. As superuser, drop the old extension & schema:

        DROP EXTENSION cyanaudit CASCADE;
        DROP SCHEMA cyanaudit CASCADE;

9. Install the cyanaudit extension in your database:
    
        CREATE EXTENSION cyanaudit;
        SELECT cyanaudit.fn_update_audit_fields('public'); --run on all logged schemas
        SELECT cyanaudit.fn_create_event_trigger();

10. Reenable connections to your database:

        ALTER DATABASE <yourdb> CONNECTION LIMIT -1;

11. Restore old backups using the new `cyanaudit_restore.pl`
    

