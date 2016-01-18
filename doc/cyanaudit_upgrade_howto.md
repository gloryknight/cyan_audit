Upgrading Cyan Audit
====================

To upgrade Cyan Audit from pre-1.0 to 1.0 (or between versions with no upgrade
sql script) with minimal downtime, please follow these directions precisely:

1. Run the old version of cyanaudit_log_rotate.pl

2. Run the new version of cyanaudit_dump.pl and let it complete

3. Block non-superuser connections to database: 

    ALTER DATABASE <yourdb> CONNECTION LIMIT 0;
    
    SELECT pg_terminate_backend(pid)
      FROM pg_stat_activity
     WHERE datname = '<yourdb>'
       AND usename != 'postgres';

   Any changes made by connected superusers will not be logged.

4. Run the old version of cyanaudit_log_rotate.pl AGAIN

5. Run the new version of cyanaudit_dump.pl AGAIN 

6. As superuser, drop the old extension:

    DROP EXTENSION cyanaudit CASCADE;

7. As superuser, drop the old extension's schema (here "cyanaudit"):

    DROP SCHEMA cyanaudit CASCADE;

8. Install the files for the new version of Cyan Audit using `make install` from
   the base directory of the extracted tar.gz file.

9. Install the cyanaudit extension in your database:
    
    CREATE EXTENSION cyanaudit;
    SELECT cyanaudit.fn_update_audit_fields('public'); --run on all logged schemas
    SELECT cyanaudit.fn_create_event_trigger();

10. Reenable connections to your database:

    ALTER DATABASE <yourdb> CONNECTION LIMIT -1;

11. Restore old backups using the new cyanaudit_restore.pl
    

