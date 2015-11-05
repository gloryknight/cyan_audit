insert into pg_depend
(
    with tt_trigger_oids as
    (
        select tr.oid,
               e.oid as cyanaudit_oid
          from pg_trigger tr
          join pg_class c
            on tr.tgrelid = c.oid
          join pg_proc p
            on tr.tgfoid = p.oid
          join pg_extension e
            on e.extname = 'cyanaudit'
          join pg_namespace en
            on e.extnamespace = en.oid
         where c.relnamespace != en.oid
           and p.pronamespace = en.oid
           and not tr.tgisinternal
    ),
    tt_pg_trigger_oid as
    (
        select c.oid
          from pg_class c
          join pg_namespace n
            on c.relnamespace = n.oid
         where c.relname = 'pg_trigger'
           and n.nspname = 'pg_catalog'
    ),
    tt_pg_extension_oid as
    (
        select c.oid
          from pg_class c
          join pg_namespace n
            on c.relnamespace = n.oid
         where c.relname = 'pg_extension'
           and n.nspname = 'pg_catalog'
    )
        select pgt.oid as classid,
               t.oid as objid,
               0 as objsubid,
               pge.oid as refclassid,
               t.cyanaudit_oid as refobjid,
               0 as refobjsubid,
               'e' as deptype
          from tt_trigger_oids t
    cross join tt_pg_extension_oid pge
    cross join tt_pg_trigger_oid pgt
);

insert into pg_depend
(
    with tt_trigger_oids as
    (
        select tr.oid,
               p.oid as proc_oid
          from pg_trigger tr
          join pg_class c
            on tr.tgrelid = c.oid
          join pg_proc p
            on tr.tgfoid = p.oid
          join pg_extension e
            on e.extname = 'cyanaudit'
          join pg_namespace en
            on e.extnamespace = en.oid
         where c.relnamespace != en.oid
           and p.pronamespace = en.oid
           and not tr.tgisinternal
    ),
    tt_pg_trigger_oid as
    (
        select c.oid
          from pg_class c
          join pg_namespace n
            on c.relnamespace = n.oid
         where c.relname = 'pg_trigger'
           and n.nspname = 'pg_catalog'
    ),
    tt_pg_proc_oid as
    (
        select c.oid
          from pg_class c
          join pg_namespace n
            on c.relnamespace = n.oid
         where c.relname = 'pg_proc'
           and n.nspname = 'pg_catalog'
    )
        select pgt.oid as classid,
               t.oid as objid,
               0 as objsubid,
               pge.oid as refclassid,
               t.cyanaudit_oid as refobjid,
               0 as refobjsubid,
               'e' as deptype
          from tt_trigger_oids t
    cross join tt_pg_extension_oid pge
    cross join tt_pg_trigger_oid pgt
);


   oid   |   relname
---------+--------------
    3079 | pg_extension
    1255 | pg_proc
    2620 | pg_trigger
 3826146 | tb_health
    1259 | pg_class
    

   oid   |           proname
---------+------------------------------
 3996922 | fn_log_audit_event_tb_health

   oid   |            tgname
---------+------------------------------
 3996923 | tr_log_audit_event_tb_health

   oid   |  extname
---------+-----------
 3996148 | cyanaudit


