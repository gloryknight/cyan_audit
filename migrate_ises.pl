#!/usr/bin/perl -w

use strict;

use DBI;
use Data::Dumper;
use FindBin;

my $handle = DBI->connect('dbi:Pg:dbname=ises', 'postgres', '',
                          { AutoCommit => 1, ShowErrorStatement => 1 } )
    or die "Could not connect to database\n";

chdir $FindBin::Bin;

system('psql -U postgres -d ises -1 -f migrate_ises.sql')
    and die "Error during migration\n";

sub update($)
{
    print scalar localtime() . ": " . $_[0];
}

my $table_list_q = "select relname "
                 . "  from pg_class c "
                 . "  join pg_namespace n "
                 . "    on c.relnamespace = n.oid "
                 . " where n.nspname = 'auditlog' "
                 . "   and c.relkind = 'r' "
                 . "   and c.relname like 'tb_audit_event_%' "
                 . " order by 1 desc ";

my $tables = $handle->selectcol_arrayref($table_list_q);

my $txarch_check_q = "select count(*) "
                   . "  from pg_class c "
                   . "  join pg_namespace n "
                   . "    on c.relnamespace = n.oid "
                   . " where c.relname = 'tb_audit_transaction_archive' "
                   . "   and n.nspname = 'audit_log' ";

my $txarch_row = $handle->selectrow_arrayref($txarch_check_q) or die;

my $txarch_exists = 0;

if( $txarch_row->[0] == 1 )
{
    $txarch_exists = 1;
}

foreach my $table (@$tables)
{
    &update( "$table: Adding foreign keys...\n" );

    my $fk1_q = "alter table auditlog.$table "
              . "   add foreign key (audit_field) "
              . "   references auditlog.tb_audit_field ";

    my $fk2_q = "alter table auditlog.$table "
              . "   add foreign key (audit_transaction_type) "
              . "   references auditlog.tb_audit_transaction_type ";

    $handle->do( $fk1_q ) or die;

    $handle->do( $fk2_q ) or die;

    &update( "$table: Updating transaction_type using tb_audit_transaction...\n");

    my $txtype_q = "update auditlog.$table ae "
                 . "   set audit_transaction_type = at.audit_transaction_type "
                 . "  from %s at "
                 . " where at.transaction_id = ae.txid ";

    $handle->do( sprintf( $txtype_q, 'public.tb_audit_transaction' ) ) or die;

    if( $txarch_exists )
    {
        &update( "$table: Updating transaction_type using tb_audit_transaction_archive...\n" );

        $handle->do(sprintf($txtype_q,'auditlog.tb_audit_transaction_archive'))
            or die;
    }

}

&update( "Dropping tables and sequences...\n" );

$handle->do('drop table public.tb_audit_transaction') or die;
$handle->do('drop table public.tb_audit_transaction_type') or die;
$handle->do('drop sequence public.sq_pk_audit_transaction') or die;
$handle->do('drop sequence public.sq_op_sequence') or die;

&update( "Dropping schema...\n" );

$handle->do('drop schema audit_log') or die;

&update( "Done\n" );
