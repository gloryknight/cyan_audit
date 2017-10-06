#!/usr/bin/perl

use strict;
use warnings;

$| = 1;

use DBI;
use Getopt::Std;
use File::Basename;

use lib dirname(__FILE__);

use Cyanaudit;

sub usage
{
    my( $msg ) = @_;

    warn "Error: $msg\n" if( $msg );
    print "Usage: $0 [ options ... ]\n"
        . "Options:\n"
        . "  -h host    database server host or socket directory\n"
        . "  -p port    database server port\n"
        . "  -U user    database user name\n"
        . "  -d db      database name\n";

    exit 1;
}

my %opts;

getopts( 'U:h:p:d:', \%opts ) or usage();

my $handle = db_connect( \%opts ) 
    or die "Could not connect to database: $DBI::errstr\n";;

my $tables_q = <<SQL;
    select c.relname as table_name
      from pg_class c
      join pg_namespace n
        on c.relnamespace = n.oid
     where c.relkind = 'r'
       and n.nspname = 'cyanaudit'
       and c.relname ~ '^tb_audit_event_\\d{8}_\\d{4}\$'
     order by 1 desc
SQL

# Returns arrayref of hashrefs, each hash containing keys 'table_name' and 'table_size_pretty'
my $tables = $handle->selectcol_arrayref( $tables_q );
my ($active_partition) = $handle->selectrow_array( 'select cyanaudit.fn_get_active_partition_name()' );

print "Validating configuration of Cyan Audit log partitions:\n";

foreach my $table (@$tables)
{
    print "$table... ";

    if( $table ne $active_partition )
    {
        $handle->do( "select cyanaudit.fn_setup_partition_inheritance( ?, true )", undef, $table );
    }

    $handle->do( "select cyanaudit.fn_verify_partition_config( ? )", undef, $table );

    print "Done.\n";
}
