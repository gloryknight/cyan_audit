#!/usr/bin/perl -w
# TODO: Dropping old tables

use strict;

$| = 1;

use DBI;
use Getopt::Std;
use File::Basename;

use lib dirname(__FILE__);

use Cyanaudit;

sub usage
{
    my ($message) = @_;

    print "Error: $message\n" if( $message );

    print "Usage: $0 [ options ... ]\n"
        . "Options:\n"
        . "  -h host    database server host or socket directory\n"
        . "  -p port    database server port\n"
        . "  -U user    database user name\n"
        . "  -d db      database name\n"
        . "  -n #       max number (count) of partitions after pruning\n"
        . "  -s #       max size (gb) of logs after pruning\n"
        . "  -a #       max age (days) of logs after pruning\n";

    exit 1;
}

my %opts;

getopts('U:h:p:d:n:', \%opts) or usage();

unless( $opts{'n'} and $opts{'n'} =~ /^\d+$/ )
{
    usage( "-n is required and must be an integer." );
}

my $handle = db_connect( \%opts ) or die "Database connect error.\n";

### Find cyanaudit schema

my $schema = 'cyanaudit';

print "Found cyanaudit in schema '$schema'\n";


my ($old_table_name) = $handle->selectrow_array( "select $schema.fn_get_active_partition_name()" );
my ($table_name) = $handle->selectrow_array( "select $schema.fn_create_new_partition()" ) or die; 

unless( $table_name )
{
    die "No events to rotate. Exiting.\n";
}

print "Created new archive table $schema.$table_name.\n";

print "Finalizing indexes and constraints... ";
$handle->do( "select $schema.fn_setup_partition_constraints( ? )", undef, $table_name );
$handle->do( "select $schema.fn_create_partition_indexes( ? )", undef, $table_name );
$handle->do( "select $schema.fn_setup_partition_inheritance( ? )", undef, $table_name );
$handle->do( "select $schema.fn_activate_partition( ? )", undef, $table_name );

if( $old_table_name )
{
    $handle->do( "select $schema.fn_setup_partition_constraints( ? )", undef, $old_table_name );
    $handle->do( "select $schema.fn_archive_partition( ? )", undef, $old_table_name ) or die;
}

print "Done.\n";

if( $opts{'n'} or $opts{'s'} or $opts{'a'} )
{
    my $archive_q = "select $schema.fn_prune_archive( ?, ?, ? )";
    my $tables = $handle->selectcol_arrayref( $archive_q, undef, $opts{'n'}, $opts{'a'}, $opts{'s'} ) 
        or die "Could not drop old audit log partition(s).\n";

    if( @$tables )
    {
        print "Dropped the following old log partitions (> qty $opts{'n'})\n";
        print "$_\n" foreach( @$tables );
    }
}
