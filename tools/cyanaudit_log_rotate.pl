#!/usr/bin/perl -w

use strict;

$| = 1;

use DBI;
use Getopt::Std;

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
        . "  -d db      database name\n";

    exit 1;
}

my %opts = {};

getopts('U:h:p:d:', \%opts) or usage();

my $handle = db_connect( \%opts ) or die "Database connect error.\n";

### Find cyanaudit schema

my $schema = get_cyanaudit_schema($handle)
    or die "Could not determine audit log schema\n";

print "Found cyanaudit in schema '$schema'\n";


my ($table_name) = $handle->selectrow_array( "select $schema.fn_create_new_partition()" ); 
die "Audit log rotation not performed: no events present.\n" unless( $table_name );

print "Created new archive table $schema.$table_name.\n";

print "Finalizing indexes and constraints... ";
$handle->do( "select $schema.fn_setup_partition_range_constraint( ? )", undef, $table_name );
$handle->do( "select $schema.fn_create_partition_indexes( ? )", undef, $table_name );
$handle->do( "select $schema.fn_activate_partition( ? )", undef, $table_name );
$handle->do( "select $schema.fn_archive_partition( ? )", undef, $table_name );
print "Done\n";
