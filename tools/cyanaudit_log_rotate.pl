#!/usr/bin/perl -w

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
        . "  -P         prune only, do not rotate\n"
        . "  -n #       max number (count) of partitions to keep after pruning\n"
        . "  -s #       max size (gb) of logs to keep after pruning\n"
        . "  -a #       max age (days) of logs to keep after pruning\n";

    exit 1;
}

my %opts;

getopts('U:h:p:d:n:', \%opts) or usage();

unless( $opts{'n'} and $opts{'n'} =~ /^\d+$/ )
{
    usage( "-n is required and must be an integer." );
}

my $handle = db_connect( \%opts ) or die "Database connect error.\n";

$handle->do( "SET application_name = 'Cyanaudit Log Rotation'" );

### cyanaudit is no longer relocatable
my $schema = 'cyanaudit';

################
### ROTATING ###
################
unless( $opts{'P'} )
{
    my ($old_table_name) = $handle->selectrow_array( "select $schema.fn_get_active_partition_name()" );
    my ($table_name) = $handle->selectrow_array( "select $schema.fn_create_new_partition()" ) or die; 

    # xmax is the first as-yet-unassigned txid. All txids greater than or equal
    # to this are not yet started as of the time of the snapshot, and thus invisible.
    my ($xmax) = $handle->selectrow_array( "select txid_snapshot_xmax( txid_current_snapshot() )" ) or die;

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

    # Initialize xmin
    my $xmin = $xmax - 1;

    # Loop until xmin, the earliest txid that is still active, is not less than
    # the previous xmax value, meaning all transactions have completed. This is
    # to prevent attempts to label a transaction while that transaction's table
    # partition is being exclusively locked for the next steps.
    until( $xmin >= $xmax )
    {
        ($xmin) = $handle->selectrow_array( "select txid_snapshot_xmin( txid_current_snapshot() )" ) or die;
    }

    # Give finished transactions a chance to be labeled using
    # fn_label_last_transaction().
    sleep(5);
    
    if( $old_table_name )
    {
        # Break inheritance so logging functions will not try to scan table / indexes
        $handle->do( "ALTER TABLE $schema.$old_table_name NO INHERIT $schema.tb_audit_event" );
        
        $handle->do( "select $schema.fn_setup_partition_constraints( ? )", undef, $old_table_name );
        $handle->do( "select $schema.fn_archive_partition( ? )", undef, $old_table_name ) or die;
        
        # Re set inheritance so that this partition is visible in vw_audit_log
        $handle->do( "ALTER TABLE $schema.$old_table_name INHERIT $schema.tb_audit_event" );
    }

    print "Done.\n";
}

###############
### PRUNING ###
###############
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
