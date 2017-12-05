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

sub wait_for_open_transactions_to_finish
{
    my ($handle) = @_;
    # xmax is the first as-yet-unassigned txid. All txids greater than or equal
    # to this are not yet started as of the time of the snapshot, and thus invisible.
    my ($xmax) = $handle->selectrow_array( "select txid_snapshot_xmax( txid_current_snapshot() )" );

    # Initialize xmin
    my $xmin = $xmax - 1;

    # Loop until xmin, the earliest txid that is still active, is not less than
    # the previous xmax value, meaning all transactions have completed. This is
    # to prevent attempts to label a transaction while that transaction's table
    # partition is being exclusively locked for the next steps.
    print "Waiting for transactions to finish and be labeled...";
    until( $xmin >= $xmax )
    {
        ($xmin) = $handle->selectrow_array( "select txid_snapshot_xmin( txid_current_snapshot() )" );
        sleep 1;
    }
    print "Done.\n";
}

my %opts;

getopts('U:h:p:d:Pn:a:s:', \%opts) or usage();

unless( ( $opts{'n'} and $opts{'n'} =~ /^\d+$/ ) 
     or ( $opts{'a'} and $opts{'a'} =~ /^\d+$/ )
     or ( $opts{'s'} and $opts{'s'} =~ /^\d+$/ ) )
{
    usage( "one of -n, -a, or -s is required and must be an integer." );
}

my $handle = db_connect( \%opts ) or die "Database connect error.\n";

$handle->do( "SET application_name = 'cyanaudit_log_rotate.pl'" );

################
### ROTATING ###
################
unless( $opts{'P'} )
{
    $handle->do("begin");

    my ($old_table_name) = $handle->selectrow_array( "select cyanaudit.fn_get_active_partition_name()" );
    my ($table_name) = $handle->selectrow_array( "select cyanaudit.fn_create_new_partition()" );

    if( !defined( $table_name ) )
    {
        print "No events to rotate. Skipping creation of new logging partition.\n";
    }
    else
    {
        print "Created new partition cyanaudit.$table_name\n";

        print "Setting up and activating new partition... ";
        $handle->do( "select cyanaudit.fn_verify_partition_config( ? )", undef, $table_name );
        $handle->do( "select cyanaudit.fn_activate_partition( ? )", undef, $table_name );
        print "Done.\n";

        $handle->do("commit");

        &wait_for_open_transactions_to_finish( $handle );

        print "Temporarily removing inheritance on old partition.\n";
        $handle->do( "select cyanaudit.fn_setup_partition_inheritance( ?, true )", undef, $old_table_name );

        print "Setting constraints, archiving, and reinstating inheritance on old partition...";
        $handle->do( "select cyanaudit.fn_verify_partition_config( ? )", undef, $old_table_name );
        print "Done.\n";
    }
}

###############
### PRUNING ###
###############
if( $opts{'n'} or $opts{'s'} or $opts{'a'} )
{
    my $tables_q = "select cyanaudit.fn_get_partitions_over_quantity_limit( ? ) "
                 . " UNION "
                 . "select cyanaudit.fn_get_partitions_over_size_limit( ? ) "
                 . " UNION "
                 . "select cyanaudit.fn_get_partitions_over_age_limit( ? * interval '1d' ) "
                 . "ORDER BY 1 ";

    my $tables = $handle->selectcol_arrayref( $tables_q, undef, $opts{'n'}, $opts{'s'}, $opts{'a'} );

    for my $table ( @$tables )
    {
        print "Dropping $table... ";
        $handle->do( "SELECT cyanaudit.fn_setup_partition_inheritance( ?, true )", undef, $table );
        $handle->do( "DROP TABLE cyanaudit.$table" );
        print "Done.\n";
    }
}
