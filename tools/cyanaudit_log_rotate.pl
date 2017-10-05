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
        sleep 1;
        print "xmin = $xmin, xmax = $xmax.\n";
        ($xmin) = $handle->selectrow_array( "select txid_snapshot_xmin( txid_current_snapshot() )" );
    }
    sleep 1;
    print "Done.\n";
}

my %opts;

getopts('U:h:p:d:Pn:s:a:', \%opts) or usage();

unless( $opts{'n'} and $opts{'n'} =~ /^\d+$/ )
{
    usage( "-n is required and must be an integer." );
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
    my ($table_name) = $handle->selectrow_array( "select cyanaudit.fn_create_new_partition('blah')" );
    print "Created new partition cyanaudit.$table_name\n";

    print "Setting up and activating new partition... ";
    $handle->do( "select cyanaudit.fn_verify_partition_config( ? )", undef, $table_name );
    $handle->do( "select cyanaudit.fn_activate_partition( ? )", undef, $table_name );
    print "Done.\n";

    $handle->do("commit");

    &wait_for_open_transactions_to_finish();

    print "Temporarily removing inheritance on old partition.\n";
    $handle->do( "select cyanaudit.fn_setup_partition_inheritance( ?, false )", undef, $old_table_name );

    print "Setting constraints, archiving, and reinstating inheritance on old partition...";
    $handle->do( "select cyanaudit.fn_verify_parititon_config( ? )", undef, $old_table_name );
    print "Done.\n";
}

###############
### PRUNING ###
###############
if( $opts{'n'} or $opts{'s'} or $opts{'a'} )
{
    my $archive_q = "select cyanaudit.fn_prune_archive( ?, ?, ? )";
    my $tables = $handle->selectcol_arrayref( $archive_q, undef, $opts{'n'}, $opts{'a'}, $opts{'s'} );

    if( @$tables )
    {
        print "Dropped the following old log partitions (> qty $opts{'n'})\n";
        print "$_\n" foreach( @$tables );
    }
}
