#!/usr/bin/perl -w

use strict;

$| = 1;

use DBI;
use Getopt::Std;
use Encode qw(encode);

use Cyanaudit;

sub usage
{
    my( $msg ) = @_;

    warn "Error: $msg\n" if( $msg );
    print "Usage: $0 [ options ] outpath\n"
        . "Options:\n"
        . "  -h host    database server host or socket directory\n"
        . "  -p port    database server port\n"
        . "  -U user    database user name\n"
        . "  -d db      database name\n";

    exit 1;
}

my %opts;

getopts( 'U:h:p:d:', \%opts ) or usage();

my $outdir = $ARGV[0];
usage( "Must specify output directory" ) unless ( $outdir );
usage( "Output directory '$outdir' is invalid" ) unless( -d $outdir );
usage( "Output directory '$outdir' is not writable" ) unless ( -w $outdir );
chdir( $outdir ) or die "Could not chdir($outdir): $!\n";

my $params = {
    port => $opts{'p'},
    user => $opts{'U'},
    dbname => $opts{'d'},
    host => $opts{'h'}
};

my $handle = db_connect($params) 
    or die "Could not connect to database: $DBI::errstr\n";;

my $schema = get_cyanaudit_schema($handle)
    or die "Could not determine cyanaudit schema.\n";

print "Found Cyan Audit in schema '$schema'.\n";

# Returns arrayref of hashrefs, each hash containing keys 'table_name' and 'table_size_pretty'
my $table_rows = get_cyanaudit_data_table_list($handle);

foreach my $table_row (@$table_rows)
{
    my $table_name = $table_row->{'table_name'};
    my $table_size_pretty = $table_row->{'table_size_pretty'};
    
    my $outfile = "$table_name.csv.gz";

    if( md5_verify($outfile) )
    {
        print "Skipping backup for table $table_name: Valid backup already present.\n";
        next;
    }

    my $exporting_msg = "Exporting $schema.$table_name ($table_size_pretty)";
    print "$exporting_msg: ";
    print "Preparing... " if( -t STDIN );

    my $total_rows_q = <<SQL;
        select reltuples::bigint
          from pg_class
         where oid = '$schema.$table_name'::regclass
SQL

    my ($total_rows) = $handle->selectrow_array($total_rows_q);

    $total_rows = 1 if $total_rows == 0;

    my $data_q = <<SQL;
           select ae.recorded,
                  ae.txid,
                  ae.uid,
                  att.label as description,
                  af.table_schema,
                  af.table_name,
                  ae.pk_vals,
                  ae.row_op,
                  af.column_name,
                  ae.old_value,
                  ae.new_value
             from $schema.$table_name ae
             join $schema.tb_audit_field af
               on ae.audit_field = af.audit_field
        left join $schema.tb_audit_transaction_type att
               on ae.audit_transaction_type = att.audit_transaction_type
         order by recorded
SQL

    $handle->do("copy ($data_q) to stdout with csv header");

    open( my $fh, "| gzip -9 -c > $outfile" ) or die "Could not open output for writing: $!\n";

    my $row;
    my $row_count = 0;

    while( $handle->pg_getcopydata(\$row) >= 0 )
    {
        my $row_encoded = encode( 'UTF-8', $row, Encode::FB_CROAK );

        print $fh $row_encoded or die "Error writing to output: $!\n";
        $row_count++;

        if ( -t STDIN and $row_count > 1 ) 
        {
            my $current_percent = $row_count / $total_rows * 100;
            $current_percent = 99.9 if( $current_percent > 99.9 );

            if( $row_count % 1000 == 0 ) 
            {
                printf "\r$exporting_msg: $row_count rows written (%0.1f%% complete)... ",
                        $current_percent;
            }
        }
    }

    printf "\r$exporting_msg: $row_count rows written (100.0%% complete)... ",

    close( $fh );

    md5_write( $outfile );

    print "Done!\n";
}
