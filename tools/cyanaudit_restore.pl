#!/usr/bin/perl

$|=1;

use strict;
use warnings;

use DBI;
use Getopt::Std;
use Data::Dumper;
use Time::HiRes qw( gettimeofday );
use File::Basename;
use Parse::CSV;
use Date::Parse;

use Cyanaudit;

use constant DEBUG => 1;

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

sub is_text_empty($)
{
    my $val = @_;
    return 1 unless( defined $val );
    return 1 if( length( $val ) == 0 );
    return 0;
}

sub microtime()
{
    return sprintf '%d.%0.6d', gettimeofday();
}

sub print_restore_rate($$$$)
{
    return unless( DEBUG );

    my( $table_name, $start_time, $rows_restored, $timestamp ) = @_;
    my $delta = microtime() - $start_time;
    my $rate = $rows_restored / $delta;
    printf "\r$table_name: Restored row %d (%-26s) @ %d rows/sec ", 
        $rows_restored - 1, $timestamp, $rate;
}


my %opts;

getopts( 'U:h:p:d:', \%opts ) or usage();

my $params = {
    port => $opts{'p'},
    user => $opts{'U'},
    dbname => $opts{'d'},
    host => $opts{'h'}
};

unless( @ARGV )
{
    usage( "Must specify at least one file to restore" );
}

foreach my $file (@ARGV)
{
    unless( -f $file and -r $file and -s $file )
    {
        usage( "File '$file' is either invalid, unreadable or 0 bytes." );
    }

    unless( $file =~ '^tb_audit_event_\d{8}_\d{4}\.csv\.gz$' )
    {
        usage( "$file: Filename must conform to pattern 'tb_audit_event_YYYYMMDD_HHMM.csv.gz\n" );
    }
}

my $handle = db_connect( $params )
    or die "Could not connect to database: $DBI::errstr\n";

my $schema = get_cyanaudit_schema( $handle )
    or die sprintf( "Could not find cyanaudit in database '%s'\n", $params->{'dbname'} ) ;

print "Found Cyan Audit in schema '$schema'\n";

my $tablespace_q    = "SELECT current_setting('cyanaudit.archive_tablespace')";
my ($tablespace)    = $handle->selectrow_array($tablespace_q)
    or die( "Could not determine archive tablespace\n" );

print "Using tablespace $tablespace\n";

my %audit_fields;
my %audit_transaction_types;

my $lookup_handle = db_connect( $params )
    or die "Could not connect to database: $DBI::errstr\n";

my $audit_field_q = "select fn_get_or_create_audit_field(?,?,?)";
my $audit_field_sth = $lookup_handle->prepare($audit_field_q);

my $audit_transaction_type_q = "select fn_get_or_create_audit_transaction_type(?)";
my $audit_transaction_type_sth = $lookup_handle->prepare($audit_transaction_type_q);
    
my $csv_xs = new Text::CSV_XS;

foreach my $file( @ARGV )
{
    print "$file: Processing...\n";

    open( my $fh, "gunzip -c $file |" ) or die "Could not open $file: $!\n";

    my $csv = Parse::CSV->new(
        handle => $fh,
        names => 1,
        csv_attr => {
            blank_is_undef => 1,
            binary => 1
        },
        filter => sub { 
            if( $_->{'pk_val'} )
            {
                $_->{'pk_vals'} = '{' . $->{'pk_val'} . '}';
            }
            return $_; 
        }
    );

    $handle->do( "BEGIN" );

    # Create table partition
    (my $table_name = $file) =~ s/\.csv\.gz$//;

    $handle->do( "CREATE TABLE $schema.$table_name () "
               . "INHERITS ($schema.tb_audit_event) TABLESPACE ${tablespace}" )
        or die( "Could not create destination table for CSV contents." );

    my $copy_q = <<SQL;
        COPY $schema.$table_name
        (
            audit_field,
            pk_vals,
            recorded,
            uid,
            row_op,
            txid,
            audit_transaction_type,
            old_value,
            new_value
        ) FROM STDIN WITH ( FORMAT csv, HEADER false )
SQL

    $handle->do( $copy_q );

    my $start_time = microtime();
    my $last_timestamp;

    while( my $row = $csv->fetch )
    {
        my $audit_field = $audit_fields{$row->{'table_schema'}}{$row->{'table_name'}}{$row->{'column_name'}};

        unless( $audit_field )
        {
            $audit_field_sth->execute( $row->{'table_schema'}, $row->{'table_name'}, $row->{'column_name'} );
            ($audit_field) = $audit_field_sth->fetchrow_array();
            unless( $audit_field )
            {
                die sprintf( "$file: audit_field %s.%s.%s could not be created/found\n",
                             $row->{'table_schema'}, $row->{'table_name'}, $row->{'column_name'} );
            }
            $audit_fields{$row->{'table_schema'}}{$row->{'table_name'}}{$row->{'column_name'}} = $audit_field;
        }

        my $audit_transaction_type = undef;
        
        if( $row->{'description'} )
        {
            $audit_transaction_type = $audit_transaction_types{$row->{'description'}};

            unless( $audit_transaction_type )
            {
                $audit_transaction_type_sth->execute( $row->{'description'} );
                ($audit_transaction_type) = $audit_transaction_type_sth->fetchrow_array();
                unless( $audit_transaction_type )
                {
                    die sprintf( "$file: audit_transaction_type with label '%s' could not be created/found\n",
                                 $row->{'description'} );
                }
                $audit_transaction_types{$row->{'description'}} = $audit_transaction_type;
            }
        }

        $csv_xs->combine(
            $audit_field,
            $row->{'pk_vals'},
            $row->{'recorded'},
            $row->{'uid'},
            $row->{'row_op'},
            $row->{'txid'},
            $audit_transaction_type,
            $row->{'old_value'},
            $row->{'new_value'}
        );

        if( $handle->pg_putcopydata($csv_xs->string . "\n") != 1 )
        {
            die "pg_putcopydata( " . $csv_xs->string . " ) failed: " . DBI::errstr . "\n";
        }

        if( $csv->row % 1000 == 1 )
        {
            print_restore_rate( $table_name, $start_time, $csv->row, $row->{'recorded'} );
        }

        $last_timestamp = $row->{'recorded'}
    }

    print_restore_rate( $table_name, $start_time, $csv->row, $last_timestamp );
    print "\n";

    $handle->pg_putcopyend();

    close( $fh ) or die( "Could not close '$file':\n$!\n" );

    #Rename table and add check constraint to partition
    print "Getting constraint bounds...\n" if( DEBUG );
    my $bounds_q = <<SQL;
        SELECT count(*),
               max( recorded ) AS max_recorded,
               min( recorded ) AS min_recorded,
               max( txid     ) AS max_txid,
               min( txid     ) AS min_txid
          FROM ${schema}.$table_name
SQL
    
    my $bounds_row = $handle->selectrow_hashref( $bounds_q )
        or die( "Could not determine the recorded date range for table partition\n" );
    
    my $count        = $bounds_row->{'count'       };
    my $max_rec      = $bounds_row->{'max_recorded'};
    my $min_rec      = $bounds_row->{'min_recorded'};
    my $max_txid     = $bounds_row->{'max_txid'    };
    my $min_txid     = $bounds_row->{'min_txid'    };

    if( $count < $csv->row - 1 )
    {
        die "$file: " . ($csv->row - 1) . " rows read, but only $count rows in table\n";
    }
    
    my $constraint_q = '';
    print "Adding constraints...\n" if( DEBUG );
    
    $constraint_q = <<__EOF__;
    ALTER TABLE ${schema}.${table_name}
        ADD CONSTRAINT ${table_name}_recorded_check 
        CHECK( recorded >= '${min_rec}'::TIMESTAMP AND recorded <= '${max_rec}'::TIMESTAMP )
__EOF__
    
    $handle->do( $constraint_q ) or die( "Could not add recorded constraint to table partition\n" );

    $constraint_q = <<__EOF__;
    ALTER TABLE ${schema}.${table_name}
        ADD CONSTRAINT ${table_name}_txid_check 
        CHECK( txid >= ${min_txid}::BIGINT AND txid <= ${max_txid}::BIGINT )
__EOF__
    
    $handle->do( $constraint_q ) or die( "Could not add txid constraint to table partition\n" );
    
    print "Generating indexes...\n" if( DEBUG );

    foreach my $field (qw( audit_field recorded txid ))
    {
        $handle->do( "CREATE INDEX ${table_name}_${field}_idx "
                   . "          ON ${schema}.${table_name}($field) "
                   . "  TABLESPACE ${tablespace}" )
            or die "Could not create index on $schema.$table_name($field) ";
    }

    print "Fixing permissions...\n" if( DEBUG );
    $handle->do( "GRANT INSERT ON ${schema}.${table_name} TO public" )
        or die "Failed to set INSERT perms\n";
    $handle->do( "GRANT SELECT (audit_transaction_type,txid) ON ${schema}.${table_name} TO public" )
        or die "Failed to set SELECT perms\n";
    $handle->do( "GRANT UPDATE (audit_transaction_type) ON ${schema}.${table_name} TO public" )
        or die "Failed to set UPDATE perms\n";

    $handle->do( "ALTER EXTENSION cyanaudit ADD TABLE ${schema}.${table_name}" )
        or die( "Could not add table ${table_name} to cyanaudit extension" );

    $handle->do( "COMMIT" ) or die( "Could not commit restore operation\n" );
    my $end_time = microtime();
    my $delta    = ( $end_time - $start_time ) / 60;
    printf "Processed '$file' in %d minutes\n", $delta;
}

print "Successfully processed " . scalar @ARGV . " files.\n";
exit 0;
