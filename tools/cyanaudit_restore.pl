#!/usr/bin/perl

$|=1;

use strict;
use warnings;

use DBI;
use Getopt::Std;
use Text::CSV_XS;
use Data::Dumper;
use Time::HiRes qw(gettimeofday);

use constant DEBUG => 1;

sub usage($)
{
    my( $msg ) = @_;

    print "Error: $msg\n" if( $msg );
    print "Usage: $0 [ options ] file [...]\n"
        . "Options:\n"
        . "  -d db      Connect to given database\n"
        . "  -h host    Connect to given host\n"
        . "  -p port    Connect on given port\n"
        . "  -U user    Connect as given user\n";
    exit 1;
}

sub microtime()
{
    return sprintf '%d.%0.6d', gettimeofday();
}

our( $opt_U, $opt_d, $opt_h, $opt_p );
&usage( "Invalid arguments" ) unless( getopts( 'U:d:h:p:' ) );

my $port    = $opt_p;
my $user    = $opt_U;
my $host    = $opt_h;
my $dbname  = $opt_d;

unless( @ARGV )
{
    &usage( "Must specify at least one file to restore" );
}

foreach my $file (@ARGV)
{
    ( -f $file and -r $file and -s $file )
        or &usage( "File '$file' is either invalid, unreadable or 0 bytes." );
}

my $connection_string = "dbi:Pg:"
                      . ($opt_d ? "dbname=$opt_d;" : "")
                      . ($opt_h ? "host=$opt_h;"   : "")
                      . ($opt_p ? "port=$opt_p;"   : "");

$connection_string =~ s/;$//;

my $handle = DBI->connect($connection_string, $user, '' )
    or die "Couldn't connect to database: $DBI::errstr\n";

my $schema_q = <<__EOF__;
    SELECT n.nspname AS schema_name
      FROM pg_extension e
INNER JOIN pg_namespace n
        ON n.oid = e.extnamespace
     WHERE e.extname = 'cyanaudit'
__EOF__

my $schema_sth = $handle->prepare( $schema_q );
   $schema_sth->execute() or die( "Could not determine audit log schema\n" );

die( "Couldn't locate auditlog schema" ) if( $schema_sth->rows() == 0 );

my $schema_row = $schema_sth->fetchrow_hashref();
my $schema     = $schema_row->{'schema_name'};

print "Found audit log extension installed to schema '$schema'\n";

my $table_q = <<__EOF__;
    SELECT c.relname
      FROM pg_class c
INNER JOIN pg_namespace n
        ON c.relnamespace = n.oid
     WHERE c.relkind = 'r'
       AND n.nspname = '${schema}'
       AND c.relname = 'tb_audit_event_current';
__EOF__

$handle->selectrow_array( $table_q )
   or die "Could not find tb_audit_event_current table\n";

my $audit_data_type_q = "SELECT $schema.fn_get_or_create_audit_data_type( ? )";
my $audit_data_type_sth = $handle->prepare( $audit_data_type_q );

my $audit_field_q   = "SELECT $schema.fn_get_or_create_audit_field(?, ?)";
my $audit_field_sth = $handle->prepare( $audit_field_q );

my $audit_transaction_type_q = "SELECT $schema.fn_get_or_create_audit_transaction_type(?)";
my $audit_transaction_type_sth = $handle->prepare( $audit_transaction_type_q );

my $audit_event_insert_q = <<__EOF__;
    INSERT INTO $schema.tb_audit_event_restore
                (
                    audit_event,
                    audit_field,
                    row_pk_val,
                    recorded,
                    uid,
                    row_op,
                    txid,
                    pid,
                    audit_transaction_type,
                    old_value,
                    new_value
                )
         VALUES
                (
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?
                )
__EOF__

my $audit_event_sth = $handle->prepare( $audit_event_insert_q );

my $update_audit_field_q = <<__EOF__;
    UPDATE ${schema}.tb_audit_field 
       SET audit_data_type = ? 
     WHERE audit_field = ?
__EOF__

my $update_audit_field_sth = $handle->prepare( $update_audit_field_q );

my %audit_data_types;

foreach my $file( @ARGV )
{
    my $start_time = microtime();
    # Check if gzip, we'll need a CSV
    my $fh;

    if( $file =~ /\.gz$/i )
    {
        open( $fh, "gunzip -c $file |" ) or die "Could not open $file: $!\n";
    }
    elsif( $file =~ /\.csv$/i )
    {
        open( $fh, "< $file" ) or die "Could not open $file: $!\n";
    }
    else
    {
        die "File $file must be either CSV or GZIP\n";
    }

    print "Restoring from $file\n";

    my $csv = Text::CSV_XS->new( { binary => 1, eol => "\n" } );

    my $headers = $csv->getline($fh);

    # Header determination logic
    my $count = 0;
    my %header_hash = map { $_ => $count++ } @$headers;
    
    my $tablespace_q    = "SELECT current_setting('${schema}.archive_tablespace')";
    my ($tablespace)    = $handle->selectrow_array($tablespace_q)
        or die( "Could not determine archive tablespace\n" );

    print "Using tablespace $tablespace\n";
    
    $handle->do( "BEGIN" );

    # Create table partition
    $handle->do( "CREATE TABLE $schema.tb_audit_event_restore ( ) "
               . "INHERITS (tb_audit_event) TABLESPACE ${tablespace}" )
        or die( "Could not create partition table to restore CSV contents" );

    my $line_count = 1;

    while( my $line = $csv->getline( $fh ) )
    {
        $line_count++;

        my $audit_event = $line->[$header_hash{'audit_event'  }];
        my $txid        = $line->[$header_hash{'txid'         }];
        my $recorded    = $line->[$header_hash{'recorded'     }];
        my $uid         = $line->[$header_hash{'uid'          }];
        my $email       = $line->[$header_hash{'email_address'}];
        my $table_name  = $line->[$header_hash{'table_name'   }];
        my $column      = $line->[$header_hash{'column_name'  }];
        my $row_pk_val  = $line->[$header_hash{'row_pk_val'   }];
        my $row_op      = $line->[$header_hash{'row_op'       }];
        my $pid         = $line->[$header_hash{'pid'          }];
        my $description = $line->[$header_hash{'description'  }];
        my $old_value   = $line->[$header_hash{'old_value'    }];
        my $new_value   = $line->[$header_hash{'new_value'    }];
        my $data_type   = $line->[$header_hash{'data_type'    }] 
            if( defined $header_hash{'data_type'} );

        my $audit_data_type = undef;

        if( $data_type )
        {
            if( not $audit_data_types{$data_type} )
            {
                $audit_data_type_sth->bind_param( 1, $data_type );
                $audit_data_type_sth->execute() 
                    or die( "Could not translate data_type '$data_type'\n" );
                my $audit_data_type_row = $audit_data_type_sth->fetchrow_arrayref();
                $audit_data_types{$data_type} = $audit_data_type_row->[0];
            }

            $audit_data_type = $audit_data_types{$data_type} || 0;
        }    
        
        $audit_field_sth->bind_param( 1, $table_name      );
        $audit_field_sth->bind_param( 2, $column          );
        #$audit_field_sth->bind_param( 3, $audit_data_type );
        $audit_field_sth->execute() 
            or die( "Could not get/create audit field for table $table_name, column $column\n" );

        my $audit_field_row = $audit_field_sth->fetchrow_arrayref();

        $audit_transaction_type_sth->bind_param( 1, $description );
        $audit_transaction_type_sth->execute() 
            or die( "Could not get audit transaction type for label $description\n" );
        my $audit_transaction_type_row = $audit_transaction_type_sth->fetchrow_arrayref();

        my $audit_field             = $audit_field_row->[0];
        my $audit_transaction_type  = $audit_transaction_type_row->[0];

        $audit_event_sth->bind_param( 1,  $audit_event               );
        $audit_event_sth->bind_param( 2,  $audit_field               );
        $audit_event_sth->bind_param( 3,  $row_pk_val                );
        $audit_event_sth->bind_param( 4,  $recorded                  );
        $audit_event_sth->bind_param( 5,  $uid                       );
        $audit_event_sth->bind_param( 6,  $row_op                    );
        $audit_event_sth->bind_param( 7,  $txid                      );
        $audit_event_sth->bind_param( 8,  $pid                       );
        $audit_event_sth->bind_param( 9,  $audit_transaction_type    );
        $audit_event_sth->bind_param( 10, $old_value                 );
        $audit_event_sth->bind_param( 11, $new_value                 );
        $audit_event_sth->execute() or die( "Could not insert row\n" );

        if( $line_count % 1000 == 1 ) 
        {
            my $delta = microtime() - $start_time;
            my $rate = $line_count / $delta;
            printf "\r%d rows restored at %d rows/sec... ", 
                $line_count - 1, $rate;
        }

    }

    close( $fh ) or die( "Could not close '$file':\n$!\n" );
    print "\nDone.\n";

    #Rename table and add check constraint to partition
    print "Getting contraint bounds and table suffix...\n" if( DEBUG );
    my $max_recorded_q = <<__EOF__;
WITH tt_recorded AS
(
    SELECT max( recorded ) AS max_recorded,
           min( recorded ) AS min_recorded,
           max( txid     ) AS max_txid,
           min( txid     ) AS min_txid
      FROM ${schema}.tb_audit_event_restore
)
    SELECT 'tb_audit_event_' || to_char( tt.max_recorded, 'YYYYMMDD_HH24MI' ) 
                AS table_name,
           tt.max_recorded,
           tt.min_recorded,
           tt.max_txid,
           tt.min_txid
      FROM tt_recorded tt
__EOF__
    
    my $max_recorded_row = $handle->selectrow_hashref( $max_recorded_q )
        or die( "Could not determine the recorded date range for table partition\n" );
    
    my $table_name   = $max_recorded_row->{'table_name'  };
    my $max_rec      = $max_recorded_row->{'max_recorded'};
    my $min_rec      = $max_recorded_row->{'min_recorded'};
    my $max_txid     = $max_recorded_row->{'max_txid'    };
    my $min_txid     = $max_recorded_row->{'min_txid'    };
    
    print "Renaming table...\n" if( DEBUG );
    
    $handle->do( "DROP TABLE IF EXISTS ${schema}.${table_name}" );
    $handle->do( "ALTER TABLE ${schema}.tb_audit_event_restore RENAME TO ${table_name}" )
         or die( "Could not rename tb_audit_event_restore to $table_name\n" );

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
    $handle->do( "CREATE INDEX ${table_name}_audit_field_idx ON ${schema}.${table_name}(audit_field) TABLESPACE ${tablespace}" )
        or die( "Failed to create audit_field index\n" );
    $handle->do( "CREATE INDEX ${table_name}_recorded_idx    ON ${schema}.${table_name}(recorded)    TABLESPACE ${tablespace}" )
        or die( "Failed to create recorded index\n"    );
    $handle->do( "CREATE INDEX ${table_name}_txid_idx        ON ${schema}.${table_name}(txid)        TABLESPACE ${tablespace}" )
        or die( "Failed to create txid index\n"        );

    print "Fixing permissions...\n" if( DEBUG );
    $handle->do( "GRANT INSERT                               ON ${schema}.${table_name} TO public" )
        or die( "Failed to set INSERT perms\n" );
    $handle->do( "GRANT SELECT (audit_transaction_type,txid) ON ${schema}.${table_name} TO public" )
        or die( "Failed to set SELECT perms\n" );
    $handle->do( "GRANT UPDATE (audit_transaction_type)      ON ${schema}.${table_name} TO public" )
        or die( "Failed to set UPDATE perms\n" );

    $handle->do( "ALTER EXTENSION cyanaudit ADD TABLE ${schema}.${table_name}" )
        or die( "Could not add table ${table_name} to cyanaudit extension" );

    $handle->do( "COMMIT" ) or die( "Could not commit restore operation\n" );
    my $end_time = microtime();
    my $delta    = ( $end_time - $start_time ) / 3600;
    print "Processed '$file' in $delta hours\n";
}

print "Successfully processed " . scalar @ARGV . " files.\n";
exit 0;
