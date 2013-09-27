#!/usr/bin/perl

$|=1;

use strict;
use warnings;

use DBI;
use Getopt::Std;
use Data::Dumper;

use constant DEBUG => 1;

sub usage($)
{
    my( $msg ) = @_;

    print "$msg\n" if( $msg );
    print "Usage:\n";
    print " $0 -f file1,file2,file3,... -U dbuser -d dbname -h dbhost [-p dbport]\n";
    exit 1; 
}

our( $opt_f, $opt_U, $opt_d, $opt_h, $opt_p );
&usage( "Invalid arguments" ) unless( getopts( 'f:U:d:h:p:' ) );

my $port    = $opt_p;
my $user    = $opt_U;
my $host    = $opt_h;
my $dbname  = $opt_d;
my $files   = $opt_f;


$port = 5432 unless( $port and length( $port ) > 0 and $port =~ /^\d+$/ and $port < 65536 );

&usage( "Invalid username provided" ) unless( $user   and length( $user   ) > 0 );
&usage( "Invalid hostname provided" ) unless( $host   and length( $host   ) > 0 );
&usage( "Invalid dbname provided"   ) unless( $dbname and length( $dbname ) > 0 );
&usage( "Invalid files provided"    ) unless( $files  and length( $files  ) > 0 );
my @files   = split( ',', $files );

foreach my $file( @files )
{
    &usage( "Invalid filename provided" ) unless( length( $file ) > 0 );
    &usage( "Files $file does not exist or cannot be read!" ) unless( -f $file and -r $file );
}

my $connection_string = "dbname=${dbname};host=${host};port=${port}";

my $handle = DBI->connect("dbi:Pg:$connection_string", $user, '' )
                 or die( "Couldn't connect to database name, check connection params and .pgpass file" );

my $schema_q = <<__EOF__;
    SELECT n.nspname AS schema_name
      FROM pg_extension e
INNER JOIN pg_namespace n
        ON n.oid = e.extnamespace
     WHERE e.extname = 'auditlog'
__EOF__

my $schema_sth = $handle->prepare( $schema_q );
   $schema_sth->execute() or die( "Could not determine audit log schema\n" );

die( "Couldn't locate auditlog schema" ) if( $schema_sth->rows() == 0 );

my $schema_row = $schema_sth->fetchrow_hashref();
my $schema     = $schema_row->{'schema_name'};

print "Found audit log extension installed to schema '$schema'\n";

my $table_q = <<__EOF__;
    SELECT c.relname,
      FROM pg_class c
INNER JOIN pg_namespace n
        ON c.relnamespace = n.oid
     WHERE c.relkind = 'r'
       AND n.nspname = '$schema'
       AND c.relname = 'tb_audit_event_current';
__EOF__

my $table_sth   = $handle->prepare( $table_q );
   $table_sth->execute() or die( "Could not run check query for tb_audit_event_current\n" );

die( "tb_audit_event_current does not exist!" ) if( $table_sth->rows() < 1 );

my $get_audit_field_q   = <<__EOF__;
    SELECT $schema.fn_get_or_create_audit_field(
               ?,
               ?
           ) AS audit_field
__EOF__

my $get_audit_field_sth = $handle->prepare( $get_audit_field_q );

my $get_audit_transaction_type_q = <<__EOF__;
    SELECT $schema.fn_get_or_create_audit_transaction_type(
               ?
           ) AS audit_transaction_type
__EOF__

my $get_audit_transaction_type_sth = $handle->prepare( $get_audit_transaction_type_q );

foreach my $file( @files )
{
    # Check if gzip, we'll need a CSV
    my $fh;
    print "Opening backup file...\n" if( DEBUG );

    if( $file =~ /\.gz$/i )
    {
        open( $fh, "gunzip -c $file |" ) or die( "Could not open $file:\n$!\n" );
    }
    elsif( $file =~ /\.csv$/i )
    {
        open( $fh, "< $file" ) or die( "Could not open $file:\n$!\n" );
    }
    else
    {
        die( "File $file must be either CSV or GZIP\n" );
    }

    my $line_count = 0;
    print "Restoring table contents...\n" if( DEBUG );

    while( my $line = <$fh> )
    {
        $line_count++;

        if( $line_count == 1 )
        {
            # Create table partition
            $handle->do( "CREATE TABLE tb_audit_event_restore ( ) INHERITS (tb_audit_event)" )
                or die( "Could not create partition table to restore CSV contents" );

            $handle->do( "COPY tb_audit_event_restore FROM STDIN WITH DELIMITER ',' " )
                or die( "Could not initiate COPY command to tb_audit_event_restore" );
        }
        else
        {
            my $cols = \split( ',', $line );

            my $audit_event = $cols->[0 ];
            my $txid        = $cols->[1 ];
            my $recorded    = $cols->[2 ];
            my $uid         = $cols->[3 ];
            my $email       = $cols->[4 ];
            my $table_name  = $cols->[5 ];
            my $column      = $cols->[6 ];
            my $row_pk_val  = $cols->[7 ];
            my $row_op      = $cols->[8 ];
            my $pid         = $cols->[9 ];
            my $description = $cols->[10];
            my $old_value   = $cols->[11];
            my $new_value   = $cols->[12];
            
            $get_audit_field_sth->bind_param( 1, $table_name );
            $get_audit_field_sth->bind_param( 2, $column     );
            $get_audit_field_sth->execute() or die( "Could not get/create audit field for table $table_name, column $column\n" );
            my $audit_field_row = $get_audit_field_sth->fetchrow_hashref();

            $get_audit_transaction_type_sth->bind_param( 1, $description );
            $get_audit_transaction_type_sth->execute() or die( "Could not get audit transaction type for label $description\n" );
            my $audit_transaction_type_row = $get_audit_transaction_type_sth->fetchrow_hashref();

            my $audit_field             = $audit_field_row->{'audit_field'};
            my $audit_transaction_type  = $audit_transaction_type_row->{'audit_transaction_type'};

            my $row = "$audit_event,$audit_field,$row_pk_val,$recorded,$uid,$row_op,$txid,$pid,$audit_transaction_type,$old_value,$new_value";
            $handle->pg_putcopydata( $row ) or die( "Could not restore row $line_count of file $file into table partition\n" ); 
        }
    }

    $handle->pg_putcopyend() or die( "Error finalizing restore\n" );
    close( $fh ) or die( "Could not close '$file':\n$!\n" );
    print "Done, restored " . ( $line_count - 1 ) . " rows.\n" if( DEBUG );

    #Rename table and add check constraint to partition
    print "Getting contraint bounds and table suffix...\n" if( DEBUG );
    my $max_recorded_q = <<__EOF__;
WITH tt_recorded AS
(
    SELECT max( recorded ) AS max_recorded,
           min( recorded ) AS min_recorded,
           max( txid     ) AS max_txid,
           min( txid     ) AS min_txid
      FROM tb_audit_event_current
)
    SELECT EXTRACT( year   FROM tt.max_recorded )
        || EXTRACT( month  FROM tt.max_recorded )
        || EXTRACT( day    FROM tt.max_recorded )
        || '_'
        || EXTRACT( hour   FROM tt.max_recorded )
        || EXTRACT( minute FROM tt.max_recorded ) AS suffix,
        tt.max_recorded,
        tt.min_recorded,
        tt.max_txid,
        tt.min_txid
      FROM tt_recorded tt
__EOF__
    
    my $max_recorded_sth = $handle->prepare( $max_recorded_q );
       $max_recorded_sth->execute() or die( "Could not determine the recorded date range for table partition\n" );
    my $max_recorded_row = $max_recorded_sth->fetchrow_hashref();
    
    my $table_suffix = $max_recorded_row->{'suffix'      };
    my $max_rec      = $max_recorded_row->{'max_recorded'};
    my $min_rec      = $max_recorded_row->{'min_recorded'};
    my $max_txid     = $max_recorded_row->{'max_txid'    };
    my $min_txid     = $max_recorded_row->{'min_txid'    };
   
    print "Renaming table...\n" if( DEBUG ); 
    $handle->do( "ALTER TABLE tb_audit_event_restore RENAME TO tb_audit_event_$table_suffix" )
         or die( "Could not rename tb_audit_event_restore to tb_audit_event_$table_suffix\n" );

    my $constraint_q = '';
    print "Adding constraints...\n" if( DEBUG );
    $constraint_q = <<__EOF__;
    ALTER TABLE tb_audit_event_${table_suffix}
        ADD tb_audit_event_${table_suffix}_recorded_check
        CHECK ( recorded >= ${min_rec}::TIMESTAMP AND recorded <= ${max_rec}::TIMESTAMP )
__EOF__
    $handle->do( $constraint_q ) or die( "Could not add recorded constraint to table partition\n" );
    $constraint_q = <<__EOF__;
    ALTER TABLE tb_audit_event_${table_suffix}
        ADD tb_audit_event_${table_suffix}_txid_check
        CHECK ( txid >= ${min_txid}::BIGINT AND txid <= ${max_txid}::BIGINT )
__EOF__
    $handle->do( $constraint_q ) or die( "Could not add txid constraint to table partition\n" );


    print "Generating indexes...\n" if( DEBUG );
    $handle->do( "CREATE INDEX tb_audit_event_${table_suffix}_audit_field_idx ON tb_audit_event_${table_suffix}(audit_field)" );
    $handle->do( "CREATE INDEX tb_audit_event_${table_suffix}_recorded_idx ON tb_audit_event_${table_suffix}(recorded)"       );
    $handle->do( "CREATE INDEX tb_audit_event_${table_suffix}_txid_idx ON tb_audit_event_${table_suffix}(txid)"               );
}

print "Successfully processed " . scalar @files . " files.\n";
exit 0;
