#!/usr/bin/perl

$|=1;

use strict;
use warnings;

use DBI;
use Getopt::Std;
use Text::CSV_XS;
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
    SELECT c.relname
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

my $get_audit_data_type_q = <<__EOF__;
    SELECT $schema.fn_get_or_create_audit_data_type(
               ?
           ) AS audit_data_type
__EOF__
my $get_audit_data_type_sth = $handle->prepare( $get_audit_data_type_q );

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

    $handle->do( "BEGIN" );
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

    my $csv = Text::CSV_XS->new( { binary => 1, eol => "\n" } );
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
    
    my $update_audit_field_q = "UPDATE $schema.tb_audit_field SET audit_data_type = ? WHERE audit_field = ?";
    my $update_audit_field_sth = $handle->prepare( $update_audit_field_q );
    my $header_hash     = { };

    while( my $line = $csv->getline( $fh ) )
    {
        $line_count++;

        if( $line_count == 1 )
        {
            my $tablespace_q    = "SELECT current_setting('${schema}.archive_tablespace') AS tablespace";
            my $tablespace_sth  = $handle->prepare( $tablespace_q );
               $tablespace_sth->execute() or die( "Could not determin archive tablespace\n" );
            my $tablespace_row  = $tablespace_sth->fetchrow_hashref();
            my $tablespace      = $tablespace_row->{'tablespace'};
            
            # Header determination logic
            my $count = 0;
            
            foreach my $header( @$line )
            {
                $header_hash->{$header} = $count;
                $count++;
            }
            
            # Create table partition
            print "Creating restoration table...\n";
            $handle->do( "CREATE TABLE $schema.tb_audit_event_restore ( ) INHERITS (tb_audit_event) TABLESPACE ${tablespace}" )
                or die( "Could not create partition table to restore CSV contents" );
        }
        else
        {
            my $audit_event = $line->[$header_hash->{'audit_event'  }];
            my $txid        = $line->[$header_hash->{'txid'         }];
            my $recorded    = $line->[$header_hash->{'recorded'     }];
            my $uid         = $line->[$header_hash->{'uid'          }];
            my $email       = $line->[$header_hash->{'email_address'}];
            my $table_name  = $line->[$header_hash->{'table_name'   }];
            my $column      = $line->[$header_hash->{'column_name'  }];
            my $row_pk_val  = $line->[$header_hash->{'row_pk_val'   }];
            my $row_op      = $line->[$header_hash->{'row_op'       }];
            my $pid         = $line->[$header_hash->{'pid'          }];
            my $description = $line->[$header_hash->{'description'  }];
            my $old_value   = $line->[$header_hash->{'old_value'    }];
            my $new_value   = $line->[$header_hash->{'new_value'    }];
            my $data_type   = $line->[$header_hash->{'data_type'    }] if( defined $header_hash->{'data_type'} );

            my $audit_data_type;

            if( $data_type )
            {
                $get_audit_data_type_sth->bind_param( 1, $data_type );
                $get_audit_data_type_sth->execute() or die( "Could not determine audit_data_type for $data_type\n" );
                my $audit_data_type_row = $get_audit_data_type_sth->fetchrow_hashref();
                $audit_data_type = $audit_data_type_row->{'audit_data_type'};
            }    
            
            $get_audit_field_sth->bind_param( 1, $table_name );
            $get_audit_field_sth->bind_param( 2, $column     );
            $get_audit_field_sth->execute() or die( "Could not get/create audit field for table $table_name, column $column\n" );
            my $audit_field_row = $get_audit_field_sth->fetchrow_hashref();

            $get_audit_transaction_type_sth->bind_param( 1, $description );
            $get_audit_transaction_type_sth->execute() or die( "Could not get audit transaction type for label $description\n" );
            my $audit_transaction_type_row = $get_audit_transaction_type_sth->fetchrow_hashref();

            my $audit_field             = $audit_field_row->{'audit_field'};
            my $audit_transaction_type  = $audit_transaction_type_row->{'audit_transaction_type'};
            
            if( $audit_data_type )
            {
                # Update audit_field row to the audit_data_type
                $update_audit_field_sth->bind_param( 1, $audit_data_type );
                $update_audit_field_sth->bind_param( 2, $audit_field     );
                $update_audit_field_sth->execute()
                    or die( "Could not update audit_field $audit_field to have audit_data_type $audit_data_type\n" );
            }

            $audit_event_sth->bind_param( 1,  $audit_event              );
            $audit_event_sth->bind_param( 2,  $audit_field              );
            $audit_event_sth->bind_param( 3,  $row_pk_val               );
            $audit_event_sth->bind_param( 4,  $recorded                 );
            $audit_event_sth->bind_param( 5,  $uid                      );
            $audit_event_sth->bind_param( 6,  $row_op                   );
            $audit_event_sth->bind_param( 7,  $txid                     );
            $audit_event_sth->bind_param( 8,  $pid                      );
            $audit_event_sth->bind_param( 9,  $audit_transaction_type   );
            $audit_event_sth->bind_param( 10, $old_value                );
            $audit_event_sth->bind_param( 11, $new_value                );
            $audit_event_sth->execute() or die( "Could not insert row\n" );
        }
    }

    #$handle->pg_putcopyend() or die( "Error finalizing restore\n" );
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
      FROM $schema.tb_audit_event_restore
)
    SELECT to_char( EXTRACT( year   FROM tt.max_recorded ), 'FM0000' )
        || to_char( EXTRACT( month  FROM tt.max_recorded ), 'FM00'   )
        || to_char( EXTRACT( day    FROM tt.max_recorded ), 'FM00'   )
        || '_'
        || to_char( EXTRACT( hour   FROM tt.max_recorded ), 'FM00'   )
        || to_char( EXTRACT( minute FROM tt.max_recorded ), 'FM00'   ) AS suffix,
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
    my $table_name   = "tb_audit_event_$table_suffix";
    
    print "Renaming table...\n" if( DEBUG ); 
    $handle->do( "ALTER TABLE ${schema}.tb_audit_event_restore RENAME TO $table_name" )
         or die( "Could not rename tb_audit_event_restore to $table_name\n" );

    my $constraint_q = '';
    print "Adding constraints...\n" if( DEBUG );
    
    $constraint_q = <<__EOF__;
    ALTER TABLE ${schema}.${table_name}
        ADD CONSTRAINT ${table_name}_recorded_check CHECK( recorded >= '${min_rec}'::TIMESTAMP AND recorded <= '${max_rec}'::TIMESTAMP )
__EOF__
    
    $handle->do( $constraint_q ) or die( "Could not add recorded constraint to table partition\n" );

    $constraint_q = <<__EOF__;
    ALTER TABLE ${schema}.${table_name}
        ADD CONSTRAINT ${table_name}_txid_check CHECK( txid >= ${min_txid}::BIGINT AND txid <= ${max_txid}::BIGINT )
__EOF__
    
    $handle->do( $constraint_q ) or die( "Could not add txid constraint to table partition\n" );
    
    print "Generating indexes...\n" if( DEBUG );
    $handle->do( "CREATE INDEX ${table_name}_audit_field_idx ON ${schema}.${table_name}(audit_field)" )
        or die( "Failed to create audit_field index\n" );
    $handle->do( "CREATE INDEX ${table_name}_recorded_idx    ON ${schema}.${table_name}(recorded)"    )
        or die( "Failed to create recorded index\n"    );
    $handle->do( "CREATE INDEX ${table_name}_txid_idx        ON ${schema}.${table_name}(txid)"        )
        or die( "Failed to create txid index\n"        );

    print "Fixing permissions...\n" if( DEBUG );
    $handle->do( "GRANT INSERT                               ON ${schema}.${table_name} TO public" )
        or die( "Failed to set INSERT perms\n" );
    $handle->do( "GRANT SELECT (audit_transaction_type,txid) ON ${schema}.${table_name} TO public" )
        or die( "Failed to set SELECT perms\n" );
    $handle->do( "GRANT UPDATE (audit_transaction_type)      ON ${schema}.${table_name} TO public" )
        or die( "Failed to set UPDATE perms\n" );

    $handle->do( "COMMIT" ) or die( "Could not commit restore operation\n" );
}

print "Successfully processed " . scalar @files . " files.\n";
exit 0;
