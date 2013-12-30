#!/usr/bin/perl -w

use strict;

$| = 1;

use DBI;
use Getopt::Std;

my %opts;

sub usage
{
    my ($message) = @_;

    if( $message )
    {
        chomp($message);

        print "Error: $message\n";
    }

    print "Usage: $0 [ options ... ]\n"
        . "Options:\n"
        . "  -d db      Connect to given database\n"
        . "  -h host    Connect to given host\n"
        . "  -p port    Connect on given port\n"
        . "  -U user    Connect as given user\n";

    exit 1;
}


getopts('U:h:p:d:', \%opts) or usage();

my @connect_params = 
(
    $opts{'p'} ? "port=".$opts{'p'} : "",
    $opts{'h'} ? "host=".$opts{'h'} : "",
    $opts{'d'} ? "dbname=".$opts{'d'} : ""
);

my $connect_string = join( ';', @connect_params );

my $handle = DBI->connect("dbi:Pg:$connect_string", $opts{'U'}, '', 
                          { PrintWarn => 0 } ) 
    or die "Database connect error. "
         . "Please verify .pgpass and environment variables\n";

my $q;

### Find cyanaudit schema

$q = "select n.nspname "
   . "  from pg_extension e "
   . "  join pg_namespace n "
   . "    on e.extnamespace = n.oid "
   . " where e.extname = 'cyanaudit'";

my ($schema) = $handle->selectrow_array($q)
    or die "Could not determine audit log schema\n";

print "Found cyanaudit in schema '$schema'\n";


### Find cyanaudit tablespace

$q = "select current_setting('cyanaudit.archive_tablespace') as tablespace ";

my ($tablespace) = $handle->selectrow_array($q)
    or die "Could not determine audit log tablespace\n";

print "Cyan Audit tablespace is '$tablespace'\n";


### Make a name for the archived table

my @time = localtime;

my $table_name = sprintf "tb_audit_event_%04d%02d%02d_%02d%02d",
                         $time[5] + 1900, 
                         $time[4] + 1, 
                         $time[3], 
                         $time[2], 
                         $time[1];

### Verify that we actually have work to do

$q = "select recorded from $schema.tb_audit_event_current limit 1";
$handle->selectrow_array($q) or die "No events to rotate. Exiting.\n";



$handle->do("BEGIN");

### Rename (archive) table

print "Archiving audit event table to $schema.$table_name... ";

$q = "ALTER TABLE $schema.tb_audit_event_current RENAME TO $table_name ";
$handle->do($q) or die "Could not rename current audit event table\n";

print "Done\n";


### Create new current table

print "Creating new $schema.tb_audit_event_current... ";

$q = "CREATE TABLE $schema.tb_audit_event_current() "
   . " INHERITS ($schema.tb_audit_event) TABLESPACE $tablespace ";
$handle->do($q) or die "Could not create new current audit event table\n";

$handle->do("COMMIT");

print "Done\n";


$handle->do("begin");

### Set extension ownership and permissions of table

print "Setting permissions and extension ownership of new table... ";

$q = "alter extension cyanaudit add table $schema.tb_audit_event_current";
$handle->do($q) or die "Could not set extension ownership of new audit table\n";

$q = "grant insert on $schema.tb_audit_event_current to public";
$handle->do($q) or die "Could not grant audit event table permissions\n";

$q = "grant select (audit_transaction_type, txid) "
   . " on $schema.tb_audit_event_current to public ";
$handle->do($q) or die "Could not grant audit event table permissions\n";

$q = "grant update (audit_transaction_type) "
   . " on $schema.tb_audit_event_current to public ";
$handle->do($q) or die "Could not grant audit event table permissions\n";

$q = "revoke all privileges on $schema.$table_name from public";
$handle->do($q) or die "Could not revoke public access to old audit table\n";

print "Done.\n";


### Rename indexes on archived table

print "Renaming indexes on archived table... ";

$q = "alter index $schema.tb_audit_event_current_txid_idx "
   . "  rename to ${table_name}_txid_idx ";
$handle->do($q) or die "Could not rename txid index\n";

$q = "alter index $schema.tb_audit_event_current_recorded_idx "
   . "  rename to ${table_name}_recorded_idx ";
$handle->do($q) or die "Could not rename recorded index\n";

$q = "alter index $schema.tb_audit_event_current_audit_field_idx "
   . "  rename to ${table_name}_audit_field_idx ";
$handle->do($q) or die "Could not rename audit_field index\n";

print "Done\n";


### Create indexes on new table

print "Creating indexes on new table... ";

$q = "create index tb_audit_event_current_txid_idx "
   . " on $schema.tb_audit_event_current(txid) tablespace $tablespace ";
$handle->do($q) or die "Could not create new txid index\n";

$q = "create index tb_audit_event_current_recorded_idx "
   . " on $schema.tb_audit_event_current(recorded) tablespace $tablespace ";
$handle->do($q) or die "Could not create new recorded index\n";

$q = "create index tb_audit_event_current_audit_field_idx "
   . " on $schema.tb_audit_event_current(audit_field) tablespace $tablespace ";
$handle->do($q) or die "Could not create new audit_field index\n";

print "Done\n";



print "Updating check constraints on old and new tables... ";

### Drop constraint

$q = "alter table $schema.$table_name "
   . "drop constraint if exists tb_audit_event_current_recorded_check ";

$handle->do($q);


### Get mins & maxes for creating check constraints

$q = "select min(recorded), max(recorded), min(txid), max(txid) "
   . "  from $schema.$table_name ";
my ($min_recorded, $max_recorded, $min_txid, $max_txid) = $handle->selectrow_array($q)
    or die "Could not get min/max txid/recorded from old audit table\n";

$q = "alter table $schema.$table_name "
   . "  add check( recorded between '$min_recorded' and '$max_recorded' ), "
   . "  add check( txid     between '$min_txid'     and '$max_txid'     )  ";
$handle->do($q) or die "Could not set range constraints on old audit table\n";

$q = "alter table $schema.tb_audit_event_current "
   . "  add check( recorded >= '$max_recorded' ) ";
$handle->do($q) or die "Could not set range constraints on new audit table\n";

$handle->do("commit");

print "Done\n";
