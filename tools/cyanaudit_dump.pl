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

    print "Usage: %0 -m months_to_keep [ options ... ]\n"
        . "Options:\n"
        . "  -d db      Connect to database by given Name\n"
        . "  -U user    Connect to database as given User\n"
        . "  -h host    Connect to database on given Host\n"
        . "  -p port    Connect to database on given Port\n"
        . "  -a         Back up All audit tables\n"
        . "  -c         Clobber (overwrite) existing files. Default is to skip these.\n"
        . "  -r         Remove table from database once it has been archived\n"
        . "  -z         gzip output file\n"
        . "  -o dir     Output directory (default current directory)\n";

    exit 1;
}


getopts('m:o:U:h:p:d:zrac', \%opts) or usage();

my $months = $opts{'m'};

unless( defined $months )
{
    usage("Must specify a number of months of audit data to keep ( -m )");
}

unless( $months =~ /^\d+$/ and $months >= 0 and $months < 120 )
{
    usage("Invalid number of months '$months' specified");
}

my $outdir = '.';

if( $opts{'o'} )
{
    $outdir = $opts{'o'};

    usage("Output directory '$outdir' is invalid") unless( -d $outdir );
    usage("Output directory '$outdir' is not writable") unless ( -w $outdir );
}

my @connect_params;

if( $opts{'p'} )
{
    if( $opts{'p'} !~ /^\d+$/ )
    {
        usage("Invalid port '$opts{'p'}' specified");
    }

    push @connect_params, 'port=' . $opts{'p'};
}

if( $opts{'h'} )
{
    push @connect_params, 'host=' . $opts{'h'};
}

if( $opts{'d'} )
{
    push @connect_params, 'dbname=' . $opts{'d'};
}

my $username = '';

if( $opts{'U'} )
{
    $username = $opts{'U'};
}

my $connect_string = join( ';', @connect_params );

my $handle = DBI->connect("dbi:Pg:$connect_string", $username, '') 
    or die "Database connect error. Please verify .pgpass and environment variables\n";

my $schema_q = "select n.nspname "
             . "  from pg_extension e "
             . "  join pg_namespace n "
             . "    on e.extnamespace = n.oid "
             . " where e.extname = 'cyanaudit'";

my $schema_row = $handle->selectrow_arrayref($schema_q)
    or die "Could not determine audit log schema\n";

my $schema = $schema_row->[0];

print "Found cyanaudit in schema '$schema'\n";

my $user_table_q = "select current_setting('cyanaudit.user_table') "
                 . "        as user_table, "
                 . "       current_setting('cyanaudit.user_table_email_col') "
                 . "        as user_table_email_col, "
                 . "       current_setting('cyanaudit.user_table_uid_col') "
                 . "        as user_table_uid_col ";

my $user_table_row = $handle->selectrow_arrayref($user_table_q);

my ($user_table, $user_table_email_col, $user_table_uid_col) = @$user_table_row;

unless( $user_table and $user_table_email_col and $user_table_uid_col )
{
    die "Could not get cyanaudit settings for user table from postgresql.conf\n";
}

my $tables_q  = "select c.relname, ";
   $tables_q .= "       pg_size_pretty(pg_total_relation_size(c.oid)), ";
   $tables_q .= "       c.relname < 'tb_audit_event_' ";
   $tables_q .= "       || to_char(now() - interval '$months months', 'YYYYMMDD_HH24MI') ";
   $tables_q .= "  from pg_class c ";
   $tables_q .= "  join pg_namespace n ";
   $tables_q .= "    on c.relnamespace = n.oid ";
   $tables_q .= " where c.relkind = 'r' ";
   $tables_q .= "   and n.nspname = '$schema' ";
   $tables_q .= "   and c.relname ~ '^tb_audit_event_\\d{8}_\\d{4}\$' ";
   unless( $opts{'a'} )
   {
        $tables_q .= "   and c.relname < 'tb_audit_event_' ";
        $tables_q .= "       || to_char(now() - interval '$months months', 'YYYYMMDD_HH24MI') ";
   }
   $tables_q .= " order by 1 ";

my $table_rows = $handle->selectall_arrayref($tables_q)
    or die "Could not get list of audit archive tables\n";

foreach my $table_row (@$table_rows)
{
    my ($table, $size, $remove) = @$table_row;
    my $file = "$table.csv";
    
    if( glob( "$outdir/$file.*" ) and not $opts{'c'} )
    {
        next;
    }

    my $exporting_msg = "Exporting $schema.$table ($size)";

    print "$exporting_msg: Preparing... ";

    my $open_str = "> $outdir/$table.csv";

    if( $opts{'z'} )
    {
        $open_str = "| gzip -9 -c $open_str.gz";
    }

    open( my $fh, $open_str ) or die "Could not open output for writing: $!\n";

    my $min_q = "select audit_event "
              . "  from $schema.$table "
              . " where recorded = "
              . "       ( "
              . "           select min(recorded) "
              . "             from $schema.$table "
              . "       ) "
              . " limit 1 ";

    my ($min_audit_event) = $handle->selectrow_array($min_q);

    my $max_q = "select audit_event "
              . "  from $schema.$table "
              . " where recorded = "
              . "       ( "
              . "           select max(recorded) "
              . "             from $schema.$table "
              . "       ) "
              . " limit 1 ";

    my ($max_audit_event) = $handle->selectrow_array($max_q);

    my $data_q = "   select ae.audit_event, "
               . "          ae.txid, "
               . "          ae.recorded, "
               . "          ae.uid, "
               . "          u.$user_table_email_col, "
               . "          af.table_name, "
               . "          af.column_name, "
               . "          adt.name as data_type, "
               . "          ae.row_pk_val, "
               . "          ae.row_op, "
               . "          ae.pid, "
               . "          att.label as description, "
               . "          ae.old_value, "
               . "          ae.new_value "
               . "     from $schema.$table ae "
               . "     join $schema.tb_audit_field af "
               . "       on ae.audit_field = af.audit_field "
               . "     join $schema.tb_audit_data_type adt "
               . "       on adt.audit_data_type = af.audit_data_type "
               . "left join $schema.tb_audit_transaction_type att "
               . "       on ae.audit_transaction_type = att.audit_transaction_type "
               . "     join $user_table u "
               . "       on ae.uid = u.$user_table_uid_col "
               . " order by recorded ";

    $handle->do("copy ($data_q) to stdout with csv header");

    my $row;
    my $row_count = 0;

    while( $handle->pg_getcopydata(\$row) >= 0 )
    {
        print $fh $row or die "Error writing to file: $!\n";

        if( $row_count > 1 and $row_count % 1000 == 1 )
        {
            (my $current_audit_event = $row) =~ s/,.*$//s;

            printf "\r$exporting_msg: %0.1f%% complete... ",
                100 * ($current_audit_event - $min_audit_event) /
                      ($max_audit_event - $min_audit_event);
        }

        $row_count++;
    }

    print "Done!\n";

    if( $opts{'r'} and $remove )
    {
        print "Dropping table $schema.$table... ";
        $handle->do("alter extension cyanaudit drop table $schema.$table");
        $handle->do("drop table $schema.$table");
        print "Done\n";
    }
}
