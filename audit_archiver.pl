#!/usr/bin/perl -w

use strict;

$| = 1;

use DBI;
use Getopt::Std;
use Data::Dumper;

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
        . "  -d db      Connect to database by given name\n"
        . "  -U user    Connect to database as given user\n"
        . "  -h host    Connect to database on given host\n"
        . "  -p port    Connect to database on given port\n"
        . "  -r         Remove table from database once it has been archived\n"
        . "  -z         gzip output file\n"
        . "  -o dir     Output directory (default current directory)\n";

    exit 1;
}


getopt('m:o:U:h:p:d:zr', \%opts) or usage();

my $months = $opts{'m'};

unless( $months )
{
    usage("Must specify a number of months of audit data to keep ( -m )");
}

unless( $months =~ /^\d+$/ and $months > 0 and $months < 120 )
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
             . " where e.extname = 'auditlog'";

my $schema_row = $handle->selectrow_arrayref($schema_q)
    or die "Could not determine audit log schema\n";

my $schema = $schema_row->[0];

print "Found auditlog in schema '$schema'\n";

my $user_table_q = "select current_setting('auditlog.user_table') "
                 . "        as user_table, "
                 . "       current_setting('auditlog.user_table_email_col') "
                 . "        as user_table_email_col, "
                 . "       current_setting('auditlog.user_table_uid_col') "
                 . "        as user_table_uid_col ";

my $user_table_row = $handle->selectrow_arrayref($user_table_q);

my ($user_table, $user_table_email_col, $user_table_uid_col) = @$user_table_row;

unless( $user_table and $user_table_email_col and $user_table_uid_col )
{
    die "Could not get auditlog settings for user table from postgresql.conf\n";
}

my $tables_q = "select c.relname, "
             . "       pg_size_pretty(pg_relation_size(c.oid)) "
             . "  from pg_class c "
             . "  join pg_namespace n "
             . "    on c.relnamespace = n.oid "
             . " where c.relkind = 'r' "
             . "   and n.nspname = '$schema' "
             . "   and c.relname < 'tb_audit_event_' "
             . "    || to_char(now() - interval '$months months', 'YYYYMMDD_HHMI') "
             . "   and c.relname ~ '^tb_audit_event_\\d{8}_\\d{4}\$' "
             . " order by 1 ";

my $table_rows = $handle->selectall_arrayref($tables_q)
    or die "Could not get list of audit archive tables\n";

foreach my $table_row (@$table_rows)
{
    my ($table, $size) = @$table_row;

    my $exporting_msg = "Exporting $schema.$table ($size)";

    print "$exporting_msg: Preparing... ";

    my $open_str = "> $outdir/$table.csv";

    if( exists $opts{'z'} )
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
               . "          af.table_name, "
               . "          af.column_name, "
               . "          ae.row_pk_val, "
               . "          ae.uid, "
               . "          u.$user_table_email_col, "
               . "          ae.row_op, "
               . "          ae.pid, "
               . "          att.label as description, "
               . "          ae.old_value, "
               . "          ae.new_value "
               . "     from $schema.$table ae "
               . "     join $schema.tb_audit_field af "
               . "       on ae.audit_field = af.audit_field "
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
        print $fh $row;

        if( $row_count > 1 and $row_count % 1000 == 1 )
        {
            (my $current_audit_event = $row) =~ s/,.*$//s;

            printf "\r$exporting_msg: %0.2f%% complete... ",
                100 * ($current_audit_event - $min_audit_event) /
                      ($max_audit_event - $min_audit_event);
        }

        $row_count++;
    }

    print "Done!\n";
}


