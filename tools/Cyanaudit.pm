package Cyanaudit;

use utf8;
use strict;
use feature "state";

use DBI;
use File::Basename;
use Exporter;
use Digest::MD5;
use Data::Dumper;

use vars qw( @ISA @EXPORT );

@ISA = qw( Exporter );

@EXPORT = qw(
    db_connect
    md5_write
    md5_verify
    get_cyanaudit_schema
    get_cyanaudit_data_table_list
);

sub db_connect($)
{
    my ($params) = @_;

    my $port = $params->{'p'} || $ENV{'PGPORT'} || 5432;
    my $host = $params->{'h'} || $ENV{'PGHOST'} || '/tmp';
    my $user = $params->{'U'} || $ENV{'PGUSER'} || 'postgres';
    my $dbname = $params->{'d'} || $ENV{'PGDATABASE'}
        or die "Must specify database name or set PGDATABASE environment variable\n";

    $port =~ /^\d+$/ or die "Invalid port: '$port'\n";
    
    my $connect_string = "dbi:Pg:host=$host;dbname=$dbname;port=$port";

    my $handle = DBI->connect( $connect_string, $user, '', { RaiseError => 1 } )
        or die "Database connect error. Please verify .pgpass and environment variables\n";

    return $handle;
}

sub md5_write($)
{
    my ($file_path) = @_;

    my $file_base = basename($file_path);
    my $md5file_path = "$file_path.md5";

    open( my $fh, "<", $file_path ) or die "Can't open $file_path for reading: $!\n";
    binmode( $fh );
    my $md5sum = Digest::MD5->new->addfile($fh)->hexdigest;
    close $fh;

    open( $fh, ">", $md5file_path ) or die "Can't open $md5file_path for writing: $!\n";
    print $fh "$md5sum  $file_base\n" or die "Can't write to $md5file_path: $!\n";
    close $fh;

    return 1;
}

sub md5_verify($)
{
    my ($file_path) = @_;
    
    my $file_base = basename($file_path);
    my $md5file_path = "$file_path.md5";

    -f $file_path or return undef;
    -f $md5file_path or return undef;
    -r $md5file_path or return undef;

    open( my $fh, "<", $md5file_path ) or die "Can't open $md5file_path for reading: $!\n";
    my @md5_lines = <$fh>;
    close $fh;

    foreach my $line (@md5_lines)
    {
        chomp( $line );
        my( $stored_md5sum, $stored_filename ) = $line =~ m/^([0-9a-f]+)\s+(.*)$/;
        next unless ( $stored_md5sum and $stored_filename );
        next unless ( $stored_filename eq $file_base );

        open( $fh, "<", $file_path ) or die "Can't open $file_path for reading: $!\n";
        binmode( $fh );
        my $md5sum = Digest::MD5->new->addfile($fh)->hexdigest;
        close $fh;

        return 1 if ( $md5sum eq $stored_md5sum );
    }

    return undef;
}

sub get_cyanaudit_schema($)
{
    my( $handle ) = @_;

    state $schema;

    unless( $schema )
    {
        my $schema_q = <<SQL;
            select n.nspname
              from pg_extension e
              join pg_namespace n
                on e.extnamespace = n.oid
             where e.extname = 'cyanaudit'
SQL

        my $schema_row = $handle->selectrow_arrayref($schema_q)
            or return undef;

        $schema = $schema_row->[0];
    }

    return $schema;
}

sub get_cyanaudit_data_table_list($)
{
    my( $handle ) = @_;

    my $schema = get_cyanaudit_schema( $handle );

    my $tables_q = <<SQL;
        select c.relname as table_name,
               pg_size_pretty(pg_total_relation_size(c.oid)) as table_size_pretty
          from pg_class c
          join pg_namespace n
            on c.relnamespace = n.oid
         where c.relkind = 'r'
           and n.nspname = '$schema'
           and c.relname ~ '^tb_audit_event_(\\d{8}_\\d{4}\$|current)'
         order by 1
SQL

    return $handle->selectall_arrayref( $tables_q, { Slice => {} } );
}
