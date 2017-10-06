#!/usr/bin/perl

use strict;
use warnings;

use DBI;
use Getopt::Std;
use File::Basename qw(dirname basename);
use File::Copy;

use lib dirname(__FILE__) . '/tools';

use Cyanaudit;

sub usage
{
    my ($message) = @_;

    print "Error: $message\n" if( $message );

    print "Usage: $0 [ options ... ]\n"
        . "  -h host    database server host or socket directory\n"
        . "  -p port    database server port\n"
        . "  -U user    database user name\n"
        . "  -d db      database name\n"
        . "  -V #.#     version to upgrade to (blank will install latest version)\n";

    exit 1
}

chomp( my $pg_version = `pg_config --version` );
if( $pg_version =~ / 8\.| 9\.[012345]\b/ )
{
    die "Cyan Audit requires PostgreSQL 9.6 or above.\n";
}

my %opts;

getopts('U:h:p:d:V:', \%opts) or usage();

my $db   = ( $opts{d} || $ENV{PGDATABASE} ) or usage( "Please specify database using -d" );
my $port = ( $opts{p} || $ENV{PGPORT} || 5432 );
my $user = ( $opts{U} || $ENV{PGUSER} || 'postgres' );
my $host = ( $opts{h} || $ENV{PGHOST} || 'localhost' );

my $handle = db_connect( \%opts ) or die "Database connect error.\n";


my $version_query = "select value from cyanaudit.tb_config where name = 'version'";
my ($current_version) = $handle->selectrow_array( $version_query );
my $new_version = $opts{V} || $current_version;


my @files_to_execute;

my $sql_dir = dirname(__FILE__) . "/sql";

my $base_sql = "$sql_dir/cyanaudit--$new_version.sql";
my $pre_sql = "$sql_dir/cyanaudit--$current_version--$new_version--pre.sql";
my $post_sql = "$sql_dir/cyanaudit--$current_version--$new_version--post.sql";

unless( -r $base_sql )
{
    usage( "Invalid version ($new_version): File not found: $base_sql" );
}

if( $current_version ne $new_version )
{
    unless( -r $pre_sql or -r $post_sql )
    {
        die "Upgrade scripts from $current_version to $new_version not found.\n";
    }
}


for my $script ($pre_sql, $base_sql, $post_sql)
{
    next unless ( -r $script );

    my $command = "psql -U $user -d $db -p $port -h $host -f '$script' > /dev/null";
    #print "Running: $command\n";
    print "Running $script ... ";
    system( $command ) == 0 or die;
    print "Success!\n";
}

print "Getting PostgreSQL bin directory: ";
my $bindir = `pg_config --bindir` or die;
chomp( $bindir );
print "$bindir\n";

my @files = <tools/*.p[lm]>;
print "Copying scripts to $bindir...\n";
foreach my $file (@files)
{
    copy( $file, $bindir ) or die "$!";
    my $dest = $bindir . '/' . basename($file);
    print "- $dest\n";
    chmod( 0755, $dest ) or die "$!";
}
print "Done!\n";
