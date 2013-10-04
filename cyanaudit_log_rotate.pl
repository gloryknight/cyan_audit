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

    print "Usage: %0 [ options ... ]\n"
        . "Options:\n"
        . "  -d db      Connect to database by given name\n"
        . "  -U user    Connect to database as given user\n"
        . "  -h host    Connect to database on given host\n"
        . "  -p port    Connect to database on given port\n"

    exit 1;
}


getopts('U:h:p:d:', \%opts) or usage();

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

my $tablespace_q = "select current_setting('auditlog.tablespace') "
                 . "        as tablespace "

my $user_table_row = $handle->selectrow_arrayref($user_table_q);

my ($tablespace) = @$user_table_row;

die "Could not determine audit log tablespace\n" unless( $tablespace );

