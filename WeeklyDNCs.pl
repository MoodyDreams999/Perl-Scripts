#!/usr/bin/perl --

use strict;
use warnings;
use DBI;
use Text::CSV;
use MIME::Lite;
use Time::Piece;
use Time::Seconds;

# Default path to astguiclient configuration file
my $PATHconf = '/etc/astguiclient.conf';

open(my $conf_fh, '<', $PATHconf) || die "can't open $PATHconf: $!\n";
my @conf = <$conf_fh>;
close($conf_fh);

# Initialize variables for database connection
my ($VARDB_server, $VARDB_database, $VARDB_user, $VARDB_pass, $VARDB_port);
$VARDB_port = '3306'; # Default port

# Parse configuration file
foreach my $line (@conf) {
    $line =~ s/ |>|\n|\r|\t|\#.*|;.*//gi;
    if ($line =~ /^VARDB_server/) {
        ($VARDB_server) = $line =~ /=(.*)/;
    }
    if ($line =~ /^VARDB_database/) {
        ($VARDB_database) = $line =~ /=(.*)/;
    }
    if ($line =~ /^VARDB_user/) {
        ($VARDB_user) = $line =~ /=(.*)/;
    }
    if ($line =~ /^VARDB_pass/) {
        ($VARDB_pass) = $line =~ /=(.*)/;
    }
    if ($line =~ /^VARDB_port/) {
        ($VARDB_port) = $line =~ /=(.*)/;
    }
}

if (!$VARDB_port) {
    $VARDB_port = '3306';
}

# Connect to the database
my $dbh = DBI->connect("DBI:mysql:$VARDB_database:$VARDB_server:$VARDB_port", "$VARDB_user", "$VARDB_pass", { RaiseError => 1, PrintError => 1 })
    or die "Couldn't connect to database: " . DBI->errstr;

# Calculate the date range for the current week (Monday to Friday)
my $t = localtime;
my $current_day = $t->_wday;  # Day of the week (1 = Monday, ..., 7 = Sunday)

# Find the most recent Monday
my $monday = $t - (($current_day - 1) * ONE_DAY);

# Get the next Friday
my $friday = $monday + (4 * ONE_DAY);

my $start_date = $monday->ymd . ' 00:00:00';
my $end_date = $friday->ymd . ' 20:00:00';
my $display_date = $monday->ymd;
my $displayend_date = $friday->ymd;

# Print debug information
print "Start Date: $start_date\n";
print "End Date: $end_date\n";

# Generate the CSV filename with the current date
my $csv_filename = sprintf("dnc_%s.csv", $t->ymd(''));

# Query to select all columns from vicidial_dnc_log between the start and end dates and for specified campaigns
my $query = "SELECT * FROM asterisk.vicidial_dnc_log WHERE action_date BETWEEN ? AND ? AND (campaign_id = 'MOR' OR campaign_id = '-SYSINT-')";
my $sth = $dbh->prepare($query);
$sth->execute($start_date, $end_date);

# Check number of rows fetched
my $num_rows = $sth->rows;
print "Number of rows fetched: $num_rows\n";

# Create a CSV file
my $csv = Text::CSV->new({ binary => 1, eol => $/ });
my $csv_file = "/home/DNC/$csv_filename"; # Adjust the path as needed
open my $csv_fh, '>', $csv_file or die "Could not open '$csv_file' $!\n";

# Get column names and print to CSV
my @columns = @{$sth->{NAME_lc}};
$csv->print($csv_fh, \@columns);

# Fetch and print all rows to CSV
while (my $row = $sth->fetchrow_arrayref) {
    $csv->print($csv_fh, $row);
}

close $csv_fh;

# Disconnect from the database
$dbh->disconnect;

# Send email with the CSV attachment
my $msg = MIME::Lite->new(
    From    => 'no-reply@conversionkingsleads.com',
    To      => 'shenson@iconicresults.com',
    Subject => "DNC Records for the week of $display_date",
    Type    => 'multipart/mixed',
);

$msg->attach(
    Type        => 'TEXT',
    Data        => "Please find attached the Vicidial DNC Log report from $display_date to $displayend_date.",
);

$msg->attach(
    Type        => 'application/csv',
    Path        => $csv_file,
    Filename    => $csv_filename,
    Disposition => 'attachment',
);

$msg->send;

print "Email sent successfully with attachment: $csv_file\n";
