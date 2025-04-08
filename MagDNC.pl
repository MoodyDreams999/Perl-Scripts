#!/usr/bin/perl --

#use strict;
#use warnings;
use DBI;
use Time::Piece;
use Time::Seconds;
use Excel::Writer::XLSX;
use MIME::Lite;

#path to astguiclient configuration file:
$PATHconf = '/etc/astguiclient.conf';

open(conf, "$PATHconf") || die "can't open $PATHconf: $!\n";
@conf = <conf>;
close(conf);
$i=0;
foreach(@conf){
    $line = $conf[$i];
    $line =~ s/ |>|\n|\r|\t|\#.*|;.*//gi;
    if ( ($line =~ /^PATHlogs/) && ($CLIlogs < 1) )
    {$PATHlogs = $line;   $PATHlogs =~ s/.*=//gi;}
    if ( ($line =~ /^PATHsounds/) && ($CLIsounds < 1) )
        {$PATHsounds = $line;   $PATHsounds =~ s/.*=//gi;}
    if ( ($line =~ /^VARserver_ip/) && ($CLIserver_ip < 1) )
        {$VARserver_ip = $line;   $VARserver_ip =~ s/.*=//gi;}
    if ( ($line =~ /^VARDB_server/) && ($CLIDB_server < 1) )
        {$VARDB_server = $line;   $VARDB_server =~ s/.*=//gi;}
    if ( ($line =~ /^VARDB_database/) && ($CLIDB_database < 1) )
        {$VARDB_database = $line;   $VARDB_database =~ s/.*=//gi;}
    if ( ($line =~ /^VARDB_user/) && ($CLIDB_user < 1) )
        {$VARDB_user = $line;   $VARDB_user =~ s/.*=//gi;}
    if ( ($line =~ /^VARDB_pass/) && ($CLIDB_pass < 1) )
        {$VARDB_pass = $line;   $VARDB_pass =~ s/.*=//gi;}
    if ( ($line =~ /^VARDB_custom_user/) && ($CLIDB_custom_user < 1) )
        {$VARDB_custom_user = $line;   $VARDB_custom_user =~ s/.*=//gi;}
    if ( ($line =~ /^VARDB_custom_pass/) && ($CLIDB_custom_pass < 1) )
       {$VARDB_custom_pass = $line;   $VARDB_custom_pass =~ s/.*=//gi;}
    if ( ($line =~ /^VARDB_port/) && ($CLIDB_port < 1) )
        {$VARDB_port = $line;   $VARDB_port =~ s/.*=//gi;}
    $i++;
}

        if (!$VARDB_port) {$VARDB_port='3306';}

my $today = localtime;
# Time::Piece: wday returns 1 for Sunday, 2 for Monday, ... 7 for Saturday.
my $wday = $today->wday;

my $monday;
if ($wday == 1) {       # Sunday: use last week's Monday
    $monday = $today - ONE_DAY * 6;
} elsif ($wday == 7) {   # Saturday: use this week’s Monday (5 days ago)
    $monday = $today - ONE_DAY * 5;
} else {                # Monday-Friday
    $monday = $today - ONE_DAY * ($wday - 2);
}
my $friday = $monday + ONE_DAY * 4;
my $monday_str = $monday->strftime('%Y-%m-%d');
my $friday_str = $friday->strftime('%Y-%m-%d');

#-----------------------------------------------------------

my $query = qq{
    SELECT phone_number, action_date
    FROM asterisk.vicidial_dnc_log
    WHERE campaign_id = 'MOR'
      AND DATE(action_date) BETWEEN '$monday_str' AND '$friday_str'
};


my $dbh = DBI->connect("DBI:mysql:$VARDB_database:$VARDB_server:$VARDB_port", 
                       $VARDB_user, $VARDB_pass, { RaiseError => 1, AutoCommit => 1 })
    or die "Couldn't connect to database: " . DBI->errstr;


my $sth = $dbh->prepare($query);
$sth->execute();


# Output file in /home/MagDNC/ 
my $filename = '/home/MagDNC/Mag_DNC_' . $friday_str . '.xlsx';
my $workbook = Excel::Writer::XLSX->new($filename);
my $worksheet = $workbook->add_worksheet('DNC Report');

# Write header row
my @headers = ('Phone Number', 'Action Date');
my $col = 0;
foreach my $header (@headers) {
    $worksheet->write(0, $col, $header);
    $col++;
}

# Write data rows
my $row = 1;
while (my @data = $sth->fetchrow_array) {
    $worksheet->write_row($row, 0, \@data);
    $row++;
}

$workbook->close();

#-----------------------------------------------------------
# Email the Excel file out
#-----------------------------------------------------------
# Set email parameters – adjust From, To, Bcc, and Subject as required.
my $to      = 'list@iconicresults.com';  # change to your recipients
my $from    = 'no-reply@iconicresults.com';
my $bcc     = 'shenson@iconicresults.com';#'iconicresultsaz@gmail.com' 'dnc@magnoliabank.com';
my $subject = "Mag Weekly DNC for the Week $monday_str to $friday_str";

# Create MIME::Lite message with attachment
my $msg = MIME::Lite->new(
    From    => $from,
    To      => $to,
    Bcc     => $bcc,
    Subject => $subject,
    Type    => 'multipart/mixed',
);

# Add a plain text part (you can add a message here)
$msg->attach(
    Type => 'TEXT',
    Data => "Please find attached the DNC report for the work week $monday_str to $friday_str.",
);

# Attach the Excel file
$msg->attach(
    Type => 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    Path => $filename,
    Filename => 'Mag_DNC_' . $friday_str . '.xlsx',
    Disposition => 'attachment',
);

$msg->send;

$sth->finish();
$dbh->disconnect();

# Optionally, you can delete the file if you do not need it to persist:
# unlink($filename);