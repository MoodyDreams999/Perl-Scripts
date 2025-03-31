#!/usr/bin/perl
use DBI;
use Time::Piece;
use Time::Seconds;
use POSIX qw(strftime);

# default path to astguiclient configuration file:
$PATHconf = '/etc/astguiclient.conf';

open(conf, "$PATHconf") || die "can't open $PATHconf: $!\n";
@conf = <conf>;
close(conf);
$i=0;
foreach(@conf)
{
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

# Connect to database
my $dbh = DBI->connect("DBI:mysql:$VARDB_database:$VARDB_server:$VARDB_port", $VARDB_user, $VARDB_pass, { RaiseError => 1, PrintError => 0 })
    or die "Couldn't connect to database: " . DBI->errstr;

# Fetch users and required fields from iconic_backend_apd_all_tl
my $query_users = "SELECT user, ugroup, name, calls, davg, xfer, xfrena, total, ratio, talk_time, wait_time, 
                          pause_time, dispo_time, report, event_time, vici_table, closer_table, TC
                   FROM asterisk.iconic_backend_apd_all_tl 
                   WHERE report='D'";

my $sth_users = $dbh->prepare($query_users);
$sth_users->execute();

my %user_data; 
while (my @row = $sth_users->fetchrow_array) {
    my ($user, $ugroup, $name, $calls, $davg, $xfer, $xfrena, $total, $ratio, $talk_time, $wait_time, 
        $pause_time, $dispo_time, $report, $event_time, $vici_table, $closer_table, $TC) = @row;

    $user_data{$user} = { 
        ugroup       => $ugroup,
        name         => $name,
        calls        => $calls,
        davg         => $davg,
        xfer         => $xfer,
        xfrena       => $xfrena,
        total        => $total,
        ratio        => $ratio,
        talk_time    => $talk_time,
        wait_time    => $wait_time,
        pause_time   => $pause_time,
        dispo_time   => $dispo_time,
        report       => $report,
        event_time   => $event_time,
        vici_table   => $vici_table,
        closer_table => $closer_table,
        TC           => $TC
    };
}
$sth_users->finish();

# Exit if no users found
if (!%user_data) {
    print "No users found for query.\n";
    $dbh->disconnect();
    exit;
}

my $today = localtime;
#my $yesterday = $today -= (2 * ONE_DAY);
#my $yesterday = $today - ONE_DAY;
#my $event_date = $yesterday->ymd;
my $event_date = $today->ymd;


# Prepare user list for next query
my $user_list = join(",", map { "'$_'" } keys %user_data);

# Fetch daily hours for users from iconic_daily_hours
my $query_hours = "SELECT user, event_date, tHours FROM asterisk.iconic_daily_hours 
                   WHERE DATE(event_date) = ? AND `user` IN ($user_list);";
my $sth_hours = $dbh->prepare($query_hours);
$sth_hours->execute($event_date);

my @insert_data;
while (my $row = $sth_hours->fetchrow_hashref) {
    my $user = $row->{user};

    push @insert_data, {
        user         => $user,
        ugroup       => $user_data{$user}->{ugroup},
        name         => $user_data{$user}->{name},
        calls        => $user_data{$user}->{calls},
        davg         => $user_data{$user}->{davg},
        xfer         => $user_data{$user}->{xfer},
        xfrena       => $user_data{$user}->{xfrena},
        total        => $user_data{$user}->{total},
        ratio        => $user_data{$user}->{ratio},
        talk_time    => $user_data{$user}->{talk_time},
        wait_time    => $user_data{$user}->{wait_time},
        pause_time   => $user_data{$user}->{pause_time},
        dispo_time   => $user_data{$user}->{dispo_time},
        report       => $user_data{$user}->{report},
        event_time   => $row->{event_date},
		weekday      => Time::Piece->strptime($row->{event_date}, "%Y-%m-%d")->strftime("%a"),
        vici_table   => $user_data{$user}->{vici_table},
        closer_table => $user_data{$user}->{closer_table},
        TC           => $user_data{$user}->{TC},
        hours_worked => $row->{tHours} || 0
    };
}
$sth_hours->finish();

# Exit if no daily hours found
if (!@insert_data) {
    print "No daily hours found for users.\n";
    $dbh->disconnect();
    exit;
}

# Insert data into iconic_backend_apd_all_tl_history
my $query_insert = "INSERT INTO asterisk.iconic_backend_apd_all_tl_history 
                    (`user`, `ugroup`, `name`, `calls`, `davg`, `xfer`, `xfrena`, `total`, `ratio`, `talk_time`, 
                     `wait_time`, `pause_time`, `dispo_time`, `report`, `event_time`, `weekday`, `vici_table`, 
                     `closer_table`, `TC`, `hours_worked`)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

my $sth_insert = $dbh->prepare($query_insert);
foreach my $data (@insert_data) {
    $sth_insert->execute(@{$data}{qw/user ugroup name calls davg xfer xfrena total ratio talk_time wait_time pause_time dispo_time report event_time weekday vici_table closer_table TC hours_worked/});
}
$sth_insert->finish();

print "Data successfully inserted into iconic_backend_apd_all_tl_history.\n";

# Disconnect from database
$dbh->disconnect();
