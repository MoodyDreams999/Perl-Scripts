#!/usr/bin/perl --
use strict;
use warnings;
use DBI;
use Time::Piece;
use Time::Seconds;
use POSIX qw(strftime);

# default path to astguiclient configuration file:
my $PATHconf = '/etc/astguiclient.conf';

# Parse configuration
open(my $conf_fh, '<', $PATHconf) || die "can't open $PATHconf: $!\n";
my @conf = <$conf_fh>;
close($conf_fh);

my ($PATHlogs, $PATHsounds, $VARserver_ip, $VARDB_server, $VARDB_database, 
    $VARDB_user, $VARDB_pass, $VARDB_custom_user, $VARDB_custom_pass, $VARDB_port);

foreach my $line (@conf) {
    $line =~ s/ |>|\n|\r|\t|\#.*|;.*//gi;
    if ($line =~ /^PATHlogs/) { $PATHlogs = $line; $PATHlogs =~ s/.*=//gi; }
    if ($line =~ /^PATHsounds/) { $PATHsounds = $line; $PATHsounds =~ s/.*=//gi; }
    if ($line =~ /^VARserver_ip/) { $VARserver_ip = $line; $VARserver_ip =~ s/.*=//gi; }
    if ($line =~ /^VARDB_server/) { $VARDB_server = $line; $VARDB_server =~ s/.*=//gi; }
    if ($line =~ /^VARDB_database/) { $VARDB_database = $line; $VARDB_database =~ s/.*=//gi; }
    if ($line =~ /^VARDB_user/) { $VARDB_user = $line; $VARDB_user =~ s/.*=//gi; }
    if ($line =~ /^VARDB_pass/) { $VARDB_pass = $line; $VARDB_pass =~ s/.*=//gi; }
    if ($line =~ /^VARDB_custom_user/) { $VARDB_custom_user = $line; $VARDB_custom_user =~ s/.*=//gi; }
    if ($line =~ /^VARDB_custom_pass/) { $VARDB_custom_pass = $line; $VARDB_custom_pass =~ s/.*=//gi; }
    if ($line =~ /^VARDB_port/) { $VARDB_port = $line; $VARDB_port =~ s/.*=//gi; }
}

if (!$VARDB_port) { $VARDB_port = '3306'; }

# Connect to database
my $dbh = DBI->connect("DBI:mysql:$VARDB_database:$VARDB_server:$VARDB_port", $VARDB_user, $VARDB_pass, { RaiseError => 1, PrintError => 1 })
    or die "Couldn't connect to database: " . DBI->errstr;

# Configure date range - adjust as needed
# Default: process the last 30 days
my $days_to_process = 30;

# If you want to process specific date range, set these variables
my $start_date = '';  # Format: YYYY-MM-DD, leave empty to use days_to_process
my $end_date = '';    # Format: YYYY-MM-DD, leave empty to use days_to_process

# If specific date range is not provided, calculate based on days_to_process
if (!$start_date) {
    my $today = localtime;
    my $start = $today - (ONE_DAY * $days_to_process);
    $start_date = $start->ymd;
    $end_date = $today->ymd;
}

print "Processing QA data updates for daily records from $start_date to $end_date\n";

# Get all daily records in the date range
my $query_daily_records = "
    SELECT id, user, DATE(event_time) AS record_date
    FROM asterisk.iconic_backend_apd_all_tl_history
    WHERE report = 'D'
    AND DATE(event_time) BETWEEN ? AND ?
    ORDER BY event_time
";

my $sth_daily_records = $dbh->prepare($query_daily_records);
$sth_daily_records->execute($start_date, $end_date);

my %records_by_date;
my @all_records;

while (my $record = $sth_daily_records->fetchrow_hashref) {
    my $date = $record->{record_date};
    
    # Group records by date for efficient processing
    if (!exists $records_by_date{$date}) {
        $records_by_date{$date} = [];
    }
    
    push @{$records_by_date{$date}}, $record;
    push @all_records, $record;
}

if (!@all_records) {
    print "No daily records found in the specified date range.\n";
    $dbh->disconnect();
    exit;
}

print "Found " . scalar(@all_records) . " daily records across " . scalar(keys %records_by_date) . " dates\n";

# Process each date
my $total_updates = 0;
my $total_skipped = 0;
my $total_errors = 0;

foreach my $date (sort keys %records_by_date) {
    print "\nProcessing QA data for date: $date\n";
    
    # Fetch QA data for this date
    my $query_qa_data = "
        SELECT 
            fail.user as lead_user,
            count(case when push='1' then 1 else null end) as push,
            count(case when `type`='warning' then 1 else null end) as warning,
            count(case when `type`='fail' then 1 else null end) as fail,
            count(case when `type`='pass' then 1 else null end) as passed,
            count(case when (`type`='warning' OR `type`='fail') AND reviewed='N' then 1 else null end) as unreviewed,
            count(*) as total
        FROM 
            asterisk.iconic_backend_qa_fail fail
        WHERE 
            DATE(call_date) = ?
        GROUP BY 
            user
    ";
    
    my $sth_qa_data = $dbh->prepare($query_qa_data);
    $sth_qa_data->execute($date);
    
    my %qa_data;
    while (my $row = $sth_qa_data->fetchrow_hashref) {
        my $user = $row->{lead_user};
        
        # Calculate fail rate
        my $fail_rate = 0.0;
        my $total_reviews = $row->{fail} + $row->{warning} + $row->{passed};
        if ($total_reviews > 0) {
            $fail_rate = ($row->{fail} / $total_reviews) * 100;
        }
        
        $qa_data{$user} = {
            pushes     => $row->{push} // 0,
            warning    => $row->{warning} // 0,
            fails      => $row->{fail} // 0,
            passed     => $row->{passed} // 0,
            unreviewed => $row->{unreviewed} // 0,
            fail_rate  => sprintf("%.2f", $fail_rate) // 0.0
        };
    }
    $sth_qa_data->finish();
    
    my $qa_users_count = scalar(keys %qa_data);
    print "Found QA data for $qa_users_count users on $date\n";
    
    # Update records for this date
    my $updates_for_date = 0;
    my $skipped_for_date = 0;
    my $errors_for_date = 0;
    
    my $update_query = "
        UPDATE asterisk.iconic_backend_apd_all_tl_history
        SET 
            pushes = ?,
            warning = ?,
            failed = ?,
            passed = ?,
            unreviewed = ?,
            fail_rate = ?
        WHERE 
            id = ?
    ";
    
    my $sth_update = $dbh->prepare($update_query);
    
    foreach my $record (@{$records_by_date{$date}}) {
        my $user = $record->{user};
        my $id = $record->{id};
        
        if (exists $qa_data{$user}) {
            eval {
                $sth_update->execute(
                    $qa_data{$user}->{pushes},
                    $qa_data{$user}->{warning},
                    $qa_data{$user}->{fails},
                    $qa_data{$user}->{passed},
                    $qa_data{$user}->{unreviewed},
                    $qa_data{$user}->{fail_rate},
                    $id
                );
                
                $updates_for_date++;
                $total_updates++;
            };
            
            if ($@) {
                print "Error updating record ID $id for user $user: $@\n";
                $errors_for_date++;
                $total_errors++;
            }
        } else {
            # No QA data found for this user on this date
            $skipped_for_date++;
            $total_skipped++;
        }
    }
    
    print "Date $date: Updated $updates_for_date records, skipped $skipped_for_date records, encountered $errors_for_date errors\n";
}

print "\nSummary: Processed " . scalar(@all_records) . " records\n";
print "  - Updated: $total_updates\n";
print "  - Skipped (no QA data): $total_skipped\n";
print "  - Errors: $total_errors\n";

# Disconnect from database
$dbh->disconnect();
print "Database connection closed\n";
print "Script completed successfully\n";