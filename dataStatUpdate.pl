#!/usr/bin/perl --
use strict;
use warnings;
use DBI;
use Time::Piece;
use Time::Seconds;
use POSIX qw(strftime);

# Default path to astguiclient configuration file:
my $PATHconf = '/etc/astguiclient.conf';

# Parse configuration file
open(my $conf_fh, '<', $PATHconf) || die "can't open $PATHconf: $!\n";
my @conf = <$conf_fh>;
close($conf_fh);

my ($PATHlogs, $PATHsounds, $VARserver_ip, $VARDB_server, $VARDB_database, 
    $VARDB_user, $VARDB_pass, $VARDB_custom_user, $VARDB_custom_pass, $VARDB_port);

my $i = 0;
foreach my $line (@conf) {
    $line =~ s/ |>|\n|\r|\t|\#.*|;.*//gi;
    if ($line =~ /^PATHlogs/ && !defined($PATHlogs)) {
        $PATHlogs = $line;
        $PATHlogs =~ s/.*=//gi;
    }
    if ($line =~ /^PATHsounds/ && !defined($PATHsounds)) {
        $PATHsounds = $line;
        $PATHsounds =~ s/.*=//gi;
    }
    if ($line =~ /^VARserver_ip/ && !defined($VARserver_ip)) {
        $VARserver_ip = $line;
        $VARserver_ip =~ s/.*=//gi;
    }
    if ($line =~ /^VARDB_server/ && !defined($VARDB_server)) {
        $VARDB_server = $line;
        $VARDB_server =~ s/.*=//gi;
    }
    if ($line =~ /^VARDB_database/ && !defined($VARDB_database)) {
        $VARDB_database = $line;
        $VARDB_database =~ s/.*=//gi;
    }
    if ($line =~ /^VARDB_user/ && !defined($VARDB_user)) {
        $VARDB_user = $line;
        $VARDB_user =~ s/.*=//gi;
    }
    if ($line =~ /^VARDB_pass/ && !defined($VARDB_pass)) {
        $VARDB_pass = $line;
        $VARDB_pass =~ s/.*=//gi;
    }
    if ($line =~ /^VARDB_custom_user/ && !defined($VARDB_custom_user)) {
        $VARDB_custom_user = $line;
        $VARDB_custom_user =~ s/.*=//gi;
    }
    if ($line =~ /^VARDB_custom_pass/ && !defined($VARDB_custom_pass)) {
        $VARDB_custom_pass = $line;
        $VARDB_custom_pass =~ s/.*=//gi;
    }
    if ($line =~ /^VARDB_port/ && !defined($VARDB_port)) {
        $VARDB_port = $line;
        $VARDB_port =~ s/.*=//gi;
    }
    $i++;
}

if (!$VARDB_port) {
    $VARDB_port = '3306';
}

# CONFIGURATION SECTION
# --------------------------------------------------------
# Configure date range for processing
my $days_to_process = 40;
my $start_date = '';  # Format: YYYY-MM-DD, leave empty to use days_to_process
my $end_date = '';    # Format: YYYY-MM-DD, leave empty to use days_to_process

# Batch processing settings
my $BATCH_SIZE = 10;          # Number of users to process in each batch
my $SLEEP_BETWEEN_BATCHES = 10; # Seconds to sleep between batches
my $SLEEP_BETWEEN_DATES = 20;  # Seconds to sleep between processing different dates

# State file to track progress
my $STATE_FILE = '/tmp/call_metrics_update_state.txt';
my $RESUME_FROM_STATE = 1;     # Set to 1 to resume from previous state if available

# Setup logging
my $log_file = '/tmp/call_metrics_update_' . strftime('%Y%m%d_%H%M%S', localtime) . '.log';
# --------------------------------------------------------

# Initialize logging
sub log_message {
    my ($message) = @_;
    my $timestamp = strftime('%Y-%m-%d %H:%M:%S', localtime);
    my $log_entry = "[$timestamp] $message\n";
    
    # Print to console
    print $log_entry;
    
    # Write to log file
    open(my $log_fh, '>>', $log_file) or die "Cannot open log file: $!";
    print $log_fh $log_entry;
    close($log_fh);
}

# Calculate date range if not specified
if (!$start_date) {
    my $today = localtime;
    my $start = $today - (ONE_DAY * $days_to_process);
    $start_date = $start->ymd;
    $end_date = $today->ymd;
}

log_message("==========================================================");
log_message("BATCHED CALL METRICS UPDATE SCRIPT");
log_message("==========================================================");
log_message("Date range: $start_date to $end_date");
log_message("Batch size: $BATCH_SIZE users");
log_message("Sleep between batches: $SLEEP_BETWEEN_BATCHES seconds");
log_message("Sleep between dates: $SLEEP_BETWEEN_DATES seconds");
log_message("State file: $STATE_FILE");
log_message("Log file: $log_file");
log_message("==========================================================");

# Connect to database
log_message("Connecting to database...");
my $dbh = DBI->connect("DBI:mysql:$VARDB_database:$VARDB_server:$VARDB_port", $VARDB_user, $VARDB_pass, { RaiseError => 1, PrintError => 1 })
    or die "Couldn't connect to database: " . DBI->errstr;
log_message("Database connection established.");

# Check for existing state file to resume from
my $last_processed_date = '';
my %processed_users_by_date;

if ($RESUME_FROM_STATE && -f $STATE_FILE) {
    log_message("Found state file. Attempting to resume from previous run.");
    open(my $state_fh, '<', $STATE_FILE) or die "Cannot open state file: $!";
    my $state_data = <$state_fh>;
    close($state_fh);
    
    if ($state_data) {
        chomp($state_data);
        my ($saved_date, $saved_users) = split(/\|/, $state_data);
        
        if ($saved_date) {
            $last_processed_date = $saved_date;
            log_message("Resuming from date: $last_processed_date");
            
            if ($saved_users) {
                my @users = split(/,/, $saved_users);
                $processed_users_by_date{$saved_date} = { map { $_ => 1 } @users };
                log_message("Already processed " . scalar(@users) . " users for date $saved_date");
            }
        }
    }
}

# Get all daily records that need to be updated
log_message("Retrieving records to process...");
my $query_daily_records = "
    SELECT id, user, DATE(event_time) AS record_date
    FROM asterisk.iconic_backend_apd_all_tl_history
    WHERE report = 'D'
    AND DATE(event_time) BETWEEN ? AND ?
    ORDER BY event_time, user
";

my $sth_daily_records = $dbh->prepare($query_daily_records);
$sth_daily_records->execute($start_date, $end_date);

my %records_by_date;
my @all_records;

while (my $record = $sth_daily_records->fetchrow_hashref) {
    my $date = $record->{record_date};
    my $user = $record->{user};
    
    # Group records by date and user for efficient processing
    if (!exists $records_by_date{$date}) {
        $records_by_date{$date} = {};
    }
    
    $records_by_date{$date}->{$user} = $record;
    push @all_records, $record;
}
$sth_daily_records->finish();

if (!@all_records) {
    log_message("No daily records found in the specified date range.");
    $dbh->disconnect();
    exit;
}

log_message("Found " . scalar(@all_records) . " daily records across " . scalar(keys %records_by_date) . " dates");

# Prepare update statement
my $update_query = "
    UPDATE asterisk.iconic_backend_apd_all_tl_history
    SET 
        calls = ?,
        davg = ?,
        xfer = ?,
        xfrena = ?,
        total = ?,
        ratio = ?,
        talk_time = ?,
        wait_time = ?,
        pause_time = ?,
        dispo_time = ?,
        vici_table = ?,
        closer_table = ?
    WHERE 
        id = ?
";

my $sth_update = $dbh->prepare($update_query);

# Progress tracking
my $start_time = time();
my $total_updates = 0;
my $total_errors = 0;
my $total_dates = scalar(keys %records_by_date);
my $dates_processed = 0;

# Skip dates already fully processed
if ($last_processed_date) {
    my @dates = sort keys %records_by_date;
    my $skip_until_idx = 0;
    
    for (my $idx = 0; $idx < scalar(@dates); $idx++) {
        if ($dates[$idx] eq $last_processed_date) {
            $skip_until_idx = $idx;
            last;
        }
    }
    
    # Count dates we're skipping
    $dates_processed = $skip_until_idx;
    
    log_message("Skipping $dates_processed previously processed dates.");
}

# Process each date
foreach my $date (sort keys %records_by_date) {
    # Skip dates that come before the last processed date
    if ($last_processed_date && $date lt $last_processed_date) {
        log_message("Skipping already processed date: $date");
        next;
    }
    
    log_message("\nProcessing call metrics for date: $date (" . ($dates_processed + 1) . " of $total_dates)");
    
    my $users_for_date = $records_by_date{$date};
    my @users = sort keys %$users_for_date;
    
    log_message("Found " . scalar(@users) . " users for date $date");
    
    # Skip users already processed for the current date
    if (exists $processed_users_by_date{$date} && scalar(keys %{$processed_users_by_date{$date}}) > 0) {
        my $skipped_count = 0;
        my @remaining_users;
        
        foreach my $user (@users) {
            if (exists $processed_users_by_date{$date}->{$user}) {
                $skipped_count++;
            } else {
                push @remaining_users, $user;
            }
        }
        
        @users = @remaining_users;
        log_message("Skipping $skipped_count already processed users for date $date");
    }
    
    # Process users in batches
    my $batch_count = 0;
    my $total_batches = int((scalar(@users) + $BATCH_SIZE - 1) / $BATCH_SIZE);
    
    for (my $i = 0; $i < scalar(@users); $i += $BATCH_SIZE) {
        $batch_count++;
        my $end_idx = $i + $BATCH_SIZE - 1;
        $end_idx = $#users if $end_idx > $#users;
        
        my @batch_users = @users[$i..$end_idx];
        log_message("Processing batch $batch_count of $total_batches for date $date (" . scalar(@batch_users) . " users)");
        
        foreach my $user (@batch_users) {
            my $record = $users_for_date->{$user};
            my $record_id = $record->{id};
            
            log_message("  Processing user: $user (ID: $record_id)");
            
            # Get call metrics for this user on this date
            my $query_metrics = "
                SELECT 
                    COUNT(*) AS calls,
                    FORMAT(SUM(dispo_sec) / NULLIF(COUNT(*), 0), 2) AS disp_avg,
                    IFNULL((SELECT COUNT(*) FROM asterisk.vicidial_agent_log 
                           WHERE status='XFER' AND user=? 
                           AND DATE(event_time)=?
                           AND pause_sec<65000 AND wait_sec<65000 AND talk_sec<65000 AND dispo_sec<65000), 0) AS xfer,
                    IFNULL((SELECT COUNT(*) FROM asterisk.vicidial_agent_log 
                           WHERE status='XFRENA' AND user=? 
                           AND DATE(event_time)=?
                           AND pause_sec<65000 AND wait_sec<65000 AND talk_sec<65000 AND dispo_sec<65000), 0) AS xfrena,
                    SEC_TO_TIME(SUM(talk_sec)) AS talk_time,
                    SEC_TO_TIME(SUM(wait_sec)) AS wait_time,
                    SEC_TO_TIME(SUM(pause_sec)) AS pause_time,
                    SEC_TO_TIME(SUM(dispo_sec)) AS dispo_time
                FROM 
                    asterisk.vicidial_agent_log
                WHERE 
                    user=? AND DATE(event_time)=?
                    AND status IS NOT NULL
                    AND pause_sec<65000 AND wait_sec<65000 AND talk_sec<65000 AND dispo_sec<65000
            ";
            
            my $sth_metrics = $dbh->prepare($query_metrics);
            $sth_metrics->execute($user, $date, $user, $date, $user, $date);
            
            my $metrics = $sth_metrics->fetchrow_hashref;
            $sth_metrics->finish();
            
            # Vici table log - transfers not in agent_log
            my $query_vici_extra = "
                SELECT COUNT(*) FROM asterisk.vicidial_log WHERE 
                lead_id NOT IN (SELECT lead_id FROM asterisk.vicidial_agent_log WHERE lead_id IS NOT NULL AND status IN ('XFER','XFRENA') AND DATE(event_time)=? AND user=?) 
                AND uniqueid NOT IN (SELECT uniqueid FROM asterisk.vicidial_agent_log WHERE uniqueid IS NOT NULL AND status IN ('XFER','XFRENA') AND DATE(event_time)=? AND user=?) 
                AND status IN ('XFER','XFRENA') AND DATE(call_date)=? AND user=?
            ";
            
            my $sth_vici_extra = $dbh->prepare($query_vici_extra);
            $sth_vici_extra->execute($date, $user, $date, $user, $date, $user);
            
            my ($vici_extra) = $sth_vici_extra->fetchrow_array;
            $sth_vici_extra->finish();
            
            # Closer table log - transfers not in agent_log or vicidial_log
            my $query_closer_extra = "
                SELECT COUNT(*) FROM asterisk.vicidial_closer_log WHERE 
                lead_id NOT IN (SELECT lead_id FROM asterisk.vicidial_agent_log WHERE status IN ('XFER','XFRENA') AND DATE(event_time)=? AND user=?) 
                AND lead_id NOT IN (SELECT lead_id FROM asterisk.vicidial_log WHERE status IN ('XFER','XFRENA') AND DATE(call_date)=? AND user=?) 
                AND uniqueid NOT IN (SELECT uniqueid FROM asterisk.vicidial_agent_log WHERE status IN ('XFER','XFRENA') AND DATE(event_time)=? AND user=?)
                AND uniqueid NOT IN (SELECT uniqueid FROM asterisk.vicidial_log WHERE status IN ('XFER','XFRENA') AND DATE(call_date)=? AND user=?)
                AND status IN ('XFER','XFRENA') AND DATE(call_date)=? AND user=?
            ";
            
            my $sth_closer_extra = $dbh->prepare($query_closer_extra);
            $sth_closer_extra->execute($date, $user, $date, $user, $date, $user, $date, $user, $date, $user);
            
            my ($closer_extra) = $sth_closer_extra->fetchrow_array;
            $sth_closer_extra->finish();
            
            # Apply default values for missing metrics
            my $calls = $metrics->{calls} || 0;
            my $davg = $metrics->{disp_avg} || 0;
            my $xfer = $metrics->{xfer} || 0;
            my $xfrena = $metrics->{xfrena} || 0;
            my $total = $xfer + $xfrena;
            my $ratio = ($total > 0) ? sprintf("%.2f", $calls / $total) : 0;
            my $talk_time = $metrics->{talk_time} || '00:00:00';
            my $wait_time = $metrics->{wait_time} || '00:00:00';
            my $pause_time = $metrics->{pause_time} || '00:00:00';
            my $dispo_time = $metrics->{dispo_time} || '00:00:00';
            
            # Update the record
            eval {
                $sth_update->execute(
                    $calls,
                    $davg,
                    $xfer,
                    $xfrena,
                    $total,
                    $ratio,
                    $talk_time,
                    $wait_time,
                    $pause_time,
                    $dispo_time,
                    $vici_extra,
                    $closer_extra,
                    $record_id
                );
                
                $total_updates++;
                log_message("    Updated metrics for user $user on $date");
                
                # Mark this user as processed
                if (!exists $processed_users_by_date{$date}) {
                    $processed_users_by_date{$date} = {};
                }
                $processed_users_by_date{$date}->{$user} = 1;
                
                # Update state file after each user
                my @processed_users = keys %{$processed_users_by_date{$date}};
                open(my $state_fh, '>', $STATE_FILE) or die "Cannot open state file: $!";
                print $state_fh "$date|" . join(',', @processed_users);
                close($state_fh);
            };
            
            if ($@) {
                log_message("ERROR updating record ID $record_id for user $user: $@");
                $total_errors++;
            }
        }
        
        # Sleep between batches to reduce load
        if ($batch_count < $total_batches) {
            log_message("Sleeping for $SLEEP_BETWEEN_BATCHES seconds between batches...");
            sleep($SLEEP_BETWEEN_BATCHES);
        }
    }
    
    $dates_processed++;
    
    # Sleep between dates to reduce load
    if ($dates_processed < $total_dates) {
        log_message("Sleeping for $SLEEP_BETWEEN_DATES seconds between dates...");
        sleep($SLEEP_BETWEEN_DATES);
    }
}

# Calculate execution time
my $execution_time = time() - $start_time;
my $hours = int($execution_time / 3600);
my $minutes = int(($execution_time % 3600) / 60);
my $seconds = $execution_time % 60;
my $time_string = sprintf("%02d:%02d:%02d", $hours, $minutes, $seconds);

log_message("\n==========================================================");
log_message("EXECUTION SUMMARY");
log_message("==========================================================");
log_message("Total records processed: " . scalar(@all_records));
log_message("Successfully updated: $total_updates");
log_message("Errors encountered: $total_errors");
log_message("Total execution time: $time_string");
log_message("Log file: $log_file");
log_message("==========================================================");

# Clean up state file if processing is complete
if (-f $STATE_FILE && $total_updates + $total_errors >= scalar(@all_records)) {
    unlink($STATE_FILE);
    log_message("Processing complete. Removed state file.");
}

# Disconnect from database
$dbh->disconnect();
log_message("Database connection closed");
log_message("Script completed successfully");