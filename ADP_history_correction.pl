#!/usr/bin/perl
use strict;
use warnings;
use DBI;
use Time::Piece;
use Time::Seconds;
use Spreadsheet::XLSX;
use Text::CSV;
use POSIX qw(strftime);

# default path to astguiclient configuration file:
my $PATHconf = '/etc/astguiclient.conf';

# DB connection variables
my ($VARDB_server, $VARDB_database, $VARDB_user, $VARDB_pass, $VARDB_port);
my ($PATHlogs, $PATHsounds, $VARserver_ip);
my ($VARDB_custom_user, $VARDB_custom_pass);

# Read configuration
open(my $conf_fh, "<", $PATHconf) || die "Can't open $PATHconf: $!\n";
while (my $line = <$conf_fh>) {
    $line =~ s/ |>|\n|\r|\t|\#.*|;.*//gi;
    if ($line =~ /^PATHlogs/) {$PATHlogs = $line; $PATHlogs =~ s/.*=//gi;}
    if ($line =~ /^PATHsounds/) {$PATHsounds = $line; $PATHsounds =~ s/.*=//gi;}
    if ($line =~ /^VARserver_ip/) {$VARserver_ip = $line; $VARserver_ip =~ s/.*=//gi;}
    if ($line =~ /^VARDB_server/) {$VARDB_server = $line; $VARDB_server =~ s/.*=//gi;}
    if ($line =~ /^VARDB_database/) {$VARDB_database = $line; $VARDB_database =~ s/.*=//gi;}
    if ($line =~ /^VARDB_user/) {$VARDB_user = $line; $VARDB_user =~ s/.*=//gi;}
    if ($line =~ /^VARDB_pass/) {$VARDB_pass = $line; $VARDB_pass =~ s/.*=//gi;}
    if ($line =~ /^VARDB_custom_user/) {$VARDB_custom_user = $line; $VARDB_custom_user =~ s/.*=//gi;}
    if ($line =~ /^VARDB_custom_pass/) {$VARDB_custom_pass = $line; $VARDB_custom_pass =~ s/.*=//gi;}
    if ($line =~ /^VARDB_port/) {$VARDB_port = $line; $VARDB_port =~ s/.*=//gi;}
}
close($conf_fh);

if (!$VARDB_port) {$VARDB_port='3306';}

# Connect to database
my $dbh = DBI->connect("DBI:mysql:$VARDB_database:$VARDB_server:$VARDB_port", 
                       $VARDB_user, $VARDB_pass, 
                       { RaiseError => 1, PrintError => 0 })
    or die "Couldn't connect to database: " . DBI->errstr;

print "Connected to database successfully.\n";

# File path - update with your actual XLSX file path
my $file_path = "APDCK.xlsx";

print "Attempting to process file: $file_path\n";

# Function to generate username from full name
sub generate_username {
    my ($fullname) = @_;
    
    # Parse the name (assuming format "LastName, FirstName")
    my ($last, $first) = split(/,\s*/, $fullname);
    
    if (!$first) {
        # If no comma, assume "FirstName LastName" format
        ($first, $last) = split(/\s+/, $fullname, 2);
    }
    
    if (!$first || !$last) {
        return lc($fullname); # Fallback if parsing fails
    }
    
    # Create username (first letter of first name + last name)
    my $username = lc(substr($first, 0, 1) . $last);
    $username =~ s/\s+//g; # Remove any spaces
    
    return $username;
}
my $count = 1782;
# Function to process Excel file (XLSX)
sub process_xlsx {
    my ($file) = @_;
    
    my @data;
    eval {
        my $excel = Spreadsheet::XLSX->new($file);
        my $sheet = $excel->{Worksheet}[0]; # Assuming data is in the first sheet
        
        my ($row_min, $row_max) = $sheet->row_range();
        my ($col_min, $col_max) = $sheet->col_range();
        
        # Skip header row
        for my $row ($row_min + 1 .. $row_max) {
            my %row_data;
            
            $row_data{name} = $sheet->{Cells}[$row][1]->{Val} || '';   # Column B: Name
			$row_data{id} = $count;
            # Generate username from agent name
            $row_data{user} = generate_username($row_data{name});
            
            $row_data{ugroup} = $sheet->{Cells}[$row][0]->{Val} || ''; # Column A: Group
            $row_data{calls} = $sheet->{Cells}[$row][2]->{Val} || 0;   # Column C: Calls
            $row_data{davg} = $sheet->{Cells}[$row][3]->{Val} || 0;    # Column D: DISPAVG
            $row_data{xfer} = $sheet->{Cells}[$row][4]->{Val} || 0;    # Column E: XFER
            $row_data{xfrena} = $sheet->{Cells}[$row][5]->{Val} || 0;  # Column F: XFRENA
            $row_data{total} = $sheet->{Cells}[$row][6]->{Val} || 0;   # Column G: TOTALS
            $row_data{ratio} = $sheet->{Cells}[$row][7]->{Val} || 0;   # Column H: RATIO
            $row_data{talk_time} = $sheet->{Cells}[$row][8]->{Val} || '00:00:00';  # Column I: Talk Time
            $row_data{pause_time} = $sheet->{Cells}[$row][9]->{Val} || '00:00:00'; # Column J: Pause Time
            $row_data{wait_time} = $sheet->{Cells}[$row][10]->{Val} || '00:00:00'; # Column K: Wait Time
            $row_data{dispo_time} = $sheet->{Cells}[$row][11]->{Val} || '00:00:00'; # Column L: Dispo Time
            $row_data{report} = 'D';  # Default value
            $row_data{event_time} = '2025-03-12 00:00:00'; # Set a fixed date
            $row_data{weekday} = 'Wed';    # Set weekday for Monday
            
            # Table ID from column AA if available
            $row_data{vici_table} = $sheet->{Cells}[$row][26]->{Val} || 0;   # Column AA: Table ID
            $row_data{closer_table} = 0; # Default value
            $row_data{TC} = 0;           # Default value
            $count++;
            
            push @data, \%row_data;
        }
    };
    
    if ($@) {
        die "Error processing Excel file: $@\n";
    }
    
    return \@data;
}

# Function to process CSV file as fallback
sub process_csv {
    my ($file) = @_;
    
    my @data;
    open my $fh, "<:encoding(utf8)", $file or die "Could not open '$file': $!";
    
    my $csv = Text::CSV->new({binary => 1, auto_diag => 1});
    
    # Read header row
    my $headers = $csv->getline($fh);
    $csv->column_names($headers);
    
    while (my $row = $csv->getline_hr($fh)) {
        # Get name first to generate username
        my $name = $row->{Name} || '';
        
        # Ensure required fields exist
        $row->{user} = generate_username($name);
        $row->{ugroup} = $row->{Group} || '';
        $row->{name} = $name;
        $row->{calls} = $row->{Calls} || 0;
        $row->{davg} = $row->{DISPAVG} || 0;
        $row->{xfer} = $row->{XFER} || 0;
        $row->{xfrena} = $row->{XFRENA} || 0;
        $row->{total} = $row->{TOTALS} || 0;
        $row->{ratio} = $row->{RATIO} || 0;
        $row->{talk_time} = $row->{'Talk Time'} || '00:00:00';
        $row->{pause_time} = $row->{'Pause Time'} || '00:00:00';
        $row->{wait_time} = $row->{'Wait Time'} || '00:00:00';
        $row->{dispo_time} = $row->{'Dispo Time'} || '00:00:00';
        $row->{report} = 'D';
        $row->{event_time} = '2025-03-12 00:00:00';
        $row->{weekday} = 'Wed';
        $row->{vici_table} = $row->{'Table ID'} || 0;
        $row->{closer_table} = 0;
        $row->{TC} = 0;
       
        
        push @data, $row;
    }
    
    close $fh;
    return \@data;
}

# Try to process as XLSX first, fallback to CSV
my $data;
eval {
    $data = process_xlsx($file_path);
};
if ($@) {
    print "Failed to process as XLSX, trying CSV format...\n";
    eval {
        $data = process_csv($file_path);
    };
    if ($@) {
        die "Failed to process file in both XLSX and CSV formats: $@\n";
    }
}

# Print generated usernames for preview
print "\nGenerated usernames:\n";
for my $i (0..min(9, $#{$data})) {  # Show first 10 entries
    print "$data->[$i]->{name} -> $data->[$i]->{user}\n";
}
print "...\n" if @$data > 10;

# Ask for confirmation
print "\nDo you want to proceed with inserting these records? (y/n): ";
my $confirm = <STDIN>;
chomp($confirm);
if (lc($confirm) ne 'y') {
    print "Operation cancelled.\n";
    exit;
}

# Prepare SQL statement for inserting data
my $query_insert = "INSERT INTO asterisk.iconic_backend_apd_all_tl_history 
                    (`id`,`user`, `ugroup`, `name`, `calls`, `davg`, `xfer`, `xfrena`, `total`, `ratio`, 
                     `talk_time`, `wait_time`, `pause_time`, `dispo_time`, `report`, `event_time`, 
                     `weekday`, `vici_table`, `closer_table`, `TC`)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

my $sth_insert = $dbh->prepare($query_insert);

# Helper function for min
sub min {
    my ($a, $b) = @_;
    return $a < $b ? $a : $b;
}

# Insert each record into the database
my $insert_count = 0;
foreach my $row (@$data) {
    eval {
        $sth_insert->execute(
			$row->{$count},
            $row->{user},
            $row->{ugroup},
            $row->{name},
            $row->{calls},
            $row->{davg},
            $row->{xfer},
            $row->{xfrena},
            $row->{total},
            $row->{ratio},
            $row->{talk_time},
            $row->{wait_time},
            $row->{pause_time},
            $row->{dispo_time},
            $row->{report},
            $row->{event_time},
            $row->{weekday},
            $row->{vici_table},
            $row->{closer_table},
            $row->{TC},
        );
        $insert_count++;
    };
    if ($@) {
        warn "Error inserting record for $row->{name}: $@\n";
    }
}

print "Successfully inserted $insert_count records into iconic_backend_apd_all_tl_history.\n";

# Disconnect from database
$dbh->disconnect();
print "Database connection closed.\n";