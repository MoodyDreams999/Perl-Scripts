#!/usr/bin/perl
use strict;
use warnings;
use Text::CSV;

my $csv = Text::CSV->new({ binary => 1, auto_diag => 1, eol => $/ });

# Open the input, output, and log files
open(my $input, '<', 'Phones_VA_20241017_numbers.csv') or die "Could not open input.csv: $!";
open(my $output, '>', 'output.csv') or die "Could not open output.csv: $!";
open(my $log, '>', 'log.csv') or die "Could not open log.csv: $!";

# Initialize counters and hash for duplicates
my $invalid_count = 0;
my $duplicate_count = 0;
my %seen_numbers;

# Write headers to the output and log
$csv->print($output, ["All", "Clean", "FDNC", "Invalid", "Cleaned_All", "Cleaned_FDNC"]);
$csv->print($log, ["Number", "Reason"]);

# Process each line from the input file
while (my $row = $csv->getline($input)) {
    # Remove spaces from all elements of the row
    @$row = map { $_ =~ s/\s+//gr } @$row;

    my ($all, $clean, $fdnc, $invalid) = @$row;

    # Function to clean and process numbers
    sub process_number {
        my ($number) = @_;
        my $reason = "";
        
        # Clean the number
        my $cleaned = $number =~ s/[^0-9]//gr;

        # If it's an 11-digit number starting with 1, remove the 1
        if (length($cleaned) == 11 && substr($cleaned, 0, 1) eq '1') {
            $cleaned = substr($cleaned, 1);
        }

        # If after cleaning, the length isn't 10 digits, set to empty and log reason
        #if (length($cleaned) != 10) {
           # $invalid_count++;
            #$reason = length($cleaned) > 10 ? "Too long" : "Too short";
          #  $csv->print($log, [$number, $reason]);
           # $cleaned = "";
        #}

        return $cleaned;
    }

    # Process the numbers in 'All' and 'FDNC' columns
    my $cleaned_all = process_number($all);
    my $cleaned_fdnc = process_number($fdnc);

    # Check for duplicates in the 'All' column
    if (exists $seen_numbers{$all}) {
        $duplicate_count++;
        $csv->print($log, [$all, "Duplicate"]);
    } else {
        $seen_numbers{$all} = 1;
    }

    # Write the cleaned numbers to the output file only if cleaned_all is not blank
    if ($cleaned_all ne '') {
        $csv->print($output, [$all, $clean, $fdnc, $invalid, $cleaned_all, $cleaned_fdnc]);
    }
}

# Print the totals to the log file
print $log "\nTotal Invalid Numbers: $invalid_count\n";
print $log "Total Duplicates in 'All' column: $duplicate_count\n";

# Close the files
close($input);
close($output);
close($log);

system("perl DNCExtractor.pl");
