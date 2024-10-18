#!/usr/bin/perl
use strict;
use warnings;
use Text::CSV;

my $csv = Text::CSV->new({ binary => 1, auto_diag => 1, eol => $/ });

open(my $input, '<', 'output2.csv') or die "Could not open output2.csv: $!";
open(my $output, '>', 'output_final.csv') or die "Could not open output_final.csv: $!";
open(my $log, '>', 'duplicates_removed_log.csv') or die "Could not open duplicates_removed_log.csv: $!";

$csv->print($log, ["Column", "DuplicateNumber", "Occurrences"]);

# Hashes to store seen numbers for each column
my %seen_all;
my %seen_clean;
my %seen_fdnc;
my %seen_invalid;
my %seen_cleaned_all;
my %seen_cleaned_fdnc;

while (my $row = $csv->getline($input)) {
    my ($all, $clean, $fdnc, $invalid, $cleaned_all, $cleaned_fdnc, $dnc) = @$row;

    # Check each column and update the corresponding hash
    $seen_all{$all}++;
    $seen_clean{$clean}++;
    $seen_fdnc{$fdnc}++;
    $seen_invalid{$invalid}++;
    $seen_cleaned_all{$cleaned_all}++;
    $seen_cleaned_fdnc{$cleaned_fdnc}++;
}

# Log duplicates from each column
sub log_duplicates {
    my ($hash_ref, $column_name) = @_;
    while (my ($key, $value) = each %$hash_ref) {
        if ($value > 1 && $key ne '') {
            $csv->print($log, [$column_name, $key, $value]);
        }
    }
}

log_duplicates(\%seen_all, "All");
log_duplicates(\%seen_clean, "Clean");
log_duplicates(\%seen_fdnc, "FDNC");
log_duplicates(\%seen_invalid, "Invalid");
log_duplicates(\%seen_cleaned_all, "Cleaned_All");
log_duplicates(\%seen_cleaned_fdnc, "Cleaned_FDNC");

close($log);
close($input);
close($output);
