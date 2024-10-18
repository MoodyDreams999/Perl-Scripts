#!/usr/bin/perl
use strict;
use warnings;
use Text::CSV;

my $csv = Text::CSV->new({ binary => 1, auto_diag => 1, eol => $/ });

open(my $input, '<', 'output.csv') or die "Could not open output.csv: $!";
open(my $temp_output, '>', 'temp_output.csv') or die "Could not open temp_output.csv: $!";

my %combined_numbers;
my %invalid_numbers;

while (my $row = $csv->getline($input)) {
    next unless $row;

    my ($All, $Clean, $FDNC, $Invalid, $Cleaned_All, $Cleaned_FDNC) = @$row;

    # Store Clean and FDNC numbers in the hash
    $combined_numbers{$Clean} = 1 if $Clean;
    $combined_numbers{$FDNC} = 1 if $FDNC;
    
    # Store Invalid numbers in the invalid_numbers hash
    $invalid_numbers{$Invalid} = 1 if $Invalid;
}

seek($input, 0, 0);

my $header = $csv->getline($input);
push @$header, 'DNC';
$csv->print($temp_output, $header);

while (my $row = $csv->getline($input)) {
    next unless $row;

    my ($All, $Clean, $FDNC, $Invalid, $Cleaned_All, $Cleaned_FDNC) = @$row;
    my $DNC = '';

    # Check if All is not in the combined_numbers hash and not in the invalid_numbers hash
    if ($All && !exists $combined_numbers{$All} && !exists $invalid_numbers{$All}) {
        $DNC = $All;  # Mark as DNC
    }

    $csv->print($temp_output, [$All, $Clean, $FDNC, $Invalid, $Cleaned_All, $Cleaned_FDNC, $DNC]);
}

close($input);
close($temp_output);

rename("temp_output.csv", "output2.csv");
