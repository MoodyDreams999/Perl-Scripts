use strict;
use warnings;
use Text::CSV;
use List::Util 'sum';

# Function to count total duplicates in a file
sub count_duplicates {
    my ($file, $column) = @_;
    my %counts;

    my $csv = Text::CSV->new({ binary => 1 });
    open my $fh, "<:encoding(UTF-8)", $file or die "Could not open '$file': $!";

    while (my $row = $csv->getline($fh)) {
        my $value = $row->[$column];
        
        # Remove BOM from the first line if present
        if ($. == 1) {
            $value =~ s/^\x{FEFF}//;
        }
        
        next unless defined $value;  # Skip undefined values
        $counts{$value}++;
    }

    close $fh;

    # Calculate total duplicates in the file
    my $total_duplicates = 0;
    foreach my $key (keys %counts) {
        if ($counts{$key} > 1) {
            $total_duplicates += ($counts{$key} - 1);  # Count duplicates for each number
        }
    }

    return ($total_duplicates, \%counts);
}

# Function to compare numbers from file1 to file2 and count matches
sub compare_files {
    my ($counts_file1, $file2, $column) = @_;
    my %matches;

    my $csv = Text::CSV->new({ binary => 1 });
    open my $fh, "<:encoding(UTF-8)", $file2 or die "Could not open '$file2': $!";

    while (my $row = $csv->getline($fh)) {
        my $value = $row->[$column];

        # Remove BOM from the first line of the second file if present
        if ($. == 1) {
            $value =~ s/^\x{FEFF}//;
        }

        next unless defined $value;  # Skip undefined values
        if (exists $counts_file1->{$value}) {
            $matches{$value}++;
        }
    }

    close $fh;

    # Return count of matches
    my $total_in_both = sum(values %matches);
    return $total_in_both;
}

# Main script
my $file1 = "file1.csv";  # First CSV file
my $file2 = "file2.csv";  # Second CSV file
my $column_index = 0;     # Column index for phone numbers (0-based)

# Step 1: Count duplicates in the first file
print "Counting duplicates in $file1...\n";
my ($total_duplicates_file1, $counts_file1) = count_duplicates($file1, $column_index);
print "Total duplicates in $file1: $total_duplicates_file1\n";

# Step 2: Count duplicates in the second file
print "Counting duplicates in $file2...\n";
my ($total_duplicates_file2, $counts_file2) = count_duplicates($file2, $column_index);
print "Total duplicates in $file2: $total_duplicates_file2\n";

# Step 3: Compare and count numbers that appear in both files
print "Comparing numbers from $file1 and $file2...\n";
my $total_in_both = compare_files($counts_file1, $file2, $column_index);
print "Total numbers from $file1 that are in $file2: $total_in_both\n";
