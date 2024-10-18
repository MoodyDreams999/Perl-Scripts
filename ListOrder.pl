use strict;
use warnings;
use Spreadsheet::ParseXLSX;
use Text::CSV;
use List::Util 'shuffle';
use POSIX qw(strftime);
use File::Path qw(make_path);
use File::Spec;

print "Script started.\n";
my $config_file_path = 'C:/Users/16025/Downloads/Programming/Perl/PerlList/Parse/log/file.txt';
my $default_path = 'C:/Users/16025/OneDrive/Documents/Blacklist/20241014/APEVA_LIst 2.xlsx';
my ($in_file, $config_base_dir);

my $use_default_path = 1;

my $source_id_col_index = 0;
my $address3_col_index = 9;

if ($use_default_path == 1) {
    $in_file = $default_path;
    print "Using hardcoded input file path: $in_file\n";
} else {
    if (-e $config_file_path) {
        open(my $config_fh, '<', $config_file_path) or die "Could not open configuration file '$config_file_path': $!";
        while (my $line = <$config_fh>) {
            chomp($line);
            $in_file = $line unless defined $in_file;
            last;
        }
        close($config_fh);
        print "Configuration loaded. Input file set to: $in_file\n";
    } else {
        die "Configuration file does not exist.";
    }
}

my $csv = Text::CSV->new({
    binary => 1,
    auto_diag => 1,
    eol => "\n",
    sep_char => ',',
    escape_char => '"',
    quote_char => '"',
    always_quote => 1,
    empty_is_undef => 1
});

my $dir_choice;
do {
    print "Use the path from the config file (config_base_dir) or default to current directory? [config/default]: ";
    chomp($dir_choice = <STDIN>);
} until ($dir_choice eq 'config' || $dir_choice eq 'default');

my $base_dir = ($dir_choice eq 'config' && $config_base_dir) ? $config_base_dir : ".";

my $date_folder = 'Parse_' . strftime("%Y%m%d", localtime);
my $target_dir = File::Spec->catfile($base_dir, $date_folder);

unless (-d $target_dir) {
    make_path($target_dir) or die "Failed to create directory '$target_dir': $!";
    print "Created directory: $target_dir\n";
} else {
    print "Directory already exists: $target_dir\n";
}

my $parser = Spreadsheet::ParseXLSX->new;
my $workbook = $parser->parse($in_file) or die "Failed to open input file '$in_file': " . $parser->error;
my ($worksheet) = $workbook->worksheets();
my ($row_min, $row_max) = $worksheet->row_range();
my ($col_min, $col_max) = $worksheet->col_range();

my @headers = map {
    my $cell = $worksheet->get_cell($row_min, $_);
    defined $cell ? $cell->value() : '';
} $col_min .. $col_max;
#print "@headers\n";

my @data_rows = map {
    my $row = $_;
    [ map {
        my $cell = $worksheet->get_cell($row, $_);
        defined $cell ? $cell->value() : '';
    } $col_min .. $col_max ]
} $row_min+1 .. $row_max;

my $source_id_value = $data_rows[0][0] // 'output';

$source_id_value = $data_rows[2][$source_id_col_index] // 'output';
my $address3_value = $data_rows[2][$address3_col_index] // 'output';

my @matching_rows = grep { defined $_->[9] && ($_->[9] =~ /Debt|VA/i) } @data_rows;
if (@matching_rows) {
    $address3_value = $matching_rows[0]->[9];
}

my $current_date = strftime("%m_%d_%Y", localtime);

print "Enter '1' for default naming (output), '2' for parameter-based naming: ";
chomp(my $naming_choice = <STDIN>);

my $base_name = $naming_choice == 2 ? "Source_${source_id_value}_${address3_value}_${current_date}" : "output";
my $file_increment = 1;
my $rows_per_file = 25000;

if (@data_rows > $rows_per_file) {
    print "Total rows exceed 25,000. Please specify how many rows per file you'd like: ";
    chomp($rows_per_file = <STDIN>);
}

my @shuffled_data_rows = shuffle @data_rows;
#for my $row_ref (@shuffled_data_rows) {
 #   print(join(", ", @$row_ref), "\n");
#}

my $current_file_number = 1;
my $out_file_ext = '.csv';
my $fh;

for my $i (0 .. $#shuffled_data_rows) {
    if ($i % $rows_per_file == 0 || $i == 0) {
        close $fh if $fh;
        my $current_out_file = sprintf("%s_%d%s", $base_name, $file_increment++, $out_file_ext);
        $current_out_file = File::Spec->catfile($target_dir, $current_out_file);
        open($fh, '>:encoding(utf8)', $current_out_file) or die "Could not open file '$current_out_file': $!";
        $csv->print($fh, \@headers);
        print "New file created: $current_out_file\n";
    }
    $csv->print($fh, $shuffled_data_rows[$i]);
}
close $fh if $fh;

print "Data processing completed. Files created.\n";
