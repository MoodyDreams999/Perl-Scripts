use strict;
use warnings;
use Spreadsheet::ParseXLSX;
use Excel::Writer::XLSX;
use POSIX qw(strftime);

# Configuration
my $infile_Path = "C:/Users/16025/OneDrive/Documents/Programming/Projects/DataParser";
my $in_file = "C:/Users/samue/OneDrive/Documents/Programming/Projects/DataParser/Source_APE_FHA_12_23_2024.xlsx";
my $out_file = "Source_APE_FHA_12_23_2024_1.xlsx";
my $log_file_dir = $ENV{'LOG_FILE_DIR'} || "C:/Users/16025/OneDrive/Documents/Programming/Perl/PerlList/Parse/log/";
my $config_file = $log_file_dir . 'file.txt';

# Headers
my $headers = [
    "source_id", "first_name", "middle", "last_name", "address1", "city", "state",
    "postal_code", "phone number", "address3", "province", "email", "Trusted_URL"
];

# State-Zip Code Mapping
my %state_zip = (
    AL => '35007', AK => '99501', AZ => '85304', AR => '71602', CA => '90005',
    CO => '80001', CT => '06001', DE => '19701', DC => '20012', FL => '32003',
    GA => '30002', HI => '96701', ID => '83203', IL => '61081', IN => '46011',
    IA => '50005', KS => '66008', KY => '40007', LA => '70001', ME => '04750',
    MD => '20601', MA => '05544', MI => '48706', MN => '54403', MS => '38601',
    MO => '64722', MT => '59001', NE => '68001', NV => '88905', NH => '03031',
    NJ => '07753', NC => '28376', NM => '87001', NY => '10028', ND => '58001',
    OH => '45434', OK => '73002', OR => '97009', PA => '15001', RI => '02823',
    SC => '29001', SD => '57002', TN => '37011', TX => '73344', TX2 => '79901',
    UT => '84002', VT => '05009', VA => '20101', WA => '98001', WV => '24712',
    WI => '54990', WY => '82002', PR => '00999', VI => '00851'
);

my %zip_code_ranges = (
    AL => [35000, 36999], AK => [99500, 99999], AZ => [85000, 86999], AR => [71600, 72999],
    CA => [90000, 96699], CO => [80000, 81999], CT => [6000, 6999], DE => [19700, 19999],
    FL => [32000, 34999], GA => [30000, 31999, 39800, 39999], HI => [96700, 96999],
    ID => [83200, 83999], IL => [60000, 62999], IN => [46000, 47999], IA => [50000, 52999],
    KS => [66000, 67999], KY => [40000, 42999], LA => [70000, 71599], ME => [3900, 4999],
    MD => [20600, 21999], MA => [1000, 2799, 5501, 5544], MI => [48000, 49999],
    MN => [55000, 56899], MS => [38600, 39999], MO => [63000, 65999], MT => [59000, 59999],
    NC => [27000, 28999], ND => [58000, 58999], NE => [68000, 69999], NV => [88900, 89999],
    NH => [3000, 3899], NJ => [7000, 8999], NM => [87000, 88499], NY => [10000, 14999, 6390, 501, 544],
    OH => [43000, 45999], OK => [73000, 74999], OR => [97000, 97999], PA => [15000, 19699],
    RI => [2800, 2999], SC => [29000, 29999], SD => [57000, 57999], TN => [37000, 38599],
    TX => [75000, 79999, 73301, 73399, 88500, 88599], UT => [84000, 84999], VT => [5000, 5999],
    VA => [20100, 20199, 22000, 24699, 20598], DC => [20000, 20099, 20200, 20599, 56900, 56999],
    WA => [98000, 99499], WV => [24700, 26999], WI => [53000, 54999], WY => [82000, 83199, 83414],
    PR => [600, 799, 900, 999], VI => [801, 851]
);


# Area Codes Mapping
my %state_area_codes = (
    AL => [qw(201 205 251 256 334)],
    AK => [qw(907)],
    AZ => [qw(480 520 602 623 928)],
    AR => [qw(479 501 870)],
    CA => [qw(209 213 279 310 323 408 415 424 442 510 559 562 619 626 628 650 657 661 669 707 714 747 760 805 818 820 831 858 909 916 925 949 951)],
    CO => [qw(303 719 720 970)],
    CT => [qw(203 475 860 959)],
    DC => [qw(202)],
    DE => [qw(302)],
    FL => [qw(239 305 321 352 386 407 561 727 754 772 786 813 850 863 904 941 954)],
    GA => [qw(229 404 470 478 678 706 762 770 912)],
    HI => [qw(808)],
    ID => [qw(208 986)],
    IL => [qw(217 224 309 312 331 618 630 708 773 779 815 847 872)],
    IN => [qw(219 260 317 463 574 765 812 930)],
    IA => [qw(319 515 563 641 712)],
    KS => [qw(316 620 785 913)],
    KY => [qw(270 364 502 606 859)],
    LA => [qw(225 318 337 504 985)],
    ME => [qw(207)],
    MD => [qw(240 301 410 443 667)],
    MA => [qw(339 351 413 508 617 774 781 857 978)],
    MI => [qw(231 248 269 313 517 586 616 734 810 906 947 989)],
    MN => [qw(218 320 507 612 651 763 952)],
    MS => [qw(228 601 662 769)],
    MO => [qw(314 417 573 636 660 816)],
    MT => [qw(406)],
    NE => [qw(308 402 531)],
    NV => [qw(702 725 775)],
    NH => [qw(603)],
    NJ => [qw(201 551 609 640 732 848 856 862 908 973)],
    NM => [qw(505 575)],
    NY => [qw(315 332 347 516 518 585 607 631 646 680 716 718 838 845 914 917 929 934)],
    NC => [qw(252 336 704 743 828 910 919 980 984)],
    ND => [qw(701)],
    OH => [qw(216 220 234 330 380 419 440 513 567 614 740 937)],
    OK => [qw(405 539 580 918)],
    OR => [qw(458 503 541 971)],
    PA => [qw(215 223 267 272 412 445 484 570 610 717 724 814 878)],
    PR => [qw(787 939)],
    RI => [qw(401)],
    SC => [qw(803 843 854 864)],
    SD => [qw(605)],
    TN => [qw(423 615 629 731 865 901 931)],
    TX => [qw(210 214 254 281 325 346 361 409 430 432 469 512 682 713 726 737 806 817 830 832 903 936 940 956 972 979)],
    UT => [qw(385 435 801)],
    VT => [qw(802)],
    VA => [qw(276 434 540 571 703 757 804)],
    WA => [qw(206 253 360 425 509 564)],
    WV => [qw(304 681)],
    WI => [qw(262 414 534 608 715 920)],
    WY => [qw(307)],
    VI => [qw(340)]
);

# Dynamic feature flag
my $feature_enabled = 1;
my ($address3_value, $province_value);

if ($feature_enabled) {
    if ($out_file =~ /Debt/i) {
        $address3_value = "Debt";
        $province_value = "Homeowner";
    } elsif ($out_file =~ /VA/i) {
        $address3_value = "VA";
        $province_value = "Veteran";
    } elsif ($out_file =~ /Mixed/i) {
        $address3_value = "MixedAddress3";
        $province_value = "MixedProvince";
    }
}

# Read in all rows from existing file
my $rows = read_excel($in_file);
my $row_number = 0;
# Update the rows
for my $row (@$rows) {
	$row_number++;
    next if $row_number == 1;  # Skip header row

    clean_phone_number($row);
    normalize_state($row);
    truncate_zip_code($row);
	
	 # 1. If ZIP is missing, populate it from state
    if (!$row->[7] && $row->[6]) {
        populate_zip_code($row);
    }

    # 2. If state is missing or invalid, populate it from ZIP code range
    if (!$row->[6] || !exists $state_zip{$row->[6]}) {
        populate_state_from_zip($row);
    }

    # 3. If both state and ZIP are still missing or invalid, populate using area code
    if ((!$row->[6] || length($row->[6]) != 2) && !$row->[7]) {
        populate_state_zip_from_area_code($row);
    }

    # Set address3 and province dynamically
    set_address_province($row);
}

# Add headers as first row
unshift(@$rows, $headers);

# Write the updated rows to new file
write_excel($out_file, $rows, scalar @$headers - 1);

# Update config file
update_config_file($config_file, $out_file, scalar @$rows, $infile_Path);

# Subroutines

# Read Excel file and return rows
sub read_excel {
    my ($file, $sheet) = @_;
    $sheet ||= 0;
    
    my $parser = Spreadsheet::ParseXLSX->new();
    my $workbook = $parser->parse($file);
    die $parser->error unless defined $workbook;
    
    my $worksheet = $workbook->worksheet($sheet);
    my ($row_min, $row_max) = $worksheet->row_range();
    my ($col_min, $col_max) = $worksheet->col_range();
    
    my @rows;
    for my $row ($row_min .. $row_max) {
        my @cells;
        for my $col ($col_min .. $col_max) {
            my $cell = $worksheet->get_cell($row, $col);
            push(@cells, $cell ? $cell->value() : '');
        }
        push(@rows, \@cells);
    }
    return \@rows;
}

# Write data to Excel file
sub write_excel {
    my ($file, $rows, $col_max) = @_;
    
    my $workbook = Excel::Writer::XLSX->new($file);
    die "Could not open file: $!" unless defined $workbook;
    
    my $worksheet = $workbook->add_worksheet();
    my $row_num = 0;
    
    for my $row (@$rows) {
        for my $col (0 .. $col_max) {
            $worksheet->write($row_num, $col, $row->[$col]);
        }
        $row_num++;
    }
    $workbook->close();
    print "$row_num lines processed";
}

# Clean phone number
sub clean_phone_number {
    my ($row) = @_;
    $row->[8] =~ s/[^0-9]//g;
    $row->[8] =~ s/^1// if length($row->[8]) == 11 && $row->[8] =~ /^1/;
}

# Normalize state to uppercase
sub normalize_state {
    my ($row) = @_;
    $row->[6] = uc($row->[6]);
}



# Truncate ZIP code if it exceeds 5 characters
sub truncate_zip_code {
    my ($row) = @_;
    if (length($row->[7]) > 5) {
        $row->[7] = substr($row->[7], 0, 5);
        print "Truncated ZIP code to 5 characters for row: $row->[0]\n";
    }
}


# Populate zip code if missing
sub populate_zip_code {
    my ($row) = @_;
    if ($row->[6] && !$row->[7]) {
        $row->[7] = $state_zip{$row->[6]};
    }
}

sub populate_state_from_zip {
    my ($row) = @_;
    my $zip = $row->[7];

    return unless defined $zip && $zip =~ /^\d+$/;  # Ensure the ZIP is numeric

    for my $state (keys %zip_code_ranges) {
        my @ranges = @{$zip_code_ranges{$state}};
        while (@ranges) {
            my $min = shift @ranges;
            my $max = shift @ranges;

            # Check if $max is defined before using it in the comparison
            if (defined $max && $zip >= $min && $zip <= $max) {
                $row->[6] = $state;
                return;
            }
        }
    }
}

# Populate state and zip from area code
sub populate_state_zip_from_area_code {
    my ($row) = @_;
    my $acode = substr($row->[8], 0, 3);
    
    for my $state (keys %state_area_codes) {
        if (grep { $_ eq $acode } @{$state_area_codes{$state}}) {
            $row->[6] = $state;
            $row->[7] = $state_zip{$state};
            last;
        }
    }
}

# Set address3 and province based on feature flag
sub set_address_province {
    my ($row) = @_;
    if ($feature_enabled) {
        $row->[9] = $address3_value if defined $address3_value && (!defined $row->[9] || $row->[9] eq '');
        $row->[10] = $province_value if defined $province_value && (!defined $row->[10] || $row->[10] eq '');
    }
}

# Update configuration file
sub update_config_file {
    my ($config_file, $out_file, $row_num, $infile_Path) = @_;
    open(my $fh, '>', $config_file) or die "Could not open file '$config_file' $!";
    print $fh "$out_file\n$row_num\n$infile_Path\n";
    close $fh;
}
