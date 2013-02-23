
#===============================================================================
#
#         FILE: DBGEN_UTIL.pm
#
#  DESCRIPTION:  Collection of utility functions for TPC-C data generator.
#
# REQUIREMENTS:  ---
#
#       AUTHOR:  Naju Mancheril, Debabrata Dash, Ippokratis Pandis
#      COMPANY:  Carnegie Mellon University 
#      VERSION:  2.1
#      CREATED:  11/07/2006 12:10:32 PM EDT
#     REVISION:  $Id$
#===============================================================================


package DBGEN_UTIL;

use List::Util qw(shuffle);



# definitions of exported functions


# @brief Generate random integer within the range ['min', 'max'].

sub generate_random_integer_within {
    
    my $min = shift;
    my $max = shift;
    
    my $range = $max - $min + 1;
    
    return $min + int(rand($range));
}


# @brief This function generates a random alphanumeric string of the
# specified length. The specification does not define alphabet for
# "alphanumeric" strings. We generate strings using the letters 'a' to
# 'z', the letters 'A' to 'Z', and the numbers '0' to '9'.

# @param len The length of the randomly generated string.

sub generate_astring_fixed {

    my $len = shift;
    my @alphabet = ('a'..'z', 'A'..'Z', '0' .. '9');

    # We will use map() to select our random characters. Our mapping
    # function randomly selects a character in our alphabet. We do this
    # 'len' times. Finally, we use join() to combine these characters
    # into a single string.
    my $randstr = join '', map $alphabet[int(rand @alphabet)], 1..$len;
    return $randstr;
}


# @brief Generate a random alphanumeric string of at least 'min' and
# at most 'max' characters. See description of
# generate_astring_fixed() for definition of alphanumeric.

# @param min The minimum number of string characters.
# @param max The maximum number of string characters.

sub generate_astring {
    my $min_len = shift;
    my $max_len = shift;
    my $len = generate_random_integer_within($min_len, $max_len);
    return generate_astring_fixed($len);
}


# @brief This function generates a random numeric string of the
# specified length. We use the digits '0' to '9'.

# @param len The length of the randomly generated string.

sub generate_nstring_fixed {
    
    my $len = shift;
    my @alphabet = ('0' .. '9');

    # We will use map() to select our random characters. Our mapping
    # function randomly selects a character in our alphabet. We do this
    # 'len' times. Finally, we use join() to combine these characters
    # into a single string.
    my $randstr = join '', map $alphabet[int(rand @alphabet)], 1..$len;
    return $randstr;
}


# @brief Generate a random numeric string of at least 'min' and at
# most 'max' characters. See description of generate_nstring_fixed()
# for definition of numeric.

# @param min The minimum number of string characters.
# @param max The maximum number of string characters.

sub generate_nstring {
    my $min_len = shift;
    my $max_len = shift;
    my $len = generate_random_integer_within($min_len, $max_len);
    return generate_nstring_fixed($len);
}


# @brief Generate a valid last name (for C_LAST attribute).

# @param selection A three-digit number which specifies the syllables
# to concatenate together to generate the last name. For example, 123
# would correspond to "BAR"."OUGHT"."ABLE".

sub generate_last_name {

    my @SYLLABLES =
        ("BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING");
    
    my $selection = shift;
    my $s1 = ($selection / 1)   % 10;
    my $s2 = ($selection / 10)  % 10;
    my $s3 = ($selection / 100) % 10;
    
    return "$SYLLABLES[$s1]$SYLLABLES[$s2]$SYLLABLES[$s3]";
}


# @brief Generate a list of 'num_ints' unique integers selected from
# the range ['min', 'max']. This function can be used to select unique
# fractional numbers as well. To select X numbers in [0.01, 100.00],
# select X integers in [1, 10000] and then divide each entry of the
# result by 100.0.

# @param num_ints The number of integers to return.
# @param min The minimum number we can return.
# @param max The maximum number we can return.

sub generate_unique_integers {

    my $num_ints = shift;
    my $min = shift;
    my $max = shift;

    my @domain = shuffle( $min .. $max ); 
    $#domain = $num_ints - 1;              # truncate array to 'num_ints' entries

    return @domain;
}


# @brief Generate a random permutation of the numbers in ['min', 'max'].

# @param min The minimum number in the permutation.
# @param max The maximum number of the permutation.

sub generate_permutation {

    my $min = shift;
    my $max = shift;
 
    my $num_ints = $max - $min + 1;
   
    return generate_unique_integers($num_ints, $min, $max);
}


# @brief Generate a spec-compliant ZIP code.

sub generate_zip {

    # A zip code is a random n-string of 4 digits concatenated with
    # the fixed string "11111".
    my $zip = generate_nstring_fixed(4);
    return $zip . "11111";
}


# @brief Generate output file name using 'basename'. Current
# implementation simply appends ".tbl" to the end of 'basename'.

# @param basename The base of the filename.

sub generate_output_file_path {

    my $output_dir = shift;
    my $filename   = shift;
    my $path       = "$output_dir/$filename"; # output file

    printf("Creating $path ...\n");

    return $path;
}


# @brief Generate output file name using 'basename'. Current
# implementation simply appends ".tbl" to the end of 'basename'.

# @param basename The base of the filename.

sub generate_table_file_path {

    my $output_dir = shift;
    my $tablename  = shift;
    return generate_output_file_path($output_dir, "$tablename.tbl");
}


# @brief TPC-C non-uniform random number distributor.

sub NURand {

    my $C = shift;
    my $A = shift;
    my $x = shift;
    my $y = shift;
    
    my $rand_1 = generate_random_integer_within( 0, $A);
    my $rand_2 = generate_random_integer_within($x, $y);
    my $rand_or = $rand_1 | $rand_2;
    
    return (($rand_or + $C) % ($y - $x + 1)) + $x;
}


sub get_current_datetime {

    my ($second,
        $minute,
        $hour,
        $day_of_month,
        $month,
        $year_offset,
        $day_of_week,
        $day_of_year,
        $daylight_savings) = localtime();
    
    $year = 1900 + $year_offset;
    return sprintf("$year-%02d-%02d %02d:%02d:%02d",
                   $month,
                   $day_of_month,
                   $hour,
                   $minute,
                   $second);
}


1;
