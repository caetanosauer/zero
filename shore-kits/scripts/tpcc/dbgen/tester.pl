#!/usr/bin/perl -w

use strict;
use DBGEN_UTIL;


sub test_nstring {
    foreach ( 1 .. 10 )
    {
        my $nstring = DBGEN_UTIL::generate_nstring_fixed(4);
        print "$nstring\n";
    }
}


sub test_last_name {
    foreach ( 1 .. 10 )
    {
        my $name = DBGEN_UTIL::generate_last_name();
        print "$name\n";
    }
}


sub test_generate_unique_integers {
    my @array = DBGEN_UTIL::generate_unique_integers(4, 1, 9);
    print "@array\n";
}


sub test_defined {

    my @array = ();
    $array[1] = 1;
    $array[4] = 1;
    $array[7] = 1;
    $array[8] = 1;
    foreach ( 1 .. 10 ) {
        if ( defined($array[$_]) )
        {
            print "1";
        }
        else
        {
            print "0";
        }
    }
    print "\n";
}


my $attribute_separator = ",";


# The I_ID attribute is "unique within [100,000]. We will simply let
# the item IDs be the numbers 1 .. 100,000.

my $NUM_UNIQUE_ITEMS = 100000;
my $NUM_OITEMS = $NUM_UNIQUE_ITEMS / 10;
my @oitems =
    DBGEN_UTIL::generate_unique_integers($NUM_OITEMS, 1, $NUM_UNIQUE_ITEMS);
my @oitems_bitmap = ();
foreach ( @oitems ) {
    $oitems_bitmap[$_] = 1;
}

foreach ( 1 .. $NUM_UNIQUE_ITEMS )
{
    my $i_id    = $_;
    my $i_im_id = DBGEN_UTIL::generate_random_integer_within(1, 10000);
    my $i_name  = DBGEN_UTIL::generate_astring(14, 24);
    my $i_price = sprintf("%.2f",
                          DBGEN_UTIL::generate_random_integer_within(100, 10000) / 100.0);
    
    my $i_data;
    if ( defined($oitems_bitmap[$i_id]) )
    {

        # Generate I_DATA string containing "ORIGINAL". I_DATA should
        # contain between 26 and 50 characters. We already know 8 of
        # those characters. We must decide how many characters come
        # before "ORIGINAL" and how many come after.
        my $num_chars = DBGEN_UTIL::generate_random_integer_within(26, 50);
        my $num_remaining_chars = $num_chars - 8;
                
        # We will generate two strings of length X and Y such that X+Y
        # equals 'num_remaining_chars'. X can vary between 0 and
        # 'num_remaining_chars'.
        my $X = int(rand($num_remaining_chars+1));
        my $Y = $num_remaining_chars - $X;
        
        $i_data =
              DBGEN_UTIL::generate_astring_fixed($X)
            . "ORIGINAL"
            . DBGEN_UTIL::generate_astring_fixed($Y);
    }
    else {
        
        # The spec does not say we need to exclude strings contains
        # "ORIGINAL" for other 90 percent.
        $i_data = DBGEN_UTIL::generate_astring(26, 50);
    }

    my @row = ($i_id, $i_im_id, $i_name, $i_price, $i_data);
    
    print join($attribute_separator, @row), "\n";
}

