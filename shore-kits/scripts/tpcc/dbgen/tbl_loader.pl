#!/usr/bin/perl -w

#===============================================================================
#
#         FILE:  tbl_loader.pl
#
#        USAGE:  ./tbl_loader.pl  [OPTIONS]
#
#  DESCRIPTION:  TPC-C data generator.
#
#      OPTIONS:  -w Specify the number of warehouses (TPC-C scale factor).
#                   May be a rational number. The default value is
#                   DEFAULT_NUM_WAREHOUSES.
#
#                -s Specify the separator to use when printing data. The
#                   default value is DEFAULT_ATTRIBUTE_SEPARATOR.
#
#                -d Output directory. Default value is DEFAULT_OUTPUT_DIRECTORY.
#
# REQUIREMENTS:  DBGEN_UTIL.pm
#
#       AUTHOR:  Naju Mancheril, Debabrata Dash, Ippokratis Pandis
#      COMPANY:  Carnegie Mellon University
#      VERSION:  2.1
#      CREATED:  11/07/2006 12:10:32 PM EDT
#     REVISION:  $Id$
#===============================================================================

use strict;
use Getopt::Std;
#use DateTime::Event::Random;
use POSIX;

use lib ".";
use DBGEN_UTIL;

use lib "../";
use DB_CONFIG;



# contants
my $A_C_LAST  = 255;
my $A_C_ID    = 1023;
my $A_OL_I_ID = 8191;



# module variables
my $warehouse_count;
my $attribute_separator;
my $output_dir;

my $C_C_LAST;
my $C_C_ID;
my $C_OL_I_ID;
my $CUSTOMER_TABLE_POPULATED_TIME;




# Print something and output it also to the README file
sub print_readme_and_echo {
    my $output = shift;
    print README $output;
    print $output;
}


# fn: Print usage and exit
sub print_usage_exit {

    print "USAGE:    $0 -w WH_COUNT -s SEP -d OUT_DIR\n";
    print "OPTIONS:  -w WH_COUNT - Specify the number of warehouses (TPC-C scale factor)\n";
    print "          -s SEP      - Specify the separator to use when printing data\n";
    print "          -d OUT_DIR  - Specify the output directory\n";

    exit();
}



# fn: Parse command-line options
my %options;
getopts("chw:s:d:", \%options);


# checks if user asked for help
print_usage_exit() if $options{h};

# uses rest options
$warehouse_count     = $options{w} ? $options{w} : $DB_CONFIG::DEFAULT_NUM_WAREHOUSES;
$attribute_separator = $options{s} ? $options{s} : $DB_CONFIG::DEFAULT_ATTRIBUTE_SEPARATOR;
$output_dir          = $options{d} ? $options{d} : $DB_CONFIG::DEFAULT_OUTPUT_DIRECTORY;


open(README,           ">".DBGEN_UTIL::generate_output_file_path($output_dir, "README"));
open(ITEM_TABLE,       ">".DBGEN_UTIL::generate_table_file_path($output_dir, "item"));
open(WAREHOUSE_TABLE,  ">".DBGEN_UTIL::generate_table_file_path($output_dir, "warehouse"));
open(STOCK_TABLE,      ">".DBGEN_UTIL::generate_table_file_path($output_dir, "stock"));
open(DISTRICT_TABLE,   ">".DBGEN_UTIL::generate_table_file_path($output_dir, "district"));
open(CUSTOMER_TABLE,   ">".DBGEN_UTIL::generate_table_file_path($output_dir, "customer"));
open(HISTORY_TABLE,    ">".DBGEN_UTIL::generate_table_file_path($output_dir, "history"));
open(ORDER_TABLE,      ">".DBGEN_UTIL::generate_table_file_path($output_dir, "order"));
open(ORDER_LINE_TABLE, ">".DBGEN_UTIL::generate_table_file_path($output_dir, "order_line"));
open(NEW_ORDER_TABLE,  ">".DBGEN_UTIL::generate_table_file_path($output_dir, "new_order"));


# fn: Print configuration
sub print_config {
    print_readme_and_echo "$0 Configuration:\n";
    print_readme_and_echo "Options: WH_COUNT = $warehouse_count\n";
    print_readme_and_echo "Options: SEP      = $attribute_separator\n";
    print_readme_and_echo "Options: OUT_DIR  = $output_dir\n";
}

# fn: Print configuration and exit
sub print_config_exit {
    print_config();
    exit();
}


# Check user option to print config and exit 
print_config_exit() if $options{c};

# Print current configuration
print_config();
 
print "\nGenerating dbgen_constants...\n";
generate_dbgen_constants();

print "\nGenerating item_data...\n";
generate_item_data();

print "\nGenerating warehouse_data...\n";
generate_warehouse_data();



close(NEW_ORDER_TABLE);
close(ORDER_LINE_TABLE);
close(ORDER_TABLE);
close(HISTORY_TABLE);
close(CUSTOMER_TABLE);
close(DISTRICT_TABLE);
close(STOCK_TABLE);
close(WAREHOUSE_TABLE);
close(ITEM_TABLE);
close(README);



sub generate_dbgen_constants {

    $C_C_LAST  = DBGEN_UTIL::generate_random_integer_within(0, $A_C_LAST);
    $C_C_ID    = DBGEN_UTIL::generate_random_integer_within(0, $A_C_ID);
    $C_OL_I_ID = DBGEN_UTIL::generate_random_integer_within(0, $A_OL_I_ID);

    $CUSTOMER_TABLE_POPULATED_TIME =
        DBGEN_UTIL::get_current_datetime();
    
    print_readme_and_echo "C for C_LAST:     $C_C_LAST\n";
    print_readme_and_echo "C for C_ID:       $C_C_ID\n";
    print_readme_and_echo "C for OL_I_ID:    $C_OL_I_ID\n";
    print_readme_and_echo "Customer created: $CUSTOMER_TABLE_POPULATED_TIME\n";
}




sub generate_item_data {
    
    # The I_ID attribute is "unique within [100,000]. We will simply let
    # the item IDs be the numbers 1 .. 100,000.

    my $NUM_UNIQUE_ITEMS = 100000;
    my $NUM_OITEM_TUPLES = $NUM_UNIQUE_ITEMS / 10;


    my @oitems =
        DBGEN_UTIL::generate_unique_integers($NUM_OITEM_TUPLES, 1, $NUM_UNIQUE_ITEMS);
    my @oitems_bitmap = ();
    foreach ( @oitems ) {
        $oitems_bitmap[$_] = 1;
    }
    

    foreach ( 1 .. $NUM_UNIQUE_ITEMS )
    {
        print "i";

        my $i_id    = $_;
        my $i_im_id = DBGEN_UTIL::generate_random_integer_within(1, 10000);
        my $i_name  = DBGEN_UTIL::generate_astring(14, 24);
        my $i_price =
            sprintf("%.2f",
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
        else
        {
            # The spec does not say we need to exclude strings contains
            # "ORIGINAL" for other 90 percent.
            $i_data = DBGEN_UTIL::generate_astring(26, 50);
        }
        

        my @row = ($i_id,
                   $i_im_id,
                   $i_name,
                   $i_price,
                   $i_data);
        
        print ITEM_TABLE join($attribute_separator, @row), "\n";
    }
}




sub generate_warehouse_data {
    

    my $W_YTD = "300000.00";


    foreach (1 .. $warehouse_count) {

        print "w";
        
        my $w_id = $_;
        my $w_name     = DBGEN_UTIL::generate_astring(6, 10);
        my $w_street_1 = DBGEN_UTIL::generate_astring(10, 20);
        my $w_street_2 = DBGEN_UTIL::generate_astring(10, 20);
        my $w_city     = DBGEN_UTIL::generate_astring(10, 20);
        my $w_state    = DBGEN_UTIL::generate_astring_fixed(2);
        my $w_zip      = DBGEN_UTIL::generate_zip();

        my $w_tax =
            sprintf("%.4f",
                    DBGEN_UTIL::generate_random_integer_within(0, 2000) / 10000.0);
        my $w_ytd = $W_YTD;


        my @row = ($w_id,
                   $w_name,
                   $w_street_1,
                   $w_street_2,
                   $w_city,
                   $w_state,
                   $w_zip,
                   $w_tax,
                   $w_ytd);
        
        print WAREHOUSE_TABLE join($attribute_separator, @row), "\n";

        generate_stock_rows($w_id);
        generate_district_rows($w_id);
    }
}




sub generate_stock_rows {
    
    my $S_YTD        = 0;
    my $S_ORDER_CNT  = 0;
    my $S_REMOTE_CNT = 0;
    
    my $NUM_STOCK_ROWS_PER_CALL = 100000;
    my $NUM_OSTOCK_TUPLES = $NUM_STOCK_ROWS_PER_CALL / 10;

    
    my $w_id = shift;
    

    my @ostocks =
        DBGEN_UTIL::generate_unique_integers
        ($NUM_OSTOCK_TUPLES, 1, $NUM_STOCK_ROWS_PER_CALL);
    my @ostocks_bitmap = ();
    foreach ( @ostocks ) {
        $ostocks_bitmap[$_] = 1;
    }

    
    foreach (1 .. $NUM_STOCK_ROWS_PER_CALL) {
        
        my $s_i_id = $_;
        my $s_w_id = $w_id;
        my $s_quantity = DBGEN_UTIL::generate_random_integer_within(10, 100);
        my $s_dist_01  = DBGEN_UTIL::generate_astring_fixed(24);
        my $s_dist_02  = DBGEN_UTIL::generate_astring_fixed(24);
        my $s_dist_03  = DBGEN_UTIL::generate_astring_fixed(24);
        my $s_dist_04  = DBGEN_UTIL::generate_astring_fixed(24);
        my $s_dist_05  = DBGEN_UTIL::generate_astring_fixed(24);
        my $s_dist_06  = DBGEN_UTIL::generate_astring_fixed(24);
        my $s_dist_07  = DBGEN_UTIL::generate_astring_fixed(24);
        my $s_dist_08  = DBGEN_UTIL::generate_astring_fixed(24);
        my $s_dist_09  = DBGEN_UTIL::generate_astring_fixed(24);
        my $s_dist_10  = DBGEN_UTIL::generate_astring_fixed(24);
        my $s_ytd        = $S_YTD;
        my $s_order_cnt  = $S_ORDER_CNT;
        my $s_remote_cnt = $S_REMOTE_CNT;

        my $s_data;
        if ( defined($ostocks_bitmap[$s_i_id]) )
        {
            # Generate S_DATA string containing "ORIGINAL". S_DATA should
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
            
            $s_data =
                DBGEN_UTIL::generate_astring_fixed($X)
                . "ORIGINAL"
                . DBGEN_UTIL::generate_astring_fixed($Y);
        }
        else
        {
            # The spec does not say we need to exclude strings contains
            # "ORIGINAL" for other 90 percent.
            $s_data = DBGEN_UTIL::generate_astring(26, 50);
        }


        my @row = ($s_i_id,
                   $s_w_id,
                   $s_quantity,
                   $s_dist_01,
                   $s_dist_02,
                   $s_dist_03,
                   $s_dist_04,
                   $s_dist_05,
                   $s_dist_06,
                   $s_dist_07,
                   $s_dist_08,
                   $s_dist_09,
                   $s_dist_10,
                   $s_ytd,
                   $s_order_cnt,
                   $s_remote_cnt,
                   $s_data);

        print STOCK_TABLE join($attribute_separator, @row), "\n";
    }
}




sub generate_district_rows {

    my $D_YTD = "30000.00";
    my $D_NEXT_O_ID = 3001;
    
    my $NUM_DISTRICT_ROWS_PER_CALL = 10;

    
    my $w_id = shift;
    
    
    foreach ( 1 .. $NUM_DISTRICT_ROWS_PER_CALL )
    {
        my $d_id = $_;
        my $d_w_id = $w_id;
        my $d_name     = DBGEN_UTIL::generate_astring(6,10);
        my $d_street_1 = DBGEN_UTIL::generate_astring(10, 20);
        my $d_street_2 = DBGEN_UTIL::generate_astring(10, 20);
        my $d_city     = DBGEN_UTIL::generate_astring(10, 20);
        my $d_state    = DBGEN_UTIL::generate_astring_fixed(2);
        my $d_zip      = DBGEN_UTIL::generate_zip();

        my $d_tax =
            sprintf("%.4f",
                    DBGEN_UTIL::generate_random_integer_within(0, 2000) / 10000.0);
        my $d_ytd = $D_YTD;
        my $d_next_o_id = $D_NEXT_O_ID;
        

        my @row = ($d_id,
                   $d_w_id,
                   $d_name,
                   $d_street_1,
                   $d_street_2,
                   $d_city,
                   $d_state,
                   $d_zip,
                   $d_tax,
                   $d_ytd,
                   $d_next_o_id);

        print DISTRICT_TABLE join($attribute_separator, @row), "\n";

        generate_customer_rows($d_id, $d_w_id);
        generate_order_rows($d_id, $d_w_id);
    }
}




sub generate_customer_rows {
    
    my $C_MIDDLE       = "OE";
    my $C_CREDIT_LIM   = "50000.00";
    my $C_BALANCE      = "-10.00";
    my $C_YTD_PAYMENT  = "10.00";
    my $C_PAYMENT_CNT  = 1;
    my $C_DELIVERY_CNT = 0;

    my $NUM_CUSTOMER_ROWS_PER_CALL = 3000;
    my $NUM_BCUSTOMER_TUPLES = $NUM_CUSTOMER_ROWS_PER_CALL / 10;


    my $d_id   = shift;
    my $d_w_id = shift;


    my @bcustomers =
        DBGEN_UTIL::generate_unique_integers
        ($NUM_BCUSTOMER_TUPLES, 1, $NUM_CUSTOMER_ROWS_PER_CALL);
    my @bcustomers_bitmap = ();
    foreach ( @bcustomers ) {
        $bcustomers_bitmap[$_] = 1;
    }


    foreach ( 1 .. $NUM_CUSTOMER_ROWS_PER_CALL )
    {
        my $c_id = $_;
        my $c_d_id = $d_id;
        my $c_w_id = $d_w_id;


        # Generate C_LAST
        my $c_last_param;
        if ( $c_id <= 1000 ) {
            # Iterate through [0,999]
            $c_last_param = $c_id - 1;
        }
        else {
            $c_last_param = DBGEN_UTIL::NURand($C_C_LAST, $A_C_LAST, 0, 999);
        }
        my $c_last = DBGEN_UTIL::generate_last_name($c_last_param);


        my $c_middle   = $C_MIDDLE;
        my $c_first    = DBGEN_UTIL::generate_astring(8, 16);
        my $c_street_1 = DBGEN_UTIL::generate_astring(10, 20);
        my $c_street_2 = DBGEN_UTIL::generate_astring(10, 20);
        my $c_city     = DBGEN_UTIL::generate_astring(10, 20);
        my $c_state    = DBGEN_UTIL::generate_astring_fixed(2);
        my $c_zip      = DBGEN_UTIL::generate_zip();

        my $c_phone    = DBGEN_UTIL::generate_nstring_fixed(16);
        
        my $c_since = $CUSTOMER_TABLE_POPULATED_TIME;

        my $c_credit;
        if ( defined($bcustomers_bitmap[$c_id]) ) {
            $c_credit = "BC";
        }
        else {
            $c_credit = "GC";
        }

        my $c_credit_lim   = $C_CREDIT_LIM;
        my $c_discount =
            sprintf("%.4f",
                    DBGEN_UTIL::generate_random_integer_within(0, 5000) / 10000.0);
        my $c_balance      = $C_BALANCE;
        my $c_ytd_payment  = $C_YTD_PAYMENT;
        my $c_payment_cnt  = $C_PAYMENT_CNT;
        my $c_delivery_cnt = $C_DELIVERY_CNT;

        my $c_data = DBGEN_UTIL::generate_astring(300, 500);


        my @row = ($c_id,
                   $c_d_id,
                   $c_w_id,
                   $c_last,
                   $c_middle,
                   $c_first,
                   $c_street_1,
                   $c_street_2,
                   $c_city,
                   $c_state,
                   $c_zip,
                   $c_phone,
                   $c_since,
                   $c_credit,
                   $c_credit_lim,
                   $c_discount,
                   $c_balance,
                   $c_ytd_payment,
                   $c_payment_cnt,
                   $c_delivery_cnt,
                   $c_data);

        print CUSTOMER_TABLE join($attribute_separator, @row), "\n";
        
        generate_history_row($c_id, $c_d_id, $c_w_id);
    }
}





sub generate_history_row {

    my $H_AMOUNT = "10.00";
    
    my $c_id   = shift;
    my $c_d_id = shift;
    my $c_w_id = shift;


    my $h_c_id   = $c_id;
    my $h_d_id   = $c_d_id;
    my $h_c_d_id = $c_d_id;
    my $h_w_id   = $c_w_id;
    my $h_c_w_id = $c_w_id;
    
    my $h_date = DBGEN_UTIL::get_current_datetime();
    my $h_amount = $H_AMOUNT;
    my $h_data   = DBGEN_UTIL::generate_astring(12, 24);


    # Attribute order set to match table schema order
    my @row = ($h_c_id,
               $h_c_d_id,
               $h_c_w_id,
               $h_d_id,
               $h_w_id,
               $h_date,
               $h_amount,
               $h_data);

    print HISTORY_TABLE join($attribute_separator, @row), "\n";
}




sub generate_order_rows {

    my $O_ALL_LOCAL = 1;
    my $NUM_ORDER_ROWS_PER_CALL = 3000;

    my $d_id   = shift;
    my $d_w_id = shift;

    
    my @o_c_ids = DBGEN_UTIL::generate_permutation(1, 3000);

    
    foreach ( 1 .. $NUM_ORDER_ROWS_PER_CALL )
    {
        my $o_id = $_;
        my $o_c_id = $o_c_ids[$o_id-1];
        my $o_d_id = $d_id;
        my $o_w_id = $d_w_id;
        my $o_entry_d = DBGEN_UTIL::get_current_datetime();

        my $o_carrier_id;
        if ( $o_id < 2101 ) {
            $o_carrier_id = DBGEN_UTIL::generate_random_integer_within(1, 10);
        }
        else {
            $o_carrier_id = ""; # null
        }
        
        my $o_ol_cnt = DBGEN_UTIL::generate_random_integer_within(5, 15);
        my $o_all_local = $O_ALL_LOCAL;


        my @row = ($o_id,
                   $o_c_id,
                   $o_d_id,
                   $o_w_id,
                   $o_entry_d,
                   $o_carrier_id,
                   $o_ol_cnt,
                   $o_all_local);

        print ORDER_TABLE join($attribute_separator, @row), "\n";

        
        foreach ( 1 .. $o_ol_cnt ) {
            my $o_ol_cnt_value = $_;
            generate_order_line_row($o_id, $o_d_id, $o_w_id, $o_ol_cnt_value, $o_entry_d);
        }
        

        if ( $o_id >= 2101 ) {
            generate_new_order_row($o_id, $o_d_id, $o_w_id);
        }
    }
}




sub generate_order_line_row {

    
    my $OL_QUANTITY = 5;

    my $o_id           = shift;
    my $o_d_id         = shift;
    my $o_w_id         = shift;
    my $o_ol_cnt_value = shift;
    my $o_entry_d      = shift;


    my $ol_o_id = $o_id;
    my $ol_d_id = $o_d_id;
    my $ol_w_id = $o_w_id;
    my $ol_number = $o_ol_cnt_value;

    my $ol_i_id = DBGEN_UTIL::generate_random_integer_within(1, 100000);
    my $ol_supply_w_id = $o_w_id;
    
    my $ol_delivery_d;
    if ( $ol_o_id < 2101 ) {
        $ol_delivery_d = $o_entry_d;
    }
    else {
        $ol_delivery_d = ""; # null
    }

    my $ol_quantity = $OL_QUANTITY;
    
    my $ol_amount;
    if ( $ol_o_id < 2101 ) {
        $ol_amount = "0.00";
    }
    else {
        $ol_amount =
            sprintf("%.2f",
                    DBGEN_UTIL::generate_random_integer_within(1, 999999) / 100.0);
    }

    my $ol_dist_info = DBGEN_UTIL::generate_astring_fixed(24);


    my @row = ($ol_o_id,
               $ol_d_id,
               $ol_w_id,
               $ol_number,
               $ol_i_id,
               $ol_supply_w_id,
               $ol_delivery_d,
               $ol_quantity,
               $ol_amount,
               $ol_dist_info);

    print ORDER_LINE_TABLE join($attribute_separator, @row), "\n";
}




sub generate_new_order_row {
    
    my $o_id   = shift;
    my $o_d_id = shift;
    my $o_w_id = shift;


    my $no_o_id = $o_id;
    my $no_d_id = $o_d_id;
    my $no_w_id = $o_w_id;


    my @row = ($no_o_id, $no_d_id, $no_w_id);
    
    print NEW_ORDER_TABLE join($attribute_separator, @row), "\n";
}

