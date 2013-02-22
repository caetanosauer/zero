#!/opt/csw/bin/perl

#@file:    rowize.pl
#
#@brief:   Gets the results of a measurement and puts all collected values for 
#          all the run with the same number of clients to the same row, calculating
#          the average value, the standard deviation and the percentage of the 
#          standard deviation to the average.
#
#@note:    Each parsed type (e.g., Throughput) should start with "+++" (e.g., "++ Throughput")
#          Each run should have a header with the second word "measue" (e.g., "(tm1-base) measure ...")
#          All the collected values should be on a separate line (e.g., 140.34)
#
#@example: ++ Throughput
#          tm1-base) measure 1 1 1 30 24
#          1024.24
#          1001.95
#          tm1-base) measure 4 1 4 30 24
#          4024.56
#          4021.56
#          ++ UserTime
#          tm1-base) measure 1 1 1 30 24
#          104.2
#          121.9
#          tm1-base) measure 4 1 4 30 24
#          404.5
#          401.6
#
#@author:  Ippokratis Pandis
#
#@date:    Feb 2010

use strict;
use Getopt::Std;

use Statistics::Descriptive;

# fn: Print usage and exit
sub print_usage_exit 
{
    print "USAGE:    $0 -f <INFILE>\n";
    print "OPTIONS:  -f - Input file\n"; 

    exit();
}

# Options
my $infile;
my $outfile;
my $outdir = ".";

# Parse command line
my %options;
getopts("hf:d:",\%options);

# If -h print usage only
print_usage_exit() if $options{h};

$infile = $options{f} ? $options{f} : print_usage_exit();
$outfile = "row.$infile";

# Variables
my $runs=0;
my $mvalues=0;

my $line;
my $value;
my $valueCnt=0;

# Characteristics of each run
my $tag;
my $sf;
my $spread;
my $clients;
my $entries;

# The four arrrays we will keep the data
my @theTags;
my @theClients;
my @theEntriesPerClient;
my @theValues;

#open(infile) or die("Could not open input file $infile.");
#open(infile);

open INFILE, "<", "$infile" or die "Cannot open file $infile.";

# Read input file
# Count the number of different measured types (mvalues)
# Count the number of runs (runs)
foreach $line (<INFILE>) {

    ($value, $tag, $sf, $spread, $clients) = split(' ',$line);

# Check if it is a new tag (measured type)
    if (lc($value) eq "++")
    {
        # printf 'TAG -> %d. %s', $mvalues, $tag;
        # printf "\n";
        $mvalues=$mvalues+1;
        push(@theTags,$tag);
        if ($mvalues == 2)
        {
            push(@theEntriesPerClient,$entries);
        }
        $entries=0;
    }
    
# It will count all the lines whose tag is "measure" and we are still in
# the first tag
# It will zero the number of entries per measurem
    if (($mvalues == 1) && (lc($tag) eq "measure"))
    {
        # printf 'CL  -> %d. %d', $runs, $clients;
        # printf "\n";
        $runs+=1;
        push(@theClients,$clients);

        # printf 'ENT -> %d. %d', $runs-1, $entries;
        # printf "\n";
        push(@theEntriesPerClient,$entries);
        $entries=0;
    }

# It will count the number of entries per measurement
    if (($mvalues == 1) && (lc($tag) ne "measure") && (lc($value) ne "++"))
    {
        # printf 'ENT -> %d. %d', $clients, $entries;
        # printf "\n";
        $entries+=1;
    }

# It will store all the values (non tags or measure)
    if ((lc($tag) ne "measure") && (lc($value) ne "++"))
    {
        # printf 'VAL -> %d. %d', $valueCnt, $value;
        # printf "\n";
        $valueCnt+=1;
        push(@theValues,$value);
    }
}

#add at the head of theTags array the Clients tag
#unshift(@theTags,"Clients");
# print "TheTags:\n";
# print "@theTags";
# print "\n";

# print "TheClients\n";
# print "@theClients";
# print "\n";

#remove the first element of theEntriesPerClient array
shift(@theEntriesPerClient);
# print "TheEntriesPerClient\n";
# print "@theEntriesPerClient";
# print "\n";

# Calculate the totalEntries
my $totalEntries=0;
foreach $entries (@theEntriesPerClient)
{
    $totalEntries+=$entries;
}

#printf "\n";
printf "$infile\n";

# print "TotalEntries: " . $totalEntries . "\n";

# print "TheValues\n";
# print "@theValues";
# print "\n";


# # Calculate the totalValues
# my $totalValues=0;
# foreach $value (@theValues)
# {
#     $totalValues+=1;
# }
# print "TotalValues: " . $totalValues . "\n";


# Write the header
print "Clients ";
foreach $tag (@theTags)
{
    print $tag . " StdDev StdDev% ";
}

my $sumEntries=0;
my $offsetClient=0;
my $offsetValue=0;
my $tagCnt=0;

my $stat;
my $stMean=0;
my $stStd=0;
my $stStdPer=0;

printf("\n");

foreach $entries (@theEntriesPerClient) 
{
    printf $theClients[$offsetClient] . " ";
    $tagCnt=0;

    foreach $tag (@theTags)
    {
        $offsetValue=($totalEntries*$tagCnt)+$sumEntries;
        $clients=0;
        $stat = Statistics::Descriptive::Full->new();

        while ($clients < $entries)
        {         
            $value=$theValues[$offsetValue];
                       
            $stat->add_data($value);

            $clients+=1;
            $offsetValue+=1;
        }
        
        $stMean=$stat->mean();
        $stStd=$stat->standard_deviation();

        # avoid division by zero
        if ($stMean==0)
        {
            $stStdPer=0;
        }
        else
        {
            $stStdPer=(100*$stStd/$stMean);
        }

        printf("%.2f %.2f %.1f\% ",$stMean,$stStd,$stStdPer);        
        
        $tagCnt+=1;
    }

    printf("\n");
    
    $sumEntries+=$entries;
    $offsetClient+=1;
}
