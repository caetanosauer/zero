#!/usr/bin/perl

# looklog.pl
#
# Monitors a directory (usually a log dir)


use strict;
use Getopt::Std;
use POSIX;

# fn: Print usage and exit
sub print_usage_exit {

    print "USAGE:    $0 -d LOGDIR\n";
    print "OPTIONS:  -d LOGDIR   - The log directory to monitor\n";
    print "          -t INTERVAL - The time interval (in secs)\n";

    exit();
}


# Variables
my $log_directory;
my $ls_interval;

my $LOGDIR = "log";
my $INTERVAL = 1;


# Parse command line
my %options;
getopts("hd:t:", \%options);

# Print usage only
print_usage_exit() if $options{h};

# Parse options
$ls_interval = $options{t} ? $options{t} : $INTERVAL;
$log_directory = $options{d} ? $options{d} : $LOGDIR;

my $count = 0;
while ($count<1) {

# while (TRUE)

    my $command = "ls -l $log_directory/*";
    system($command);
    sleep($ls_interval);
}


