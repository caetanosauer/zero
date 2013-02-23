#!/usr/bin/perl -w

# a script to munge the output of 'svn log' into something approaching the 
# style of a GNU ChangeLog.
#
# to use this, just fill in the 'hackers' hash with the usernames and 
# name/emails of the people who work on your project, go to the top level 
# of your working copy, and run:
#
# $ svn log | /path/to/gnuify-changelog.pl > ChangeLog

%hackers = ( "ryan"      => 'Ryan Johnson <ryanjohn@ece.cmu.edu>',
             "ryanjohn"  => 'Ryan Johnson <ryanjohn@ece.cmu.edu>',
	     "ipandis"   => 'Ippokratis Pandis <ipandis@cs.cmu.edu>',
	     "nhardave"  => 'Nikos Hardavellas <nhardave@andrew.cmu.edu>',
	     "ddash"     => 'Debabrata Dash <ddash@andrew.cmu.edu>',
	     "ngm"       => 'Naju Mancheril <naju@cmu.edu>',
	     "root"      => 'root <>' );

$parse_next_line = 0;

while (<>) {
  # axe windows style line endings, since we should try to be consistent, and 
  # the repos has both styles in it's log entries.
  $_ =~ s/\r\n$/\n/;

  if (/^-+$/) {

    # we're at the start of a log entry, so we need to parse the next line
    $parse_next_line = 1;
    $first_line=0;
    print "\n";
  } elsif ($parse_next_line) {
    # transform from svn style to GNU style
    $parse_next_line = 0;

    @parts = split (/ /, $_);


    # Remove uninitialized string...
    print "$parts[0] $parts[4] $hackers{$parts[2]}\n";
    # read all the blank lines
    $first_line=1;
  } else {
      if ($first_line) {
	  if (! /^$/) {
	      print "  $_";
	      $first_line=0;
	  }
      } else {
	  print "  $_";
      }
  }
}

