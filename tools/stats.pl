#!/s/std/bin/perl -w

# <std-header style='perl' orig-src='shore'>
#
#  $Id: stats.pl,v 1.31 2010/06/08 22:29:23 nhall Exp $
#
# SHORE -- Scalable Heterogeneous Object REpository
#
# Copyright (c) 1994-99 Computer Sciences Department, University of
#                       Wisconsin -- Madison
# All Rights Reserved.
#
# Permission to use, copy, modify and distribute this software and its
# documentation is hereby granted, provided that both the copyright
# notice and this permission notice appear in all copies of the
# software, derivative works or modified versions, and any portions
# thereof, and that both notices appear in supporting documentation.
#
# THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
# OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
# "AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
# FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.
#
# This software was developed with support by the Advanced Research
# Project Agency, ARPA order number 018 (formerly 8230), monitored by
# the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
# Further funding for this work was provided by DARPA through
# Rome Research Laboratory Contract No. F30602-97-2-0247.
#
#   -- do not edit anything above this line --   </std-header>

#
# *************************************************************
#
# usage: <this-script> [-v] [-C] filename [filename]*
#
# -v verbose
# -C issue C code that can be compiled by a C compiler
# -enumOnly only generate ENUM file
# 
# *************************************************************
#
# INPUT: any number of sets of stats for software
#    layers called "name", with (unique) masks as follows:
#
#    name = mask class {
#    type STATNAME    Error string
#    type STATNAME    Error string
#     ...
#    type STATNAME    Error string
#    }
#
#    "type" must be a one-word type:
#        base_stat_t, base_float_t
#        are the only types the 
#        statistics package knows about.
#              int,uint,u_int,long,u_long,ulong are all converted to
#                base_stat_t
#              float,double are converted to base_float_t
#
#    "name" is used for module name; it's printed before printing the 
#        group of statistics for that module 
#
#  "mask" can be in octal (0777777), hex (0xabcdeff) or decimal 
#       notation
#
#   "class" is required. Output files (.i) are prefixed by class name.
#
#    Error string will be quoted by the translator.  Don't
#        put it in quotes in the .dat file.
#
#    Stat_info[] structure is meant to be part of a class:
#        w_error_info_t <class>::stat_names[] = ...

# *************************************************************
#
# OUTPUT:
#  for each class  this script creates:
#    MSG:  <class>_msg_gen.h    -- the descriptive strings
#    DEFWSTAT: <class>_def_gen.h -- #defined manifest <base,index> pairs
#                              for use with w_statistics_t
#    STRUCT: <class>_struct_gen.h -- the statistics variables. Defines
#                               the structure data members, 1 per stat.
#    CODEINCR: <class>_inc_gen.cpp -- the operator+=
#    CODEDECR: <class>_dec_gen.cpp -- the operator-=
#    CODEWSTAT: <class>_stat_gen.cpp -- the operator<< to w_statistics_t
#    CODEOUTP: <class>_out_gen.cpp -- the normal operator<< 
#    COLLECT: <class>_collect_gen.cpp -- code for some::vtable_collect()
#    GENERIC: <class>_generic_gen.cpp -- code for anything; usage defines how
#                                it's interpreted by defining a macro 
#                                GENERIC_CODE(x)
#                                -- REMOVED
#    ENUM: <class>_collect_enum_gen.h -- tokens vtable_enum.h
#
# *************************************************************


use Getopt::Long;

sub Usage
{
    my $progname = $0;
    $progname = s/.*[\\\/]//;
    print STDERR <<EOF;
Usage: $progname [-v] [-C] [--enumOnly] [--help] inputfile...
Generate C++ code which represents the statitistics from the inputfile.

    -v             verbose
    -C             generate valid C code only
    --enumOnly     generate only the enumeration
    --w            generate code for w_statistics use
    --help|h       print this message and exit
EOF
}

my %options = (v => 0, C => 0, enumOnly => 0, help => 0, w => 0);
my @options = ("v!", "C!", "enumOnly!", "help|h!", "w!");
my $ok = GetOptions(\%options, @options);

if (!$ok || $options{help})  {
    Usage();
    die(!$ok);
}

my $v = $options{v};
my $C = $options{C};
my $enumOnly = $options{enumOnly};
my $wstats = $options{w};

# initialize:
# can't initialize with proper basename
# until we read the name of the package

$basename = "unknown";

$maxw = 0; # max width of name yet found

$enumOnly = '' if !defined $enumOnly;
$wstats = '' if !defined $wstats;


foreach $FILE (@ARGV) {
    &translate($FILE);
    if($v) { printf(STDERR "done\n");}
}

sub pifdef {
    local(*F)=$_[0];
    if($C) {
    printf(F "#ifdef __cplusplus\n");
    }
}
sub pelse {
    local(*F)=$_[0];
    if($C) {
    printf(F "#else /*__cplusplus*/\n");
    }
}
sub pendif {
    local(*F)=$_[0];
    if($C) {
    printf(F "#endif /*__cplusplus*/\n");
    }
}

sub head {
    local(*FILE, $fname, $includes) = @_;

    my $hdrExclName = uc($fname);
    $hdrExclName =~ tr/A-Z0-9/_/c;

    if($v) {
        printf(STDERR 
        "head: trying to open "."$fname\n");
    }
    open(FILE, ">$fname") or die "cannot open $fname: $!\n";

    my $timeStamp = localtime;

    print FILE <<EOF;
#ifndef $hdrExclName
#define $hdrExclName

/* DO NOT EDIT --- GENERATED from $file by stats.pl
           on $timeStamp

<std-header orig-src='shore' genfile='true'>

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/
EOF

    if($includes) {
        print FILE <<EOF2;

#include "w_defines.h"
EOF2
    }

    print FILE <<EOF3;
/*  -- do not edit anything above this line --   </std-header>*/


EOF3


}

sub foot {
    local (*FILE, $fname) = @_;

    local $hdrExclName = uc($fname);
    $hdrExclName =~ tr/A-Z0-9/_/c;

    print FILE "\n#endif /* $hdrExclName */\n";
    if($v) {
    print STDERR "foot: trying to close $fname\n";
    }
    close FILE;
}

sub translate {
    local($file)=$_[0];
    local($base)="e";
    local($typelist)='"';

    open(FILE,$file) || die "Cannot open $file\n";
    if($v) { printf (STDERR "translating $file ...\n"); }

    LINE: while (<>)  {
    {
        # Handle comments -- some start with # after whitespace
        next LINE if (m/^\s*#/);
        # some are c++-style comments
        next LINE if (m=^\s*[/]{2}=);
    }

    # { to match the one in the next line (pattern)
    s/\s*[}]// && do {
        if($v) { 
        printf(STDERR 
        "END OF PACKAGE: ".$basename.",".$BaseName." = 0x%x\n", $base);
        }

        {
        if (!$enumOnly)  {
            # final stuff before closing files...

            if ($wstat)  {
                printf(DEFWSTAT "#define $BaseName%-20s 0x%x\n",'_STATMIN', $base);
                printf(DEFWSTAT "#define $BaseName%-20s 0x%x\n",'_STATMAX', $highest);
                printf(DEFWSTAT "\n");
            }
			# print MSG "\t\"dummy stat code\"\n";


            &pifdef(*CODEOUTP);
            # finish off operator<< to ostream 
            printf(CODEOUTP "\treturn o;\n}\n");
            &pendif(*CODEOUTP);

            &pifdef(*CODEINCR);
            # finish off operator+=  
            printf(CODEINCR "\treturn s;\n}\n");

            &pifdef(*CODEDECR);
            # finish off operator-=  
            printf(CODEDECR "\treturn s;\n}\n");

            if ($wstat)  {
                # define operator<<  to w_statistics_t
                printf(CODEWSTAT "w_statistics_t &\n");
                printf(CODEWSTAT 
                    "operator<<(w_statistics_t &s,const $class &t)\n{\n");
                printf(CODEWSTAT "    w_rc_t e;\n");
                printf(CODEWSTAT 
                    "    e = s.add_module_static(\"$description\",0x%x,%d,".
                    "t.stat_names,t.stat_types,(&t.$first)+1);\n",
                    $base,$cnt+1 );
                printf(CODEWSTAT 
                    "    if(e.is_error()) {\n");
                printf(CODEWSTAT "        cerr <<  e << endl;\n");
                printf(CODEWSTAT "    }\n");


                printf(CODEWSTAT "    return s;\n}\n");
                &pendif(*CODEWSTAT);

                &pifdef(*CODEWSTAT);
                printf(CODEWSTAT "const\n");
                &pendif(*CODEWSTAT);
                printf(CODEWSTAT "\t\tchar    *$class"."::stat_types =\n");
                printf(CODEWSTAT "$typelist\";\n");
            }

            &pifdef(*STRUCT);
            printf(STRUCT "public: \nfriend ostream &\n");
            printf(STRUCT "    operator<<(ostream &o,const $class &t);\n");
            &pifdef(*STRUCT);
            if ($wstat)  {
				printf(STRUCT "public: \nfriend w_statistics_t &\n");
				printf(STRUCT "    operator<<(w_statistics_t &s,const $class &t);\n");
			}

            printf(STRUCT "public: \nfriend $class &\n");
            printf(STRUCT "    operator+=($class &s,const $class &t);\n");
            printf(STRUCT "public: \nfriend $class &\n");
            printf(STRUCT "    operator-=($class &s,const $class &t);\n");
            printf(STRUCT "static const char    *stat_names[];\n");
            printf(STRUCT "static const char    *stat_types;\n");

            printf(STRUCT "#define W_$class  $maxw + 2\n");
            &pendif(*STRUCT);
        }
        }

        { # close files
            if (!$enumOnly)  {
                &foot(*MSG,$MSG_fname);
                if ($wstat)  {
                    &foot(*DEFWSTAT,$DEFWSTAT);
                    &foot(*CODEWSTAT,$CODEWSTAT);
                }
                &foot(*CODEINCR,$CODEINCR_fname);
                &foot(*CODEDECR,$CODEDECR_fname);
                &foot(*CODEOUTP,$CODEOUTP_fname);
                &foot(*COLLECT,$COLLECT_fname);
				# &foot(*GENERIC,$GENERIC_fname);
                &foot(*STRUCT,$STRUCT_fname);
            }
            &foot(*ENUM,$ENUM_fname);
        }

        $description = "";
        $basename = "";
        $BaseName = "";
        $base = "e";
    };

    s/\s*(\S+)\s+([\s\S]*)[=]\s*([0xabcdef123456789]+)\s*(\S+)\s*[{]// && do
        # } to match the one in the pattern
    { 
        # a new group
        $basename = $1;
        $description = $2;
        $base = $3;
        $class = $4;
        $first = "";
		if ($wstat)  {
			$first = "_dummy_before_stats";
		}
        $typelist='"'; # starting point

        $MSG_fname = $class."_msg_gen.h";
        $STRUCT_fname = $class."_struct_gen.h";
        if ($wstat)  {
            $DEFWSTAT = $class."_def_gen.h";
            $CODEWSTAT = $class."_stat_gen.cpp";
        }
        $CODEINCR_fname = $class."_inc_gen.cpp";
        $CODEDECR_fname = $class."_dec_gen.cpp";
        $CODEOUTP_fname = $class."_out_gen.cpp";
        $COLLECT_fname = $class."_collect_gen.cpp";
		# $GENERIC_fname = $class."_generic_gen.cpp";
        $ENUM_fname = $class."_collect_enum_gen.h";

        $BaseName = $basename;

        # translate lowercase to upper case 
        $BaseName =~ y/a-z/A-Z/;
        $base = oct($base) if $base =~ /^0/;
        if($class){
        if($v) {
            printf(STDERR "CLASS=$class\n");
        }
        } else {
        printf(STDERR "missing class name.");
        exit 1;
        }

        $cnt = -1;
        $highest = 0;

        if($v) {
        printf(STDERR "PACKAGE: $basename,$BaseName = 0x%x\n", $base);
        }

        {
        # open each file and write boilerplate
        {
            if (!$enumOnly)  {
            &head(*CODEINCR,$CODEINCR_fname,"true");
            &head(*CODEDECR,$CODEDECR_fname,"true");
            if ($wstat)  {
                &head(*DEFWSTAT,$DEFWSTAT,"true");
                &head(*CODEWSTAT,$CODEWSTAT,"true");
            }
            &head(*CODEOUTP,$CODEOUTP_fname,"true");
            &head(*COLLECT,$COLLECT_fname); # false
			# &head(*GENERIC,$GENERIC_fname); # false
            &head(*STRUCT,$STRUCT_fname,"true");
            &head(*MSG,$MSG_fname); # false
            }
            &head(ENUM,$ENUM_fname);
        }
        # end of boilerplate

        # stuff after boilerplate
        if (!$enumOnly)  {
            &pifdef(*CODEINCR);
            printf(CODEINCR "$class &\n");
            printf(CODEINCR 
                "operator+=($class &s,const $class &t)\n{\n");#}
            &pendif(*CODEINCR);

            &pifdef(*CODEDECR);
            printf(CODEDECR "$class &\n");
            printf(CODEDECR 
                "operator-=($class &s,const $class &t)\n{\n");#}
            &pendif(*CODEDECR);

            # define operator<<  to ostream
            &pifdef(*CODEOUTP);
            printf(CODEOUTP "ostream &\n");
            printf(CODEOUTP 
                "operator<<(ostream &o,const $class &t)\n{\n");#}
            &pendif(*CODEOUTP);


			if ($wstat)  {
				&pifdef(*STRUCT);
				printf(STRUCT " w_stat_t $first;\n");
				&pelse(*STRUCT);
				if($C) {
					printf(STRUCT " int $first;\n");
				}
				&pendif(*STRUCT);
			}
        }

        } # end opening files and writing tops of files
    };  
        
    next LINE if $base =~ "e";
    # peel off whitespace at beginning
    s/^\s+//;

    ($typ, $def, $msg) = split(/\s+/, $_, 3);
    next LINE unless $def;

    # peel semicolon off def if it's there
    $def =~ s/;$//;

#    {
#        #  save the name of the first value in the set
#        if($cnt==-1) {
#            $first = "$def";
#        }
#    }
    {
        # update counters
        ++$cnt;
        $val = $cnt + $base;
        if($highest < $val) { $highest = $val; }
    }
    {
        # take newline off msg
        chop $msg;
        # put the message in double quotes
        $msg = qq/$msg/;
    }

    {
        if($v) {
        printf(STDERR "typ is $typ\n");
        }
        # clean up abbreviated types
        $typ =~ s/^float/w_base_t::base_float_t/;
        $typ =~ s/^base_float/w_base_t::base_float_t/;
        $typ =~ s/^double/w_base_t::base_float_t/;
        $typ =~ s/^base_stat/w_base_t::base_stat_t/;
        $typ =~ s/^unsigned int/w_base_t::base_stat_t/;
        $typ =~ s/^unsigned long/w_base_t::base_stat_t/;
        $typ =~ s/^unsigned/w_base_t::base_stat_t/;
        $typ =~ s/^u_int/w_base_t::base_stat_t/;
        $typ =~ s/^u_long/w_base_t::base_stat_t/;
        $typ =~ s/^int/w_base_t::base_stat_t/;
        $typ =~ s/^long/w_base_t::base_stat_t/;
        $typ =~ s/^ulong/w_base_t::base_stat_t/;

        if ($typ =~ m/(w_base_t::base_float_t)/) {
        $typechar = 'd';
        } elsif ($typ =~ m/(w_base_t::base_stat_t)/) {
        $typechar = 'i';
        } elsif ($typ =~ m/([a-z])/) {
        $typechar = $1;
        }
        $typelist .= $typechar;
        if($v) {
        printf(STDERR "typelist is $typelist\n");
        }
    }
    {
        # do the printing for this line

        if (!$enumOnly)  {
        if ($wstat)  {
            printf(DEFWSTAT 
            "#define $BaseName"."_$def"."              0x%08x,$cnt\n",$base);
        }

        printf(MSG "/* $BaseName%s%-18s */ \"%s\",\n",  '_', $def, $msg);

        printf(STRUCT " $typ $def;\n");

        # code for vtable_collect function and generic code
        if ($typ =~ m/base/) {
            printf(COLLECT "\tt.set_base(VT_$def, TMP_GET_STAT($def));\n");
        } elsif ($typ =~ m/unsigned long/) {
            printf(COLLECT "\tt.set_ucounter(VT_$def, TMP_GET_STAT($def));\n");
        } elsif ($typ =~ m/int/) {
            printf(COLLECT "\tt.set_counter(VT_$def, TMP_GET_TSTAT($def));\n");
        } elsif ($typ =~ m/double/) {
            printf(COLLECT "\tt.set_double(VT_$def, TMP_GET_TSTAT($def));\n");
        } else {
           printf(STDERR "cannot handle type $typ\n");
           exit(1);
        }

        # code for generic use.  Note the lack of semicolon or comma.
        # The context-dependent GENERIC_CODE macro must add any punctuation
		# printf(GENERIC "\tGENERIC_CODE(VT_$def, $def)\n");

        # code for operator +=
        printf(CODEINCR "\ts.$def += t.$def;\n");

        # code for operator -=
        printf(CODEDECR "\ts.$def -= t.$def;\n");

        # code for operator << to ostream
        printf(CODEOUTP 
            "\to << setw(W_$class) << \"$def \" << t.$def << endl;\n");
        }
        if ($maxw < length($def)) {  $maxw = length($def); }

        printf(ENUM "\tVT_$def,\n");
    }

    } # LINE: while

    if($v) { printf(STDERR "translated $file\n");}

    close FILE;
}
