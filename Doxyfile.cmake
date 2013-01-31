# Doxyfile 1.4.7

#---------------------------------------------------------------------------
# Project related configuration options
#---------------------------------------------------------------------------
PROJECT_NAME           = "Shore-MT with Fence-BTree"
PROJECT_NUMBER         = "Release 1.0"
OUTPUT_DIRECTORY       = dox
CREATE_SUBDIRS         = YES
OUTPUT_LANGUAGE        = English
BRIEF_MEMBER_DESC      = YES
REPEAT_BRIEF           = YES
ABBREVIATE_BRIEF       = "The $name class" \
                         "The $name widget" \
                         "The $name file" \
                         is \
                         provides \
                         specifies \
                         contains \
                         represents \
                         a \
                         an \
                         the
ALWAYS_DETAILED_SEC    = NO
INLINE_INHERITED_MEMB  = NO
FULL_PATH_NAMES        = YES
STRIP_FROM_PATH        = @PROJECT_SOURCE_DIR@
STRIP_FROM_INC_PATH    = 
SHORT_NAMES            = NO
JAVADOC_AUTOBRIEF      = YES
MULTILINE_CPP_IS_BRIEF = NO
INHERIT_DOCS           = YES
SEPARATE_MEMBER_PAGES  = NO
TAB_SIZE               = 1
OPTIMIZE_OUTPUT_FOR_C  = NO
OPTIMIZE_OUTPUT_JAVA   = NO
BUILTIN_STL_SUPPORT    = YES
DISTRIBUTE_GROUP_DOC   = NO
SUBGROUPING            = YES
#---------------------------------------------------------------------------
# Build related configuration options
#---------------------------------------------------------------------------
EXTRACT_ALL            = NO
EXTRACT_PRIVATE        = NO
EXTRACT_STATIC         = NO
EXTRACT_LOCAL_CLASSES  = YES
EXTRACT_LOCAL_METHODS  = NO
HIDE_UNDOC_MEMBERS     = YES
HIDE_UNDOC_CLASSES     = YES
HIDE_FRIEND_COMPOUNDS  = YES
HIDE_IN_BODY_DOCS      = NO
INTERNAL_DOCS          = NO
CASE_SENSE_NAMES       = NO
HIDE_SCOPE_NAMES       = NO
SHOW_INCLUDE_FILES     = NO
INLINE_INFO            = YES
SORT_MEMBER_DOCS       = NO
SORT_BRIEF_DOCS        = NO
SORT_BY_SCOPE_NAME     = NO
GENERATE_TODOLIST      = YES
GENERATE_TESTLIST      = YES
GENERATE_BUGLIST       = YES
GENERATE_DEPRECATEDLIST= YES
ENABLED_SECTIONS       = 
MAX_INITIALIZER_LINES  = 45
SHOW_USED_FILES        = YES
SHOW_DIRECTORIES       = YES
FILE_VERSION_FILTER    = 
#---------------------------------------------------------------------------
# configuration options related to warning and progress messages
#---------------------------------------------------------------------------
QUIET                  = NO
WARNINGS               = YES
WARN_IF_UNDOCUMENTED   = NO
WARN_IF_DOC_ERROR      = YES
WARN_NO_PARAMDOC       = YES
WARN_FORMAT            = "$file:$line: $text"
WARN_LOGFILE           = ./warnings
#---------------------------------------------------------------------------
# configuration options related to the input files
#---------------------------------------------------------------------------
INPUT                  = @CMAKE_CURRENT_SOURCE_DIR@/src/mainpage.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/api.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/internal.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/references.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/fc \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/common \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/sthread \
                         @CMAKE_CURRENT_SOURCE_DIR@/tools \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/sm/bf_hashtable.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/sm/bf_tree.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/sm/bf_fixed.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/sm/btcursor.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/sm/btree.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/sm/btree_impl.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/sm/btree_p.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/sm/page.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/sm/page_s.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/sm/smthread.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/sm/smstats.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/sm/sm_base.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/sm/sm_int_3.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/sm/lock_s.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/sm/sm.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/sm/xct.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/sm/sm_s.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/sm/stnode_p.h
FILE_PATTERNS          = *.c \
                         *.cpp \
                         *.h \
                         *.inc \
                         *.dox \
                         *.LICENSE \
                         *.h
RECURSIVE              = YES
EXCLUDE                = @CMAKE_CURRENT_SOURCE_DIR@/src/common/regcomp.cpp \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/common/regcomp_i.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/common/regerror.cpp \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/common/regerror_i.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/common/regex_cclass.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/common/regex_cname.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/common/regexec.cpp \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/common/regex_engine.cpp \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/common/regex_engine_i.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/common/regex.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/common/regex_posix.cpp \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/common/regex_posix.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/common/regex_utils.h \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/common/regfree.cpp \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/fc/rusage.cpp
EXCLUDE_SYMLINKS       = YES
EXCLUDE_PATTERNS       = */tests/* \
                         */smsh/* \
                         *_gen.h \
                         *_gen.cpp \
                         *.i \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/sm/*.cpp
EXAMPLE_PATH           = @CMAKE_CURRENT_SOURCE_DIR@/src/sm/tests \
                         @CMAKE_CURRENT_SOURCE_DIR@/src/sm/logdef.dat
EXAMPLE_PATTERNS       = *
EXAMPLE_RECURSIVE      = NO
IMAGE_PATH             = 
INPUT_FILTER           = 
FILTER_PATTERNS        = 
FILTER_SOURCE_FILES    = NO
#---------------------------------------------------------------------------
# configuration options related to source browsing
#---------------------------------------------------------------------------
SOURCE_BROWSER         = YES
INLINE_SOURCES         = NO
STRIP_CODE_COMMENTS    = NO
REFERENCED_BY_RELATION = YES
REFERENCES_RELATION    = YES
REFERENCES_LINK_SOURCE = NO
USE_HTAGS              = NO
VERBATIM_HEADERS       = YES
#---------------------------------------------------------------------------
# configuration options related to the alphabetical class index
#---------------------------------------------------------------------------
ALPHABETICAL_INDEX     = YES
COLS_IN_ALPHA_INDEX    = 5
IGNORE_PREFIX          = 
#---------------------------------------------------------------------------
# configuration options related to the HTML output
#---------------------------------------------------------------------------
GENERATE_HTML          = YES
HTML_OUTPUT            = html
HTML_FILE_EXTENSION    = .html
HTML_HEADER            = 
HTML_FOOTER            = 
HTML_STYLESHEET        = 
HTML_ALIGN_MEMBERS     = YES
GENERATE_HTMLHELP      = YES
CHM_FILE               = 
HHC_LOCATION           = 
GENERATE_CHI           = NO
BINARY_TOC             = NO
TOC_EXPAND             = NO
DISABLE_INDEX          = NO
ENUM_VALUES_PER_LINE   = 4
GENERATE_TREEVIEW      = YES
TREEVIEW_WIDTH         = 250
#---------------------------------------------------------------------------
# configuration options related to the LaTeX output
#---------------------------------------------------------------------------
GENERATE_LATEX         = NO
LATEX_OUTPUT           = latex
LATEX_CMD_NAME         = latex
MAKEINDEX_CMD_NAME     = makeindex
COMPACT_LATEX          = NO
PAPER_TYPE             = a4wide
EXTRA_PACKAGES         = 
LATEX_HEADER           = 
PDF_HYPERLINKS         = YES
USE_PDFLATEX           = YES
LATEX_BATCHMODE        = YES
LATEX_HIDE_INDICES     = YES
#---------------------------------------------------------------------------
# configuration options related to the RTF output
#---------------------------------------------------------------------------
GENERATE_RTF           = NO
RTF_OUTPUT             = rtf
COMPACT_RTF            = NO
RTF_HYPERLINKS         = NO
RTF_STYLESHEET_FILE    = 
RTF_EXTENSIONS_FILE    = 
#---------------------------------------------------------------------------
# configuration options related to the man page output
#---------------------------------------------------------------------------
GENERATE_MAN           = NO
MAN_OUTPUT             = man
MAN_EXTENSION          = .3
MAN_LINKS              = NO
#---------------------------------------------------------------------------
# configuration options related to the XML output
#---------------------------------------------------------------------------
GENERATE_XML           = NO
XML_OUTPUT             = xml
XML_SCHEMA             = 
XML_DTD                = 
XML_PROGRAMLISTING     = YES
#---------------------------------------------------------------------------
# configuration options for the AutoGen Definitions output
#---------------------------------------------------------------------------
GENERATE_AUTOGEN_DEF   = NO
#---------------------------------------------------------------------------
# configuration options related to the Perl module output
#---------------------------------------------------------------------------
GENERATE_PERLMOD       = NO
PERLMOD_LATEX          = NO
PERLMOD_PRETTY         = YES
PERLMOD_MAKEVAR_PREFIX = 
#---------------------------------------------------------------------------
# Configuration options related to the preprocessor   
#---------------------------------------------------------------------------
ENABLE_PREPROCESSING   = YES
MACRO_EXPANSION        = NO
EXPAND_ONLY_PREDEF     = NO
SEARCH_INCLUDES        = NO
INCLUDE_PATH           = 
INCLUDE_FILE_PATTERNS  = 
PREDEFINED             = 
EXPAND_AS_DEFINED      = 
SKIP_FUNCTION_MACROS   = YES
#---------------------------------------------------------------------------
# Configuration::additions related to external references   
#---------------------------------------------------------------------------
TAGFILES               = 
GENERATE_TAGFILE       = 
ALLEXTERNALS           = NO
EXTERNAL_GROUPS        = NO
PERL_PATH              = /usr/bin/perl
#---------------------------------------------------------------------------
# Configuration options related to the dot tool   
#---------------------------------------------------------------------------
CLASS_DIAGRAMS         = NO
HIDE_UNDOC_RELATIONS   = NO
HAVE_DOT               = YES
CLASS_GRAPH            = YES
COLLABORATION_GRAPH    = YES
GROUP_GRAPHS           = YES
UML_LOOK               = NO
TEMPLATE_RELATIONS     = YES
INCLUDE_GRAPH          = YES
INCLUDED_BY_GRAPH      = YES
CALL_GRAPH             = YES
CALLER_GRAPH           = YES
GRAPHICAL_HIERARCHY    = YES
DIRECTORY_GRAPH        = YES
DOT_IMAGE_FORMAT       = png
DOT_PATH               = @DOXYGEN_DOT_PATH@
DOTFILE_DIRS           = 
MAX_DOT_GRAPH_DEPTH    = 1000
DOT_TRANSPARENT        = NO
DOT_MULTI_TARGETS      = NO
GENERATE_LEGEND        = YES
DOT_CLEANUP            = YES
#---------------------------------------------------------------------------
# Configuration::additions related to the search engine   
#---------------------------------------------------------------------------
SEARCHENGINE           = YES
