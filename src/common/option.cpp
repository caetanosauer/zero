/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager
   
                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne
   
                         All Rights Reserved.
   
   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.
   
   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/*<std-header orig-src='shore'>

 $Id: option.cpp,v 1.58 2010/12/08 17:37:34 nhall Exp $

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

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#define OPTION_C
#ifdef __GNUC__
#   pragma implementation
#endif

#include "w.h"
#include <cstring>
#include <cctype>
#include "w_autodel.h"
#include "option.h"
#include "w_debug.h"

#include "regex_posix.h"

#ifdef EXPLICIT_TEMPLATE
template class w_list_i<option_t,unsafe_list_dummy_lock_t>;
template class w_list_t<option_t,unsafe_list_dummy_lock_t>;
#endif /*__GNUG__*/


option_t::option_t() : _name(NULL), _value(NULL), _setFunc(NULL)
{
}

option_t::~option_t()
{
    _link.detach();
    if (_value) free(_value);
    _value = NULL;
    _name = NULL;
}

w_rc_t option_t::init(const char* name, const char* newPoss,
                          const char* defaultVal, const char* descr,
                          bool req, OptionSetFunc setFunc,
                      ostream *err_stream) 
{
    _name = name;
    _possible_values = newPoss;
    _default_value = defaultVal;
    _required = req;
    _description = descr;
    _setFunc = setFunc;
    _set = false;
    if (!_required) {
            if (_default_value) {
            w_rc_t rc = set_value(_default_value, false, err_stream);
            _set = false;
            return rc;
            }
    }
    return RCOK;
}

bool option_t::match(const char* matchName, bool exact)
{
    int i;
    bool equal;

    i = 0;
    equal = true;

    DBG(<<"name to match is " << matchName);
    while(equal) {
            if ( (matchName[i] != '\0') && (_name[i] != '\0') ) {
            if (matchName[i] != _name[i]) {
                    DBG(<<"fails because at " << i << 
                        " matchName is " << matchName[i] <<
                        " _name is " << _name[i]
                    );
                    equal = false;
            } 
            } else {
            if (matchName[i] == '\0') {
                break; // since at end of string
            } else {
                DBG(<<"fails because at " << i << 
                    " matchName is " << matchName[i] <<
                    " _name is " << _name[i]
                );
                equal = false;         // since _name[i] == '\0' 
                                // matchName must be longer
            }
            }
            i++;
    }

    if (i == 0) {
            equal = false;
    } else {
        if (exact && (matchName[i] != '\0' || _name[i] != '\0')) {
            equal = false;
        }
    }
    return equal;
}

w_rc_t option_t::set_value(const char* value, bool overRide, ostream* err_stream)
{
    DBG(<<"option_t::set_value " << name()
        << " value = " << value);
    if (_set && !overRide) {
            /* option not set */
            return RCOK;
    }

    if (value == NULL) {
        if (_value) {
            free(_value);
            _set = false;
        }
        return RCOK;
    } else {
        W_DO(_setFunc(this, value, err_stream));
    }
    return RCOK; /* option was set successfully */
}

w_rc_t option_t::copyValue(const char* value)
{
    char* new_value;
   
    if (_value) {
        new_value = (char*)realloc(_value, strlen(value)+1);
    } else {
        new_value = (char*)malloc(strlen(value)+1);
    }
    if (!new_value) {
        return RC(fcOUTOFMEMORY);
    }
    _value = new_value;
    strcpy(_value, value);
    _set = true;
    return RCOK; /* option was set successfully */
}

w_rc_t option_t::concatValue(const char* value)
{
    char* new_value;
    const char* separator = "\n";

    if (_value) {
        new_value = (char*)realloc(_value, strlen(_value) + strlen(separator) + strlen(value)+1);
    } else {
        new_value = (char*)malloc(strlen(value)+1);
    }
    if (!new_value) {
        return RC(fcOUTOFMEMORY);
    }
    _value = new_value;
    strcat(_value, separator);
    strcat(_value, value);
    _set = true;
    return RCOK; /* option was set successfully */
}

bool option_t::str_to_bool(const char* str, bool& badStr)
{
    badStr = true;
    if (strlen(str) < 1) return false;

    switch (str[0]) {
        case 't': case 'T': case 'y': case 'Y':
            badStr = false;
            return true;
            //break;
        case 'f': case 'F': case 'n': case 'N':
            badStr = false;
            return false;
            //break;
        default:
            return false;
    }
}

w_rc_t option_t::set_value_bool(option_t* opt, const char* value, ostream* err_stream)
{
    bool badVal;
    str_to_bool(value, badVal);
    if (badVal) {
        if (err_stream) *err_stream << "value must be true,false,yes,no";
        return RC(OPT_BadValue);
    }
    W_DO(opt->copyValue(value));
    return RCOK;
}

w_rc_t 
option_t::set_value_int4(
        option_t* opt, 
        const char* value, 
        ostream* err_stream)
{
    long l;
    char* lastValid;

    errno = 0;
    l = strtol(value, &lastValid, 0/*any base*/);
    if(((l == LONG_MAX) || (l == LONG_MIN)) && errno == ERANGE) {
        /* out of range */
        if (err_stream)  {
                *err_stream 
                << "value is out of range for a long integer " 
                << value;
            return RC(OPT_BadValue);
        }
    }
    if (lastValid == value) {
        // not integer could be formed
        if (err_stream) *err_stream << "no valid integer could be formed from " << value;
        return RC(OPT_BadValue);
   }
    // value is good
    W_DO(opt->copyValue(value));
    return RCOK;
}

w_rc_t 
option_t::set_value_long(
        option_t* opt, 
        const char* value, 
        ostream* err_stream)
{
        /* XXX breaks on 64 bit machines? */
        return        set_value_int4(opt, value, err_stream);
}

w_rc_t 
option_t::set_value_int8(
        option_t* opt, 
        const char* value, 
        ostream* err_stream)
{

    char* lastValid;
    errno = 0;
    // Keep compiler from complaining about
    // unused l: 
    // int64_t l =
    (void) w_base_t::strtoi8(value, &lastValid);
    if (errno == ERANGE) {
        /* out of range */
        if (err_stream)  {
                *err_stream 
                << "value is out of range for a long integer " 
                << value;
            return RC(OPT_BadValue);
        }
    }
    if (lastValid == value) {
        // not integer could be formed
        if (err_stream) *err_stream 
            << "no valid integer could be formed from " << value;
        return RC(OPT_BadValue);
    }
    // value is good

    W_DO(opt->copyValue(value));
    return RCOK;
}

w_rc_t 
option_t::set_value_long_long(
        option_t* opt, 
        const char* value, 
        ostream* err_stream)
{
        return        set_value_int8(opt, value, err_stream);
}

w_rc_t option_t::set_value_charstr(
        option_t* opt, 
        const char* value, 
        ostream * //err_stream_unused
        )
{
    W_DO(opt->copyValue(value));
    return RCOK;
}

///////////////////////////////////////////////////////////////////
//            option_group_t functions                                 //
///////////////////////////////////////////////////////////////////

bool option_group_t::_error_codes_added = false;

#include "opt_einfo_gen.h"

option_group_t::option_group_t(int maxNameLevels)
: _options(W_LIST_ARG(option_t, _link), unsafe_nolock),
  _class_name(NULL),
  _levelLocation(NULL),
  _maxLevels(maxNameLevels),
  _numLevels(0)
{
    if (!_error_codes_added) {
        if (!(w_error_t::insert(
                "Options Package",
                opt_error_info, 
                OPT_ERRMAX - OPT_ERRMIN + 1)) ) {
            abort();
        }
        _error_codes_added = true;
    }

    
    _class_name = (char*)malloc(1); // use malloc so we can realloc
    _levelLocation = new char*[_maxLevels];

    if (_class_name == NULL || _levelLocation == NULL) {
        W_FATAL(fcOUTOFMEMORY);
    }
    _class_name[0] = '\0';
}

option_group_t::~option_group_t()
{
    w_list_i<option_t,unsafe_list_dummy_lock_t> scan(_options);
    // This list mod is ok
    while (scan.next()) {
        delete scan.curr();
    }
    if (_class_name) free(_class_name);
    if (_levelLocation) delete[]  _levelLocation;
}

w_rc_t option_group_t::add_option(
        const char* name, const char* newPoss,
        const char* default_value, const char* description,
        bool required, option_t::OptionSetFunc setFunc,
        option_t*& newOpt,
        ostream *err_stream
        )
{
    DBG(<<"option_group_t::add_option " << name );
    W_DO(lookup(name, true, newOpt));
    if (newOpt) return RC(OPT_Duplicate);

    newOpt = new option_t();
    if (!newOpt) return RC(fcOUTOFMEMORY);
    w_rc_t rc = newOpt->init(name, newPoss, 
        default_value, description, required, setFunc, err_stream);
    if (rc.is_error()) {
        delete newOpt;
        newOpt = NULL;
        return rc;
    }
    _options.append(newOpt);
    return RCOK;
}

w_rc_t option_group_t::add_class_level(const char* name)
{
    if (_numLevels == _maxLevels) {
            return RC(OPT_TooManyClasses);
    }

    char* new_str = (char*)realloc(_class_name, strlen(_class_name)+strlen(name)+2/*for . and \0*/);
    if (!new_str) {
        return RC(fcOUTOFMEMORY);
    }
    _class_name = new_str;
    _levelLocation[_numLevels] = &(_class_name[strlen(_class_name)]);
    _numLevels++;
    strcat(_class_name, name);
    strcat(_class_name, ".");

    return RCOK;
}

w_rc_t option_group_t::lookup(const char* name, bool exact, option_t*& returnOption)
{
    DBG(<<"option_group_t::lookup " << name << " exact=" << exact);
    w_rc_t rc;

    returnOption = NULL;

    w_list_i<option_t,unsafe_list_dummy_lock_t> scan(_options);
    while (scan.next()) {
         DBG(<<"scan.curr()==|" << scan.curr()->name() <<"|");
         if (scan.curr()->match(name, exact)) {
            DBG(<<"match");
            if (returnOption != NULL) {
                returnOption = NULL;        // duplicate
                rc = RC(OPT_Duplicate);
            } else {
                // match found;
                returnOption = scan.curr();
            }
            break;
        } else {
            DBG(<<"nomatch");
        }
    }
    DBG(<<"option_group_t::lookup " << name << " scan done" );
    return rc;
}

w_rc_t
option_group_t::lookup_by_class(
    const char* optClassName, 
    option_t*& returnOption,
    bool exact
)
{
    const char*                c;
    const char*                lastSpecial;

    int                        lastNewSpecial;
    w_rc_t                     rc;
    int                        newClen;
    const char*                regex = NULL;
    bool                       backSlash = false;

    DBG(<<"option_group_t::lookup_by_class " << optClassName);

    // regular expr is placed here, at most
    // it can be twice as long as optClassName
    int                        newC_len = strlen(optClassName)*2;
    char*                newC = new char[newC_len];
    if (!newC) return RC(fcOUTOFMEMORY);
    w_auto_delete_array_t<char>        newC_delete(newC);

    // process the option name and classification suffix
    // Make a regular expression for the option classification
    lastSpecial = optClassName-1;
    lastNewSpecial = 0;
    newC[lastNewSpecial] = '^';
    for (c = optClassName, newClen = 1; *c != '\0'; c++, newClen++) {
            if (!backSlash) {
                    switch (*c) {
                    case '*':
                            newC[newClen] = '.';
                            newClen++;
                            newC[newClen] = '*';
                            lastSpecial = c;
                            lastNewSpecial = newClen;
                            break;
                    case '.':
                            newC[newClen] = '\\';
                            newClen++;
                            newC[newClen] = '.';
                            lastSpecial = c;
                            lastNewSpecial = newClen;
                            break;
                    case '?':
                            newC[newClen++] = '[';
                            newC[newClen++] = '^';
                            newC[newClen++] = '.';
                            newC[newClen++] = ']';
                            newC[newClen] = '*';
                            lastSpecial = c;
                            lastNewSpecial = newClen;
                            break;
                    case ':':
                        // no semicolons allowed (really internal error)
                        rc = RC(OPT_Syntax);
                            break;
                    case ' ': case '\t':        
                            rc = RC(OPT_IllegalClass);
                            break;
                    case '\\':
                            backSlash = true;
                            newClen--;
                            break;
                    default:
                            newC[newClen] = *c;
                    }

            } else {
                    newC[newClen] = *c;
                    backSlash = false;
            }

            if (lastNewSpecial == newC_len) {
            rc = RC(OPT_ClassTooLong);
            }
    }

    if (rc.is_error()) return rc;

    if (*c != '\0') {
            return RC(OPT_Syntax);
    } else {
            newC[newClen] = *c;
    }

    //        See if class name is missing
    if (lastSpecial == (optClassName-1)) {
            return RC(OPT_IllegalClass);
    }

    newC[lastNewSpecial+1] = '$';
    newC[lastNewSpecial+2] = '\0';

    if (newC[1] == '$') {
            strcat(newC, ".*");
    }

    regex = re_comp(newC);
    if (regex != NULL) {
        cerr << "regular expression error: " << regex << endl;
        rc = RC(OPT_IllegalClass);
    } else {
            if (re_exec(_class_name) == 1) {
            DBG(<<"re_exec("<<_class_name<<") returned 1");

            // see if option name matches
            const char* option = lastSpecial+1;
            return lookup(option, exact, returnOption);
            } else {
            DBG(<<"re_exec("<<_class_name<<") failed");
            rc = RC(OPT_NoClassMatch);
        }

    }

    delete regex;
    returnOption = NULL;        
    return rc; 
}

w_rc_t
option_group_t::set_value(
    const char* name, bool exact,
    const char* value, bool overRide,
    ostream* err_stream)
{
    DBG(<<"option_group_t::set_value: " << name
        << " exact=" << exact);
    option_t* opt = 0;
    W_DO(lookup(name, exact, opt));
    if (!opt) {
        DBG(<<"nomatch");
        return RC(OPT_NoOptionMatch);
    }
    DBG(<<"MATCH");
    W_DO(opt->set_value(value, overRide, err_stream));
    return RCOK;
}


void
option_group_t::print_usage(bool longForm, ostream& err_stream)
{
    option_t*        current;

    w_list_i<option_t,unsafe_list_dummy_lock_t> scan(_options);
    while (scan.next()) {
        current = scan.curr();
        if (current->is_required()) {
            err_stream << " ";
        } else {
            err_stream << " [";
        }
        if (current->possible_values() == NULL) {
            err_stream << "-" << current->name();
        } else {
            err_stream << "-" << current->name() 
                << " <" << current->possible_values() << ">";
        }

        if (!current->is_required()) err_stream << "]";

        if (longForm) {
            err_stream << "\n\t\t" << current->description() << "\n";
            if (current->default_value() == NULL) {
                err_stream << "\t\tdefault value: <none>\n";
            } else {
                err_stream << "\t\tdefault value: " << current->default_value() << "\n";
            }
        }
    }
    if (!longForm) err_stream << endl;
    err_stream << "[brackets means optional]" << endl;

    return;
}

void option_group_t::print_values(bool longForm, ostream& err_stream)
{
    option_t*        current;

    err_stream << "Values for options of class " << _class_name << ":";
    if (longForm) err_stream << "\n";
    w_list_i<option_t,unsafe_list_dummy_lock_t> scan(_options);
    while (scan.next()) {
        current = scan.curr();
            if (current->is_set()) { // only print option which have a value set
            if (!current->is_required()) {
                err_stream << " [-" << current->name() << " ";
            } else {
                err_stream << " -" << current->name() << " ";
            }
            if (current->value() == NULL) {
                err_stream << "<not-set>";
            } else {
                err_stream << current->value();
            }
            if (!current->is_required()) err_stream << "]";

            if (longForm) err_stream << "\n";
            }
    }
    if (!longForm) err_stream << endl;

    return;
}

w_rc_t option_group_t::check_required(ostream* err_stream)
{
    DBG(<<"option_group_t::check_required");
    w_rc_t rc;
    option_t* curr;
    bool at_least_one_not_set = false;

    w_list_i<option_t,unsafe_list_dummy_lock_t> scan(_options);
    while ((curr = scan.next())) {
        if (curr->is_required() && !curr->is_set()) {
            if (err_stream) *err_stream << "option <" << curr->name() << "> is required but not set\n"; 
            at_least_one_not_set = true;
        }
    }
    if (at_least_one_not_set) rc = RC(OPT_NotSet);
    return rc;
}

w_rc_t option_group_t::parse_command_line(const char** argv, int& argc, size_t min_len, ostream* err_stream)
{

    /*
     * FUNCTION DESCRIPTION:
     *
     * This function examines command line arguments for configuration
     * options.  It returns argv with any sm configuration options
     * removed.  Argc is adjusted as well.
     */

    w_rc_t        rc;
    int                i;
    option_t*         opt;

    i = 0;
    while (i < argc && !rc.is_error()) {
        if (argv[i][0] == '-' && strlen(argv[i]) > min_len) {
            rc = lookup(argv[i]+1, false, opt);
            if (!rc.is_error() && opt) {
                // found the option
                if (i+1 == argc) {

                    if (err_stream) *err_stream << "missing argument for " << argv[i];
                    // remove this option argument
                    argc--;
                    rc = RC(OPT_BadValue);
                } else {
                        // VCPP Wierdness if (rc = ...)
                    rc = opt->set_value(argv[i+1], true, err_stream);
                        if (rc.is_error()) {
                        if (err_stream) *err_stream << "bad value for argument " << argv[i];
                    }

                    // remove these option and value arguments
                    for (int j = i+2; j < argc; j++) {
                        argv[j-2] = argv[j];
                    }
                    argc -= 2;
                }
            } else if (!rc.is_error()) {
                // no real error, just not found
                i++;  // advance to next argument
            } else {
                // fall out of loop due to error
            }
        } else {
            i++;  // advance to next argument
        }
    }
    return(rc);
}

///////////////////////////////////////////////////////////////////
//            option_file_scan_t functions                                 //
///////////////////////////////////////////////////////////////////

const char *option_stream_scan_t::default_label = "istream";

option_stream_scan_t::option_stream_scan_t(istream &is, option_group_t *list)
: _input(is),
  _optList(list),
  _line(0),
  _label(default_label),
  _lineNum(0)
{
}

option_stream_scan_t::~option_stream_scan_t()
{
        if (_line) {
                delete [] _line;
                _line = 0;
        }
        if (_label != default_label) {
                delete [] _label;
                _label = default_label;
        }
}


void option_stream_scan_t::setLabel(const char *newLabel)
{
        if (_label != default_label) {
                delete [] _label;
                _label = default_label;
        }
        if (newLabel) {
                // behavior in case of memory failure is fail safe
                char *s = new char[strlen(newLabel) + 1];
                if (s) {
                        strcpy(s, newLabel);
                        _label = s;
                }
        }
}

w_rc_t option_stream_scan_t::scan(
        bool overRide, 
        ostream& err_stream, 
        bool exact,
        bool mismatch_ok
)
{
    option_t*        optInfo;
    int                optBegin, optEnd, valBegin, valEnd, valLength;
    int         i;
    bool        backSlash = false;
    const char* optionName = NULL;

    if (!_line) {
            _line = new char[_maxLineLen+1];        
            if (!_line)
                return RC(fcOUTOFMEMORY);
    }

    DBG(<<"scanning options stream " << _label);

    w_rc_t rc;
    while ( !rc.is_error() && (_input.getline(_line, _maxLineLen) != NULL) ) {
            _lineNum++;
        DBG(<<"scan line " << _lineNum);
        
        if (strlen(_line)+1 >= _maxLineLen) {
            err_stream << "line " << _lineNum << " is too long";
            rc = RC(OPT_IllegalDescLine);
            break;
        }

            // 
            //        Find the classOption field
            //
            optBegin = -1;
            optEnd = -1;
            for (i = 0; _line[i] != '\0'; i++) {
            if (optBegin < 0) {
                    /* if whitespace */
                    if (isspace(_line[i])) {
                            continue; 
                    } else {
                            optBegin = i;
                    }
            }
            if (_line[i] == '\\') {
                backSlash = !backSlash;
                if (backSlash) continue;
            }
            if (_line[i] == ':' && !backSlash) {
                optEnd = i;        
                break; /* out of for loop */
            } 
            backSlash = false;
            }

            // check for a comment or blank line and skip it
            if (optBegin < 0 || _line[optBegin] == '#' || _line[optBegin] == '!') {
            continue;        
            }

            // check syntax
            if (optEnd < 0) {
                    err_stream << "syntax error at " << _label << ":" << _lineNum;
                    rc = RC(OPT_Syntax);
                    break;
            }
        _line[optEnd] = '\0';

        optionName = _line+optBegin;

            rc = _optList->lookup_by_class(optionName, optInfo, exact);
        if (!rc.is_error() && optInfo == NULL) {
            //option name was not found
            rc = RC(OPT_NoOptionMatch);
        }

            switch (rc.err_num()) {
            case 0:
                    break;
            case OPT_NoClassMatch:
                // no error message needed since this is ok
                    break;
            case OPT_NoOptionMatch:
                if(!mismatch_ok) {
                    err_stream << "unknown option at " << _label << ":" << _lineNum;
                }
                    break;
            case OPT_Duplicate:
                    err_stream << "option name is not unique at "
                        << _label << ":" << _lineNum;
                    break;
            case OPT_Syntax:
                    err_stream << "syntax error at " << _label << ":" << _lineNum;
                    break;
            case OPT_IllegalClass:
                    err_stream << "illegal/missing option class at "
                        << _label << ":" << _lineNum;
                    break;
            default:
                    err_stream << "general error in option at "
                        << _label << ":" << _lineNum;
                    break;        
            }

            if (rc.is_error()) {
            if (rc.err_num() == OPT_NoClassMatch) {
                // this is ok, we just skip the line
                rc = RCOK;
            }
            if (mismatch_ok && rc.err_num() == OPT_NoOptionMatch) {
                // this is ok, we just skip the line
                rc = RCOK;
            }
            continue;
            }

            // 
            //        Find the option value field
            //
            valBegin = -1;
            valEnd = -1;
            for (i = optEnd+1; _line[i] != '\0'; i++) {
            /* if whitespace */
            if (isspace(_line[i])) {
                if (valBegin < 0) {
                    continue; 
                }        
            } else {
                if (valBegin < 0) {
                    valBegin = i;
                }        
                valEnd = i;
            }
            }

            if (valBegin < 0) {
            err_stream << "syntax error (missing option value) at "
                << _label << ":" << _lineNum;
            rc = RC(OPT_Syntax);
            break;
            }

            // remove any quote marks
            if (_line[valBegin] == '"') {
            valBegin++;
            if (_line[valEnd] != '"') {
                    err_stream << "syntax error (missing \") at "
                        << _label << ":" << _lineNum;
                rc = RC(OPT_Syntax);
                break;
            }
            valEnd--;
            }
            valLength = valEnd - valBegin + 1;

            if (valLength < 0) {
            err_stream << "syntax error (bad option value) at "
                << _label << ":" << _lineNum;
            rc = RC(OPT_Syntax);
            break;
            }

            if (rc.is_error()) {
            continue;
            }

            // if option was found to set 
            if (optInfo != NULL) {
            _line[valEnd+1] = '\0';
            rc = optInfo->set_value(_line+valBegin, overRide, &err_stream);
            if (rc.is_error()) {
                err_stream << "Option value error at "
                        << _label << ":" << _lineNum;
                break;
            }
            }
    
    }
    DBG(<<"last line scanned: " << _lineNum);

    return rc; 
}

option_file_scan_t::option_file_scan_t(const char* optFile, option_group_t* list)
: _fileName(optFile),
  _optList(list)
{
}

option_file_scan_t::~option_file_scan_t()
{
}

w_rc_t option_file_scan_t::scan(
        bool overRide, 
        ostream& err_stream, 
        bool exact,
        bool mismatch_ok
)
{
    w_rc_t        e;

    DBG(<<"scanning options file " << _fileName);

    ifstream f(_fileName);

    if (!f) {
        e = RC(fcOS);    
        DBG(<<"scan: open failure file " << _fileName);
        err_stream << "Could not open the option file " << _fileName;
        return e;
    }
    DBG(<<"scanning options file " << _fileName);

    option_stream_scan_t        ss(f, _optList);
    ss.setLabel(_fileName);

    return ss.scan(overRide, err_stream, exact, mismatch_ok);
}

