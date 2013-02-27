/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-kits -- Benchmark implementations for Shore-MT
   
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

/** @file   confparser.cpp
 *
 *  @brief  Class for reading named values from configuration files
 *
 *  @note   See file tests/config_file_example.cpp for more examples.
 */

#include "util/confparser.h"


ConfigFile::ConfigFile( string filename, string delimiter,
                        string comment, string sentry )
    : myDelimiter(delimiter), myComment(comment), mySentry(sentry),
      _fname(filename)
{
    // Construct a ConfigFile, getting keys and values from given file  
    std::ifstream in( filename.c_str() );
  
    if( !in ) throw file_not_found( filename );   
    in >> (*this);
}


ConfigFile::ConfigFile()
    : myDelimiter( string(1,'=') ), myComment( string(1,'#') )
{
    // Construct a ConfigFile without a file; empty
}


// Write current configuration to the initial file
void ConfigFile::saveCurrentConfig()
{
    std::ofstream of(_fname.c_str());
    if (!of) throw file_not_found(_fname);
    of << (*this);
}


void ConfigFile::remove( const string& key )
{
    // Remove key and its value
    myContents.erase( myContents.find( key ) );
    return;
}


bool ConfigFile::keyExists( const string& key ) const
{
    // Indicate whether key is found
    mapci p = myContents.find( key );
    return ( p != myContents.end() );
}


/* static */
void ConfigFile::trim( string& s )
{
    // Remove leading and trailing whitespace
    static const char whitespace[] = " \n\t\v\r\f";
    s.erase( 0, s.find_first_not_of(whitespace) );
    s.erase( s.find_last_not_of(whitespace) + 1U );
}


std::ostream& operator<<( std::ostream& os, const ConfigFile& cf )
{
    // Save a ConfigFile to os
    for( ConfigFile::mapci p = cf.myContents.begin();
         p != cf.myContents.end();
         ++p )
        {
            os << p->first << " " << cf.myDelimiter << " ";
            os << p->second << std::endl;
        }
    return os;
}


std::istream& operator>>( std::istream& is, ConfigFile& cf )
{
    // Load a ConfigFile from is
    // Read in keys and values, keeping internal whitespace
    typedef string::size_type pos;
    const string& delim  = cf.myDelimiter;  // separator
    const string& comm   = cf.myComment;    // comment
    const string& sentry = cf.mySentry;     // end of file sentry
    const pos skip = delim.length();        // length of separator
	
    string nextline = "";  // might need to read ahead to see where value ends
	
    while( is || nextline.length() > 0 ) {
        // Read an entire line at a time
        string line;
        if( nextline.length() > 0 )
            {
                line = nextline;  // we read ahead; use it now
                nextline = "";
            }
        else
            {
                std::getline( is, line );
            }
		
        // Ignore comments
        line = line.substr( 0, line.find(comm) );
		
        // Check for end of file sentry
        if( sentry != "" && line.find(sentry) != string::npos ) return is;
		
        // Parse the line if it contains a delimiter
        pos delimPos = line.find( delim );
        if( delimPos < string::npos ) {
            // Extract the key
            string key = line.substr( 0, delimPos );
            line.replace( 0, delimPos+skip, "" );
			
            // See if value continues on the next line
            // Stop at blank line, next line with a key, end of stream,
            // or end of file sentry
            bool terminate = false;
            while( !terminate && is ) {
                std::getline( is, nextline );
                terminate = true;
				
                string nlcopy = nextline;
                ConfigFile::trim(nlcopy);
                if( nlcopy == "" ) continue;
				
                nextline = nextline.substr( 0, nextline.find(comm) );
                if( nextline.find(delim) != string::npos )
                    continue;
                if( sentry != "" && nextline.find(sentry) != string::npos )
                    continue;
				
                nlcopy = nextline;
                ConfigFile::trim(nlcopy);
                if( nlcopy != "" ) line += "\n";
                line += nextline;
                terminate = false;
            }
			
            // Store key and value
            ConfigFile::trim(key);
            ConfigFile::trim(line);
            cf.myContents[key] = line;  // overwrites if key is repeated
        }
    }
	
    return is;
}

