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

/** @file tpch_util.h
 *
 *  @brief Enums for type conversions in TPC-H database
 *
 *  @author: Nastaran Nikparto, Summer 2011
 *  @author Ippokratis Pandis (ipandis)
 *
 */

#ifndef __TPCH_UTIL_H
#define __TPCH_UTIL_H

#include "workload/tpch/tpch_struct.h"

ENTER_NAMESPACE(tpch);

tpch_l_shipmode str_to_shipmode(char* shipmode);
void shipmode_to_str(char* str, tpch_l_shipmode shipmode);

void Brand_to_srt(char* str, int brand);
int str_to_Brand(char* Brand );

void types1_to_str(char*str , int s1);
tpch_p_type_s1 str_to_types1(char *s1);


void types2_to_str(char* str , int s2);
tpch_p_type_s2 str_to_types2(char* s2);

void types3_to_str(char* str , int s3);
tpch_p_type_s3 str_to_types3(char* s3);

void type_to_str(tpch_p_type t, char* str);

tpch_p_container_s1 str_to_containers1(char* s1);
void containers1_to_str( tpch_p_container_s1 s1, char* str);

tpch_p_container_s2 str_to_containers2(char* s2);
void containers2_to_str( tpch_p_container_s2 s2, char* str);

void container_to_str(int p_container, char* str);

void pname_to_str(int p, char* srt);

tpch_o_priority str_to_priority(char * str );

void segment_to_str(char* str, int seg);
tpch_c_segment str_to_segment(char *str);

tpch_n_name str_to_nation(char* str );
void nation_to_str( int n_nation , char* str );

tpch_r_name str_to_region(char* str);
void region_to_str(int r_name, char* str);

EXIT_NAMESPACE(tpch);
#endif
