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

/** @file tpch_util.cpp
 *
 *  @brief Enums for type conversions in TPC-H database
 *
 *  @author: Nastaran Nikparto, Summer 2011
 *  @author Ippokratis Pandis (ipandis)
 *
 */

#include "workload/tpch/tpch_struct.h"
#include "workload/tpch/tpch_util.h"
#include <string>

ENTER_NAMESPACE(tpch);

tpch_l_shipmode str_to_shipmode(char* shipmode)
{
  tpch_l_shipmode ret;
  if ( !strcmp(shipmode,"REG_AIR") || !strcmp(shipmode,"REG AIR") )
    { ret=REG_AIR; }
  else if (!strcmp(shipmode,"AIR"))
    { ret=AIR;  }
  else if (!strcmp(shipmode,"RAIL"))
    { ret=RAIL;  }
  else if (!strcmp(shipmode,"TRUCK"))
    { ret=TRUCK; }
  else if (!strcmp(shipmode,"MAIL"))
    { ret=MAIL;  }
  else if (!strcmp(shipmode,"FOB"))
    { ret=FOB;  }
  else //if (strcmp(shipmode,"SHIP")==0)
    { ret=SHIP;  }

  return ret;
}

void shipmode_to_str(char* str, tpch_l_shipmode shipmode)
{

  if ( shipmode == REG_AIR )
    { strcpy(str,"REG AIR"); }
  else if ( shipmode == AIR )
    { strcpy(str,"AIR"); }
  else if ( shipmode == RAIL )
    { strcpy(str,"RAIL"); }
  else if ( shipmode == TRUCK )
    { strcpy(str,"TRUCK"); }
  else if ( shipmode == MAIL )
    { strcpy(str,"MAIL"); }
  else if ( shipmode == FOB )
    { strcpy(str,"FOB"); }
  else //if ( shipmode == SHIP )
    { strcpy(str,"SHIP"); }

}

void Brand_to_srt(char* str, int brand){

  if( brand/100 != 0)
    return;

  char num[3];
  sprintf(num, "%d", brand);

  strcpy(str, "Brand#");
  strcat(str, num);

  return;
}

int str_to_Brand(char* Brand ){

  char num[3];

  for(int i = 6; i < 9; i++ )
    num[ i - 6 ] = Brand[i];

  return atoi(num);
}

void types1_to_str(char*str , int s1){

  if( s1 == STANDARD)
    strcpy(str, "STANDARD");
  else if( s1 == SMALL)
    strcpy(str, "SMALL");
  else if( s1 == MEDIUM)
    strcpy(str, "MEDIUM");
  else if( s1 == LARGE)
    strcpy(str, "LARGE");
  else if( s1 == ECONOMY)
    strcpy(str,"ECONOMY");
  else if ( s1 == PROMO)
    strcpy(str, "PROMO");

}

tpch_p_type_s1 str_to_types1(char *s1){

  if(strcmp(s1, "STANDARD") == 0)
    return STANDARD;

  else if(strcmp(s1, "SMALL") == 0)
    return SMALL;

  else if(strcmp(s1, "MEDIUM") == 0)
    return MEDIUM;

  else if(strcmp(s1, "LARGE") == 0)
    return LARGE;

  else if(strcmp(s1, "ECONOMY") == 0)
    return ECONOMY;

  else if(strcmp(s1, "PROMO") == 0)
    return PROMO;

}

void types2_to_str(char* str , int s2){

  if( s2 == ANODIZED )
    strcpy(str, "ANODIZED");

  else if( s2 == BURNISHED )
    strcpy(str, "BURNISHED");

  else if( s2 == PLATED)
    strcpy(str, "PLATED");

  else if( s2 == POLISHED)
    strcpy(str, "POLISHED");

  else if( s2 == BRUSHED)
    strcpy(str, "BRUSHED");

  return;
}

tpch_p_type_s2 str_to_types2(char* s2){

  if( strcmp(s2, "ANODIZED") == 0)
    return ANODIZED;

  else if( strcmp(s2, "BURNISHED") ==0)
    return BURNISHED;

  else if( strcmp(s2, "PLATED") ==0)
    return PLATED;

  else if( strcmp(s2, "POLISHED") ==0)
    return POLISHED;

  else if( strcmp(s2, "BRUSHED") ==0)
    return BRUSHED;

}

void types3_to_str(char* str , int s3){

  if( s3 == TIN)
    strcpy( str, "TIN");

  else if ( s3 == NICKEL)
    strcpy( str, "NICKEL");

  else if ( s3 == BRASS)
    strcpy( str, "BRASS");

  else if ( s3 == STEEL)
    strcpy( str, "STEEL");

  else if ( s3 == COPPER)
    strcpy( str, "COPPER");
}

tpch_p_type_s3 str_to_types3(char* s3){

  if( strcmp(s3, "TIN") == 0)
    return TIN;

  else if( strcmp(s3, "NICKEL") == 0)
    return NICKEL;

  else if( strcmp(s3, "BRASS") == 0)
    return BRASS;

  else if( strcmp(s3, "STEEL") == 0)
    return STEEL;

  else if( strcmp(s3, "COPPER") == 0)
    return COPPER;
}

void type_to_str(tpch_p_type t, char* str){
  char types1[STRSIZE(25)];
  char types2[STRSIZE(25)];
  char types3[STRSIZE(25)];
  types1_to_str( types1, t.s1);
  types2_to_str( types2, t.s2);
  types3_to_str( types3, t.s3);

  str[0] = '\0';
  strcat(str, types1);
  strcat(str, " ");
  strcat(str, types2);
  strcat(str, " ");
  strcat(str, types3);

  return;
}

tpch_p_container_s1 str_to_containers1(char* s1){
  if( strcmp(s1, "SM") == 0)
    return SM;
  else if(strcmp(s1, "LG") == 0)
    return LG;
  else if(strcmp(s1, "MED") == 0)
    return MED;
  else if(strcmp(s1, "JUMBO") == 0)
    return JUMBO;
  else if(strcmp(s1, "WRAP") == 0)
    return WRAP;
}

void containers1_to_str( tpch_p_container_s1 s1, char* str){
  if( s1 == SM)
    strcpy(str, "SM");
  else if( s1 == LG)
    strcpy(str, "LG");
  else if( s1 == MED)
    strcpy(str, "MED");
  else if( s1 == JUMBO)
    strcpy(str, "JUMBO");
  else if( s1 == WRAP)
    strcpy(str, "WRAP");

}

tpch_p_container_s2 str_to_containers2(char* s2){

  if( strcmp(s2, "CASE") == 0)
    return CASE;
  else if ( strcmp(s2, "BOX") == 0)
    return BOX;
  else if ( strcmp(s2, "BAG") == 0)
    return BAG;
  else if ( strcmp(s2, "JAR") == 0)
    return JAR;
  else if ( strcmp(s2, "PKG") == 0)
    return PKG;
  else if ( strcmp(s2, "PACK") == 0)
    return PACK;
  else if ( strcmp(s2, "CAN") == 0)
    return CAN;
  else if ( strcmp(s2, "DRUM") == 0)
    return DRUM;

}

void containers2_to_str( tpch_p_container_s2 s2, char* str){
  if(s2 == CASE)
    strcpy(str, "CASE");
  else if(s2 == BOX)
    strcpy(str, "BOX");
  else if(s2 == BAG)
    strcpy(str, "BAG");
  else if(s2 == JAR)
    strcpy(str, "JAR");
  else if(s2 == PKG)
    strcpy(str, "PKG");
  else if(s2 == PACK)
    strcpy(str, "PACK");
  else if(s2 == CAN)
    strcpy(str, "CAN");
  else if(s2 == DRUM)
    strcpy(str, "DRUM");

}

void container_to_str(int p_container, char* str) {
	char s1[STRSIZE(6)];
	char s2[STRSIZE(4)];

	switch(p_container / 10) {
	case SM:
		strcpy(s1, "SM ");
		break;
	case LG:
		strcpy(s1, "LG ");
		break;
	case MED:
		strcpy(s1, "MED ");
		break;
	case JUMBO:
		strcpy(s1, "JUMBO ");
		break;
	case WRAP:
		strcpy(s1, "WRAP ");
		break;
	}

	switch(p_container % 10) {
	case CASE:
		strcpy(s2, "CASE");
		break;
	case BOX:
		strcpy(s2, "BOX");
		break;
	case BAG:
		strcpy(s2, "BAG");
		break;
	case JAR:
		strcpy(s2, "JAR");
		break;
	case PKG:
		strcpy(s2, "PKG");
		break;
	case PACK:
		strcpy(s2, "PACK");
		break;
	case CAN:
		strcpy(s2, "CAN");
		break;
	case DRUM:
		strcpy(s2, "DRUM");
		break;
	}
	str[0] = '\0';
	strcat(str, s1);
	strcat(str, s2);
}

void pname_to_str(int p, char* srt){

  switch(p){

  case almond:
    strcpy(srt,"almond");
    break;
  case antique:
     strcpy(srt,"antique");
    break;
  case aquamarine:
    strcpy(srt,"aquamarine");
    break;
  case azure:
    strcpy(srt,"azure");
    break;
  case beige:
    strcpy(srt,"beige");
    break;
  case bisque:
    strcpy(srt,"bisque");
    break;
  case black:
    strcpy(srt,"black");
    break;
  case blanched:
    strcpy(srt,"blanched");
    break;
  case blue:
    strcpy(srt,"blue");
    break;
  case blush:
    strcpy(srt,"blush");
    break;
  case brown:
    strcpy(srt,"brown");
    break;
  case burlywood:
    strcpy(srt,"burlywood");
    break;
  case burnished:
    strcpy(srt,"burnished");
    break;
  case chartreuse:
    strcpy(srt,"chartreuse");
    break;
  case chiffon:
    strcpy(srt,"chiffon");
    break;
  case chocolate:
    strcpy(srt,"chocolate");
    break;
  case coral:
    strcpy(srt,"coral");
    break;
  case cornflower:
    strcpy(srt,"cornflower");
    break;
  case cornsilk:
    strcpy(srt,"cornsilk");
    break;
  case cream:
    strcpy(srt,"cream");
    break;
  case cyan:
    strcpy(srt,"cyan");
    break;
  case dark:
    strcpy(srt,"dark");
    break;
  case deep:
    strcpy(srt,"deep");
    break;
  case dim:
    strcpy(srt,"dim");
    break;
  case dodger:
    strcpy(srt,"dodger");
    break;
  case drab:
    strcpy(srt,"drab");
    break;
  case firebrick:
    strcpy(srt,"firebrick");
    break;
  case floral:
    strcpy(srt,"floral");
    break;
  case forest:
    strcpy(srt,"forest");
    break;
  case frosted:
    strcpy(srt,"frosted");
    break;
  case gainsboro:
    strcpy(srt,"gainsboro");
    break;
  case ghost:
    strcpy(srt,"ghost");
    break;
  case goldenrod:
    strcpy(srt,"goldenrod");
    break;
  case green:
    strcpy(srt,"green");
    break;
  case grey:
    strcpy(srt,"grey");
    break;
  case honeydew:
    strcpy(srt,"honeydew");
    break;
  case hot:
    strcpy(srt,"hot");
    break;
  case indian:
    strcpy(srt,"indian");
    break;
  case ivory:
    strcpy(srt,"ivory");
    break;
  case khaki:
    strcpy(srt,"khaki");
    break;
  case lace:
    strcpy(srt,"lace");
    break;
  case lavender:
    strcpy(srt,"lavender");
    break;
  case lawn:
    strcpy(srt,"lawn");
    break;
  case lemon:
    strcpy(srt,"lemon");
    break;
  case light:
    strcpy(srt,"light");
    break;
  case lime:
    strcpy(srt,"lime");
    break;
  case linen:
    strcpy(srt,"linen");
    break;
  case magenta:
    strcpy(srt,"magenta");
    break;
  case maroon:
    strcpy(srt,"maroon");
    break;
  case medium:
    strcpy(srt,"medium");
    break;
  case metallic:
    strcpy(srt,"metallic");
    break;
  case midnight:
    strcpy(srt,"midnight");
    break;
  case mint:
    strcpy(srt,"mint");
    break;
  case misty:
    strcpy(srt,"misty");
    break;
  case moccasin:
    strcpy(srt,"moccasin");
    break;
  case navajo:
    strcpy(srt,"navajo");
    break;
  case navy:
    strcpy(srt,"navy");
    break;
  case olive:
    strcpy(srt,"olive");
    break;
  case orange:
    strcpy(srt,"orange");
    break;
  case orchid:
    strcpy(srt,"orchid");
    break;
  case pale:
    strcpy(srt,"pale");
    break;
  case papaya:
    strcpy(srt,"papaya");
    break;
  case peach:
    strcpy(srt,"peach");
    break;
  case peru:
    strcpy(srt,"peru");
    break;
  case pink:
    strcpy(srt,"pink");
    break;
  case plum:
    strcpy(srt,"plum");
    break;
  case powder:
    strcpy(srt,"powder");
    break;
  case puff:
    strcpy(srt,"puff");
    break;
  case purple:
    strcpy(srt,"purple");
    break;
  case red:
    strcpy(srt,"red");
    break;
  case rose:
    strcpy(srt,"rose");
    break;
  case rosy:
    strcpy(srt,"rosy");
    break;
  case royal:
    strcpy(srt,"royal");
    break;
  case saddle:
    strcpy(srt,"saddle");
    break;
  case salmon:
    strcpy(srt,"salmon");
    break;
  case sandy:
    strcpy(srt,"sandy");
    break;
  case seashell:
    strcpy(srt,"seashell");
    break;
  case sienna:
    strcpy(srt,"sienna");
    break;
  case sky:
    strcpy(srt,"sky");
    break;
  case slate:
    strcpy(srt,"slate");
    break;
  case smoke:
    strcpy(srt,"smoke");
    break;
  case snow:
    strcpy(srt,"snow");
    break;
  case spring:
    strcpy(srt,"spring");
    break;
  case steel:
    strcpy(srt,"steel");
    break;
  case tan:
    strcpy(srt,"tan");
    break;
  case thistle:
    strcpy(srt,"thistle");
    break;
  case tomato:
    strcpy(srt,"tomato");
    break;
  case turquoise:
    strcpy(srt,"turquoise");
    break;
  case violet:
    strcpy(srt,"violet");
    break;
  case wheat:
    strcpy(srt,"wheat");
    break;
  case white:
    strcpy(srt,"white");
    break;
  case yellow:
    strcpy(srt,"yellow");
    break;
  }
}

tpch_o_priority str_to_priority(char * str ){

  tpch_o_priority ret;

  if( !strcmp(str, "1-URGENT") )
    ret = O_URGENT;
  else if( !strcmp(str, "2-HIGH"))
    ret = O_HIGH;
  else if( !strcmp(str, "3-MEDIUM"))
    ret = O_MEDIUM;
  else if( !strcmp(str, "4-NOT SPECIFIED"))
    ret = O_NOT_SPECIFIED;
  else if( !strcmp(str, "5-LOW"))
    ret = O_LOW;

  return ret;

}

void segment_to_str(char* str, int seg){
    switch(seg) {
        case AUTOMOBILE:
            strcpy(str, "AUTOMOBILE");
            break;
        case BUILDING:
            strcpy(str, "BUILDING");
            break;
        case FURNITURE:
            strcpy(str, "FURNITURE");
            break;
        case MACHINERY:
            strcpy(str, "MACHINERY");
            break;
        case HOUSEHOLD:
            strcpy(str, "HOUSEHOLD");
            break;
    }
}

tpch_c_segment str_to_segment(char *str){

  tpch_c_segment ret;

  if( !strcmp(str, "AUTOMOBILE"))
    ret = AUTOMOBILE;
  else if( !strcmp(str, "BUILDING"))
    ret = BUILDING;
   else if( !strcmp(str, "FURNITURE"))
    ret = FURNITURE;
  else if( !strcmp(str, "MACHINERY"))
    ret = MACHINERY;
  else if( !strcmp(str, "HOUSEHOLD"))
    ret = HOUSEHOLD;

  return ret;
}

tpch_n_name str_to_nation(char* str ){

  if(strcmp( "ALGERIA", str) == 0 )
    return ALGERIA;
  else if(strcmp( "ARGENTINA", str) == 0 )
    return ARGENTINA;
  else if(strcmp( "BRAZIL", str) == 0 )
    return BRAZIL;
  else if(strcmp( "CANADA", str) == 0 )
    return CANADA;
  else if(strcmp( "EGYPT", str) == 0 )
    return EGYPT;
  else if(strcmp( "ETHIOPIA", str) == 0 )
    return ETHIOPIA;
  else if(strcmp( "FRANCE", str) == 0 )
    return FRANCE;
  else if(strcmp( "GERMANY", str) == 0 )
    return GERMANY;
  else if(strcmp( "INDIA", str) == 0 )
    return INDIA;
  else if(strcmp( "INDONESIA", str) == 0 )
    return INDONESIA;
  else if(strcmp( "IRAN", str) == 0 )
    return IRAN;
  else if(strcmp( "IRAQ", str) == 0 )
    return IRAQ;
  else if(strcmp( "JAPAN", str) == 0 )
    return JAPAN;
  else if(strcmp( "JORDAN",  str) == 0 )
    return JORDAN;
  else if(strcmp( "KENYA", str) == 0 )
    return KENYA;
  else if(strcmp( "MOROCCO", str) == 0 )
    return MOROCCO;
  else if(strcmp( "MOZAMBIQUE", str) == 0 )
    return MOZAMBIQUE;
  else if(strcmp( "PERU", str) == 0 )
    return PERU;
  else if(strcmp( "CHINA", str) == 0 )
    return CHINA;
  else if(strcmp( "ROMANIA", str) == 0 )
    return ROMANIA;
  else if(strcmp( "SAUDI ARABIA", str) == 0 )
    return SAUDI_ARABIA;
  else if(strcmp( "VIETNAM", str) == 0 )
    return VIETNAM;
  else if(strcmp( "RUSSIA", str) == 0 )
    return RUSSIA;
  else if(strcmp( "UNITED KINGDOM", str) == 0 )
    return UNITED_KINGDOM;
  else if(strcmp( "UNITED STATES", str) == 0 )
    return UNITED_STATES;

}

void nation_to_str( int n_nation , char* str ){

  switch(n_nation){
  case ALGERIA:
    strcpy(str,"ALGERIA");
    break;

  case ARGENTINA:
    strcpy(str,"ARGENTINA");
    break;

  case BRAZIL:
    strcpy(str,"BRAZIL");
    break;

  case CANADA:
    strcpy(str,"CANADA");
    break;

  case EGYPT:
    strcpy(str,"EGYPT");
    break;

  case ETHIOPIA:
    strcpy(str,"ETHIOPIA");
    break;

  case FRANCE:
    strcpy(str,"FRANCE");
    break;

  case GERMANY:
    strcpy(str,"GERMANY");
    break;

  case INDIA:
    strcpy(str,"INDIA");
    break;

  case INDONESIA:
    strcpy(str,"INDONESIA");
    break;

  case IRAN:
    strcpy(str,"IRAN");
    break;

  case IRAQ:
    strcpy(str,"IRAQ");
    break;

  case JAPAN:
    strcpy(str,"JAPAN");
    break;

  case JORDAN:
    strcpy(str,"JORDAN");
    break;

  case KENYA:
    strcpy(str,"KENYA");
    break;

  case MOROCCO:
    strcpy(str,"MOROCCO");
    break;

  case MOZAMBIQUE:
    strcpy(str,"MOZAMBIQUE");
    break;

  case PERU:
    strcpy(str,"PERU");
    break;

  case CHINA:
    strcpy(str,"CHINA");
    break;

  case ROMANIA:
    strcpy(str,"ROMANIA");
    break;

  case SAUDI_ARABIA:
    strcpy(str,"SAUDI ARABIA");
    break;

  case VIETNAM:
    strcpy(str,"VIETNAM");
    break;

  case RUSSIA:
    strcpy(str,"RUSSIA");
    break;

  case UNITED_KINGDOM:
    strcpy(str,"UNITED KINGDOM");
    break;

  case UNITED_STATES:
    strcpy(str,"UNITED STATES");
    break;
  }

  return;
}

tpch_r_name str_to_region(char* str) {
    if(strcmp( "AFRICA", str) == 0 )
    return AFRICA;
  else if(strcmp( "AMERICA", str) == 0 )
    return AMERICA;
  else if(strcmp( "ASIA", str) == 0 )
    return ASIA;
  else if(strcmp( "EUROPE", str) == 0 )
    return EUROPE;
  else if(strcmp( "MIDDLE EAST", str) == 0 )
    return MIDDLE_EAST;
}

void region_to_str(int r_name, char* str) {
    switch(r_name){
  case AFRICA:
    strcpy(str,"AFRICA");
    break;

  case AMERICA:
    strcpy(str,"AMERICA");
    break;

  case ASIA:
    strcpy(str,"ASIA");
    break;

  case EUROPE:
    strcpy(str,"EUROPE");
    break;

  case MIDDLE_EAST:
    strcpy(str,"MIDDLE EAST");
    break;
    }
}

EXIT_NAMESPACE(tpch);
