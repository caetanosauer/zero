
#include "workload/ssb/ssb_struct.h"
#include "workload/ssb/ssb_util.h"
#include <string>

//enum ssb_l_shipmode{REG_AIR, AIR};
ENTER_NAMESPACE(ssb);

ssb_l_shipmode str_to_shipmode(char* shipmode)
{
  ssb_l_shipmode ret;
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

void shipmode_to_str(char* str, ssb_l_shipmode shipmode)
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

void get_nation(char* nation_name, ssb_nation nation_id)
{
  if (nation_id==ALGERIA)
    { strcpy(nation_name,"ALGERIA"); }
  else if (nation_id==ARGENTINA)
    { strcpy(nation_name,"ARGENTINA"); }
  else if (nation_id==BRAZIL)
    { strcpy(nation_name,"BRAZIL"); }
  else if (nation_id==CANADA)
    { strcpy(nation_name,"CANADA"); }
  else if (nation_id==CHINA)
    { strcpy(nation_name,"CHINA"); }
  else if (nation_id==EGYPT)
    { strcpy(nation_name,"EGYPT"); }
  else if (nation_id==ETHIOPIA)
    { strcpy(nation_name,"ETHIOPIA"); }
  else if (nation_id==FRANCE)
    { strcpy(nation_name,"FRANCE"); }
  else if (nation_id==GERMANY)
    { strcpy(nation_name,"GERMANY"); }
  else if (nation_id==INDIA)
    { strcpy(nation_name,"INDIA"); }
  else if (nation_id==INDONESIA)
    { strcpy(nation_name,"INDONESIA"); }
  else if (nation_id==IRAN)
    { strcpy(nation_name,"IRAN"); }
  else if (nation_id==IRAQ)
    { strcpy(nation_name,"IRAQ"); }
  else if (nation_id==JAPAN)
    { strcpy(nation_name,"JAPAN"); }
  else if (nation_id==JORDAN)
    { strcpy(nation_name,"JORDAN"); }
  else if (nation_id==KENYA)
    { strcpy(nation_name,"KENYA"); }
  else if (nation_id==MOROCCO)
    { strcpy(nation_name,"MOROCCO"); }
  else if (nation_id==MOZAMBIQUE)
    { strcpy(nation_name,"MOZAMBIQUE"); }
  else if (nation_id==PERU)
    { strcpy(nation_name,"PERU"); }
  else if (nation_id==ROMANIA)
    { strcpy(nation_name,"ROMANIA"); }
  else if (nation_id==RUSSIA)
    { strcpy(nation_name,"RUSSIA"); }
  else if (nation_id==SAUDI_ARABIA)
    { strcpy(nation_name,"SAUDI ARABIA"); }
  else if (nation_id==UNITED_KINGDOM)
    { strcpy(nation_name,"UNITED KINGDOM"); }
  else if (nation_id==UNITED_STATES)
    { strcpy(nation_name,"UNITED_STATES"); }
  else if (nation_id==VIETNAM)
    { strcpy(nation_name,"VIETNAM"); }

}


EXIT_NAMESPACE(ssb);
