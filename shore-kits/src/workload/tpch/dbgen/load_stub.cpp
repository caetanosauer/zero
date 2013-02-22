/*
 * $Id: load_stub.c,v 1.2 2005/01/03 20:08:58 jms Exp $
 *
 * Revision History
 * ===================
 * $Log: load_stub.c,v $
 * Revision 1.2  2005/01/03 20:08:58  jms
 * change line terminations
 *
 * Revision 1.1.1.1  2004/11/24 23:31:46  jms
 * re-establish external server
 *
 * Revision 1.1.1.1  2003/04/03 18:54:21  jms
 * recreation after CVS crash
 *
 * Revision 1.1.1.1  2003/04/03 18:54:21  jms
 * initial checkin
 *
 *
 */
/*****************************************************************
 *  Title:      load_stub.c
 *  Description:
 *              stub routines for:
 *          inline load of dss benchmark
 *          header creation for dss benchmark
 *
 *****************************************************************
 */

#include <stdio.h>
#include "workload/tpch/dbgen/config.h"
#include "workload/tpch/dbgen/dss.h"
#include "workload/tpch/dbgen/dsstypes.h"


ENTER_NAMESPACE(dbgentpch);



int 
close_direct(void)
{
  /* any post load cleanup goes here */
  return(0);
}

int 
prep_direct(char*)
{
  /* any preload prep goes here */
  return(0);
}

int hd_cust (FILE* /* f */)
{
  static int count = 0;

  if (! count++)
    printf("No header has been defined for the customer table\n");

  return(0);
}

int ld_cust (void* /* cp */, int /* mode */)
{
  
  static int count = 0;

  if (! count++)
    printf("%s %s\n",
           "No load routine has been defined",
           "for the customer table");

  return(0);
}

int hd_part (FILE* /* f */)
{
  static int count = 0;

  if (! count++)
    printf("No header has been defined for the part table\n");

  return(0);
}

int ld_part (void* /* vpp */, int /* mode */)
{
  //part_t* pp = (part_t*)vpp; // IP: Unused
  static int count = 0;

  if (! count++)
    printf("No load routine has been defined for the part table\n");

  return(0);
}

int ld_psupp (void* /* vpp */, int /* mode */)
{
  //part_t* pp = (part_t*)vpp; // IP: Unused
  static int count = 0;

  if (! count++)
    printf("%s %s\n",
           "No load routine has been defined for the",
           "psupp table\n");

  return(0);

}


int hd_supp (FILE* /* f */)
{
  static int count = 0;

  if (! count++)
    printf("No header has been defined for the supplier table\n");

  return(0);
}

int ld_supp (void* /* vsp */, int /* mode */)
{
  //supplier_t* sp = (supplier_t*)vsp; // IP: Unused
  static int count = 0;

  if (! count++)
    printf("%s %s\n",
           "No load routine has been defined",
           "for the supplier table\n");

  return(0);
}


int hd_order (FILE* /* f */)
{
  static int count = 0;

  if (! count++)
    printf("No header has been defined for the order table\n");

  return(0);
}

int ld_order (void* /* vp */, int /* mode */)
{
  //order_t* p = (order_t*)vp; // IP: Unused
  static int count = 0;

  if (! count++)
    printf("%s %s\n",
           "No load routine has been defined",
           "for the order table");

  return(0);
}

int ld_line (void* /* vp */, int /* mode */)
{
  // order_t* p = (order_t*)vp; // IP: Unused
  static int count = 0;

  if (! count++)
    printf("%s %s\n",
           "No load routine has been defined",
           "for the line table");

  return(0);
}



int hd_psupp (FILE* /* f */)
{
  static int count = 0;

  if (! count++)
    printf("%s %s\n",
           "No header has been defined for the",
           "part supplier table");

  return(0);
}


int hd_line (FILE* /* f */)
{
  static int count = 0;

  if (! count++)
    printf("No header has been defined for the lineitem table\n");

  return(0);
}

int hd_nation (FILE* /* f */)
{
  static int count = 0;

  if (! count++)
    printf("No header has been defined for the nation table\n");

  return(0);
}

int ld_nation (void* /* vcp */, int /* mode */)
{
  //code_t* cp = (code_t*)vcp; // IP: Unused
  static int count = 0;

  if (! count++)
    printf("%s %s\n",
           "No load routine has been defined",
           "for the nation table");

  return(0);
}

int hd_region (FILE* /* f */)
{
  static int count = 0;

  if (! count++)
    printf("No header has been defined for the region table\n");

  return(0);
}

int ld_region (void* /* vcp */, int /* mode */)
{
  // code_t* cp = (code_t*)vcp; // IP: Unused
  static int count = 0;

  if (! count++)
    printf("%s %s\n",
           "No load routine has been defined",
           "for the region table");

  return(0);
}

int 
ld_order_line (void *vp, int mode)
{
  order_t* p = (order_t*)vp;

  ld_order(p, mode);
  ld_line (p, mode);

  return(0);
}

int 
hd_order_line (FILE *f)
{
  hd_order(f);
  hd_line (f);

  return(0);
}

int 
ld_part_psupp (void *vp, int mode)
{
  part_t* p = (part_t*)vp;

  ld_part(p, mode);
  ld_psupp (p, mode);

  return(0);
}

int 
hd_part_psupp (FILE *f)
{
  hd_part(f);
  hd_psupp(f);

  return(0);
}


EXIT_NAMESPACE(dbgentpch);

