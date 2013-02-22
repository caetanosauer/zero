/*****************************************************************
 *  Title:      load_stub.c
 *  Sccsid:     @(#)load_stub.c	2.1.8.1
 *  Description:
 *              stub routines for:
 *          inline load of dss benchmark
 *          header creation for dss benchmark
 *
 *****************************************************************
 */

#include <stdio.h>
#include "workload/ssb/dbgen/config.h"
#include "workload/ssb/dbgen/dss.h"
#include "workload/ssb/dbgen/dsstypes.h"

ENTER_NAMESPACE(dbgenssb)

int 
close_direct(void)
{
    /* any post load cleanup goes here */
    return(0);
}

int 
prep_direct(void*)
{
    /* any preload prep goes here */
    return(0);
}

int 
hd_cust (FILE */*f*/)
{
    static int count = 0;

    if (! count++)
        printf("No header has been defined for the customer table\n");

    return(0);
}


int 
ld_cust (void */*cp*/, int /*mode*/)
{
    static int count = 0;

    if (! count++)
        printf("%s %s\n",
            "No load routine has been defined",
            "for the customer table");

    return(0);
}

int 
hd_part (FILE */*f*/)
{
    static int count = 0;

    if (! count++)
        printf("No header has been defined for the part table\n");

    return(0);
}

int 
ld_part (void */*pp*/, int /*mode*/)
{
    static int count = 0;

    if (! count++)
        printf("No load routine has been defined for the part table\n");

    return(0);
}

int 
ld_psupp (void */*pp*/, int /*mode*/)
{
    static int count = 0;

    if (! count++)
        printf("%s %s\n",
            "No load routine has been defined for the",
            "psupp table\n");

    return(0);

}


int 
hd_supp (FILE */*f*/)
{
    static int count = 0;

    if (! count++)
        printf("No header has been defined for the supplier table\n");

    return(0);
}

int 
ld_supp (void */*sp*/, int /*mode*/)
{
    static int count = 0;

    if (! count++)
        printf("%s %s\n",
            "No load routine has been defined",
            "for the supplier table\n");

    return(0);
}


int 
hd_order (FILE */*f*/)
{
    static int count = 0;

    if (! count++)
        printf("No header has been defined for the order table\n");

    return(0);
}

int 
ld_order (void */*p*/, int /*mode*/)
{
    static int count = 0;

    if (! count++)
        printf("%s %s\n",
            "No load routine has been defined",
            "for the order table");

    return(0);
}

int
ld_line (void */*p*/, int /*mode*/)
{
    static int count = 0;

    if (! count++)
        printf("%s %s\n",
            "No load routine has been defined",
            "for the line table");

    return(0);
}



int 
hd_psupp (FILE */*f*/)
{
    static int count = 0;

    if (! count++)
        printf("%s %s\n",
            "No header has been defined for the",
            "part supplier table");

    return(0);
}


int 
hd_line (FILE */*f*/)
{
    static int count = 0;

    if (! count++)
        printf("No header has been defined for the lineitem table\n");

    return(0);
}

int 
hd_nation (FILE */*f*/)
{
    static int count = 0;

    if (! count++)
        printf("No header has been defined for the nation table\n");

    return(0);
}

#ifdef SSBM
#else
int 
ld_nation (void */*cp*/, int /*mode*/)
{
    static int count = 0;

    if (! count++)
        printf("%s %s\n",
            "No load routine has been defined",
            "for the nation table");

    return(0);
}

int 
hd_region (FILE */*f*/)
{
    static int count = 0;

    if (! count++)
        printf("No header has been defined for the region table\n");

    return(0);
}

int 
ld_region (void */*cp*/, int /*mode*/)
{
    static int count = 0;

    if (! count++)
        printf("%s %s\n",
            "No load routine has been defined",
            "for the region table");

    return(0);
}

int 
ld_order_line (void */*p*/, int /*mode*/)
{
    order_t*pp=(order_t*)p;
    ld_order(pp, mode);
    ld_line (pp, mode);

    return(0);
}

int 
hd_order_line (FILE */*f*/)
{
    hd_order(f);
    hd_line (f);

    return(0);
}

int 
ld_part_psupp (void */*p*/, int /*mode*/)
{
  part_t *pp=(part_t *)p;
    ld_part(pp, mode);
    ld_psupp (pp, mode);

    return(0);
}

int 
hd_part_psupp (FILE */*f*/)
{
    hd_part(f);
    hd_psupp(f);

    return(0);
}
#endif

#ifdef SSBM
int
ld_date (void */*d*/, int /*mode*/)
{
    /*do nothing for now*/
    return(0);
}

#endif


EXIT_NAMESPACE(dbgenssb)





