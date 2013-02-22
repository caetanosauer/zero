/*
 * $Id: permute.c,v 1.3 2007/01/04 21:29:21 jms Exp $
 *
 * Revision History
 * ===================
 * $Log: permute.c,v $
 * Revision 1.3  2007/01/04 21:29:21  jms
 * Porting changes uncovered as part of move to VS2005. No impact on data set
 *
 * Revision 1.2  2005/01/03 20:08:59  jms
 * change line terminations
 *
 * Revision 1.1.1.1  2004/11/24 23:31:47  jms
 * re-establish external server
 *
 * Revision 1.1.1.1  2003/08/07 17:58:34  jms
 * recreation after CVS crash
 *
 * Revision 1.2  2003/08/07 17:58:34  jms
 * Convery RNG to 64bit space as preparation for new large scale RNG
 *
 * Revision 1.1.1.1  2003/04/03 18:54:21  jms
 * initial checkin
 *
 *
 */
/*
 * permute.c -- a permutation generator for the query 
 *              sequences in TPC-H and TPC-R
 */

#ifdef TEST
#define DECLARER
#endif

#include "workload/tpch/dbgen/config.h"
#include "workload/tpch/dbgen/dss.h"

#ifdef TEST
#include <stdlib.h>

#include <unistd.h>
#include <sys/wait.h>
#include <stdio.h>				/* */
#include <limits.h>
#include <math.h>
#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <errno.h>
#ifdef HP
#include <strings.h>
#endif

#endif


ENTER_NAMESPACE(dbgentpch);


DSS_HUGE NextRand(DSS_HUGE seed);
long *permute(long *set, int cnt, long stream, DSS_HUGE& source, long* cs);
long *permute_dist(distribution *d, long stream, DSS_HUGE& source, distribution* cd);
long seed;
const char *eol[2] = {" ", "},"};
extern seed_t Seed[];
#ifdef TEST
tdef tdefs = { NULL };
#endif


#define MAX_QUERY	22
#define ITERATIONS	1000
#define UNSET	0


// IP: making a thread-safe version of permute
// The source, and the set are now passed as parameters
long *
permute(long *a, int c, long s, DSS_HUGE& source, long* cset)
{
  int i;
  long temp;
    
  if (a != (long *)NULL) {
    for (i=0; i < c; i++) {
      *(a + i) = i;
    }

    for (i=0; i < c; i++) {
      RANDOM(source, 0L, (long)(c - 1), s);
      temp = *(a + source);
      *(a + source) = *(a + i) ;
      *(a + i) = temp;
      source = 0;
    }
  }
  else {
    source += 1;
  }
  
  if (source >= c) {
    source -= c;
  }

  return (cset + source);
}


long *
permute_dist(distribution *d, long stream, 
             DSS_HUGE& source, distribution* cd)
{
  static bool bInit = false;
  static distribution *dist = NULL;
	
  if (d != NULL) {
    if (d->permute == (long *)NULL) {
      d->permute = (long *)malloc(sizeof(long) * DIST_SIZE(d));
      MALLOC_CHECK(d->permute);
      //IP: permute does the same 
      //for (int i=0; i < DIST_SIZE(d); i++) {
      //   *(d->permute + i) = i;
      //}
    }

    while (!bInit) {
      dist = d;
      bInit = true;
    }
    // IP: This will not work in general, but afaict from the code
    //     this function (permute_dist) is called only by mk_part
    //     so 'dist' will never have to change its value. 
    //     This assertion ensures that 'dist' will have a single 
    //     value.    
    assert (dist == d);
    return (permute(dist->permute, DIST_SIZE(dist), stream, 
                    source, cd->permute));
  }
		
  if (dist != NULL) {
    return (permute(NULL, DIST_SIZE(dist), stream, source, cd->permute));
  }
  else {
    INTERNAL_ERROR("Bad call to permute_dist");	
  }

  return (NULL);
}



EXIT_NAMESPACE(dbgentpch);



#ifdef TEST

main(int ac, char *av[])
{
  long *sequence, 
    i,
    j,
    streams = UNSET,
    *a;
  char sep;
  int index = 0;
	
  set_seeds = 0;
  sequence = (long *)malloc(MAX_QUERY * sizeof(long));
  a = sequence;
  for (i=0; i < MAX_QUERY; i++)
    *(sequence + i) = i;
  if (ac < 3) 
    goto usage;
  Seed[0].value = (long)atoi(av[1]);
  streams = atoi(av[2]);
  if (Seed[0].value == UNSET || streams == UNSET) 
    goto usage;
	
  index = 0;
  printf("long permutation[%d][%d] = {\n", streams, MAX_QUERY);
  for (j=0; j < streams; j++) {
    sep = '{';
    printf("%s\n", eol[index]);
    for (i=0; i < MAX_QUERY; i++)
      {
        printf("%c%2d", sep, *permute(a, MAX_QUERY, 0) + 1);
        a = (long *)NULL;
        sep = ',';
      }
    a = sequence;
    index=1;
  }

  printf("}\n};\n");
  return(0);
	
 usage:
  printf("Usage: %s <seed> <streams>\n",av[0]);
  printf("  uses <seed> to start the generation of <streams> permutations of [1..%d]\n", MAX_QUERY);
  return(-1);
	
}

#endif /* TEST */
