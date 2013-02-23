/*
 * $Id: driver.c,v 1.7 2008/09/24 22:35:21 jms Exp $
 *
 * Revision History
 * ===================
 * $Log: driver.c,v $
 * Revision 1.7  2008/09/24 22:35:21  jms
 * remove build number header
 *
 * Revision 1.6  2008/09/24 22:30:29  jms
 * remove build number from default header
 *
 * Revision 1.5  2008/03/21 17:38:39  jms
 * changes for 2.6.3
 *
 * Revision 1.4  2006/04/26 23:01:10  jms
 * address update generation problems
 *
 * Revision 1.3  2005/10/28 02:54:35  jms
 * add release.h changes
 *
 * Revision 1.2  2005/01/03 20:08:58  jms
 * change line terminations
 *
 * Revision 1.1.1.1  2004/11/24 23:31:46  jms
 * re-establish external server
 *
 * Revision 1.5  2004/04/07 20:17:29  jms
 * bug #58 (join fails between order/lineitem)
 *
 * Revision 1.4  2004/02/18 16:26:49  jms
 * 32/64 bit changes for overflow handling needed additional changes when ported back to windows
 *
 * Revision 1.3  2004/01/22 05:49:29  jms
 * AIX porting (AIX 5.1)
 *
 * Revision 1.2  2004/01/22 03:54:12  jms
 * 64 bit support changes for customer address
 *
 * Revision 1.1.1.1  2003/08/08 21:50:33  jms
 * recreation after CVS crash
 *
 * Revision 1.3  2003/08/08 21:35:26  jms
 * first integration of rng64 for o_custkey and l_partkey
 *
 * Revision 1.2  2003/08/07 17:58:34  jms
 * Convery RNG to 64bit space as preparation for new large scale RNG
 *
 * Revision 1.1.1.1  2003/04/03 18:54:21  jms
 * initial checkin
 *
 *
 */
/* main driver for dss benchmark */

#define DECLARER				/* EXTERN references get defined here */
#define NO_FUNC (int (*) ()) NULL	/* to clean up tdefs */
#define NO_LFUNC (long (*) (int, DSS_HUGE)) NULL		/* to clean up tdefs */

#include "workload/tpch/dbgen/config.h"
#include "workload/tpch/dbgen/release.h"
#include <stdlib.h>
#if (defined(_POSIX_)||!defined(WIN32))		/* Change for Windows NT */
#include <unistd.h>
#include <sys/wait.h>
#endif /* WIN32 */
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

#include "workload/tpch/dbgen/dss.h"
#include "workload/tpch/dbgen/dsstypes.h"


ENTER_NAMESPACE(dbgentpch);



/*
 * Function prototypes
 */
void	usage (void);
int	prep_direct (char *);
int	close_direct (void);
void	kill_load (void);
void	gen_tbl (int tnum, DSS_HUGE start, DSS_HUGE count, long upd_num);
int	pr_drange (int tbl, DSS_HUGE min, DSS_HUGE cnt, long num);
int	set_files (int t, int pload);
int	partial (int, int);


extern int optind, opterr;
extern char *optarg;
DSS_HUGE rowcnt = 0, minrow = 0;
long upd_num = 0;
double flt_scale;
#if (defined(WIN32)&&!defined(_POSIX_))
char *spawn_args[25];
#endif
#ifdef RNG_TEST
extern seed_t Seed[];
#endif


/*
 * general table descriptions. See dss.h for details on structure
 * NOTE: tables with no scaling info are scaled according to
 * another table
 *
 *
 * the following is based on the tdef structure defined in dss.h as:
 * typedef struct
 * {
 * char     *name;            -- name of the table; 
 *                               flat file output in <name>.tbl
 * long      base;            -- base scale rowcount of table; 
 *                               0 if derived
 * int       (*header) ();    -- function to prep output
 * int       (*loader[2]) (); -- functions to present output
 * long      (*gen_seed) ();  -- functions to seed the RNG
 * int       (*verify) ();    -- function to verfiy the data set without building it
 * int       child;           -- non-zero if there is an associated detail table
 * unsigned long vtotal;      -- "checksum" total 
 * }         tdef;
 *
 */

/*
 * flat file print functions; used with -F(lat) option
 */
int pr_cust       (void * c, int mode);
int pr_line       (void * o, int mode);
int pr_order      (void * o, int mode);
int pr_part       (void * p, int mode);
int pr_psupp      (void * p, int mode);
int pr_supp       (void * s, int mode);
int pr_order_line (void * o, int mode);
int pr_part_psupp (void * p, int mode);
int pr_nation     (void * c, int mode);
int pr_region     (void * c, int mode);

/*
 * inline load functions; used with -D(irect) option
 */
int ld_cust       (void * c, int mode);
int ld_line       (void * o, int mode);
int ld_order      (void * o, int mode);
int ld_part       (void * p, int mode);
int ld_psupp      (void * p, int mode);
int ld_supp       (void * s, int mode);
int ld_order_line (void * o, int mode);
int ld_part_psupp (void * p, int mode);
int ld_nation     (void * c, int mode);
int ld_region     (void * c, int mode);

/*
 * seed generation functions; used with '-O s' option
 */
long sd_cust       (int child, DSS_HUGE skip_count);
long sd_line       (int child, DSS_HUGE skip_count);
long sd_order      (int child, DSS_HUGE skip_count);
long sd_part       (int child, DSS_HUGE skip_count);
long sd_psupp      (int child, DSS_HUGE skip_count);
long sd_supp       (int child, DSS_HUGE skip_count);
long sd_order_line (int child, DSS_HUGE skip_count);
long sd_part_psupp (int child, DSS_HUGE skip_count);

/*
 * header output functions); used with -h(eader) option
 */
int hd_cust       (FILE * f);
int hd_line       (FILE * f);
int hd_order      (FILE * f);
int hd_part       (FILE * f);
int hd_psupp      (FILE * f);
int hd_supp       (FILE * f);
int hd_order_line (FILE * f);
int hd_part_psupp (FILE * f);
int hd_nation     (FILE * f);
int hd_region     (FILE * f);

/*
 * data verfication functions; used with -O v option
 */
int vrf_cust       (void * c, int mode);
int vrf_line       (void * o, int mode);
int vrf_order      (void * o, int mode);
int vrf_part       (void * p, int mode);
int vrf_psupp      (void * p, int mode);
int vrf_supp       (void * s, int mode);
int vrf_order_line (void * o, int mode);
int vrf_part_psupp (void * p, int mode);
int vrf_nation     (void * c, int mode);
int vrf_region     (void * c, int mode);


tdef tdefs[] =
  {
    {"part.tbl", "part table", 200000, 
     hd_part,
     {pr_part, 
      ld_part}, 
     sd_part, 
     vrf_part, 
     PSUPP, 
     0},
    {"partsupp.tbl", "partsupplier table", 200000, hd_psupp,
     {pr_psupp, ld_psupp}, sd_psupp, vrf_psupp, NONE, 0},
    {"supplier.tbl", "suppliers table", 10000, hd_supp,
     {pr_supp, ld_supp}, sd_supp, vrf_supp, NONE, 0},
    {"customer.tbl", "customers table", 150000, hd_cust,
     {pr_cust, ld_cust}, sd_cust, vrf_cust, NONE, 0},
    {"orders.tbl", "order table", 150000, hd_order,
     {pr_order, ld_order}, sd_order, vrf_order, LINE, 0},
    {"lineitem.tbl", "lineitem table", 150000, hd_line,
     {pr_line, ld_line}, sd_line, vrf_line, NONE, 0},
    {"orders.tbl", "orders/lineitem tables", 150000, hd_order_line,
     {pr_order_line, ld_order_line}, sd_order, vrf_order_line, LINE, 0},
    {"part.tbl", "part/partsupplier tables", 200000, hd_part_psupp,
     {pr_part_psupp, ld_part_psupp}, sd_part, vrf_part_psupp, PSUPP, 0},
    {"nation.tbl", "nation table", NATIONS_MAX, hd_nation,
     {pr_nation, 
      ld_nation}, 
     NO_LFUNC, 
     vrf_nation, 
     NONE, 0},
    {"region.tbl", "region table", NATIONS_MAX, hd_region,
     {pr_region, ld_region}, NO_LFUNC, vrf_region, NONE, 0},
  };

int *pids;


/*
 * routines to handle the graceful cleanup of multi-process loads
 */

void
stop_proc (int /* signum */)
{
  exit (0);
}

void
kill_load (void)
{
  int i;
	
#if !defined(U2200) && !defined(DOS)
  for (i = 0; i < children; i++)
    if (pids[i])
      KILL (pids[i]);
#endif /* !U2200 && !DOS */
  return;
}

/*
 * re-set default output file names 
 */
int
set_files (int i, int pload)
{
  char line[80], *new_name;
	
  if (table & (1 << i))
  child_table:
    {
      if (pload != -1)
        sprintf (line, "%s.%d", tdefs[i].name, pload);
      else
        {
          printf ("Enter new destination for %s data: ",
                  tdefs[i].name);
          if (fgets (line, sizeof (line), stdin) == NULL)
            return (-1);;
          if ((new_name = strchr (line, '\n')) != NULL)
            *new_name = '\0';
          if (strlen (line) == 0)
            return (0);
        }
      new_name = (char *) malloc (strlen (line) + 1);
      MALLOC_CHECK (new_name);
      strcpy (new_name, line);
      tdefs[i].name = new_name;
      if (tdefs[i].child != NONE)
        {
          i = tdefs[i].child;
          tdefs[i].child = NONE;
          goto child_table;
        }
    }
	
  return (0);
}



/*
 * read the distributions needed in the benchamrk
 */
void
load_dists (void)
{
  read_dist (env_config (DIST_TAG, DIST_DFLT), "p_cntr", &p_cntr_set);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "colors", &colors);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "p_types", &p_types_set);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "nations", &nations);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "regions", &regions);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "o_oprio",
             &o_priority_set);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "instruct",
             &l_instruct_set);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "smode", &l_smode_set);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "category",
             &l_category_set);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "rflag", &l_rflag_set);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "msegmnt", &c_mseg_set);

  /* load the distributions that contain text generation */
  read_dist (env_config (DIST_TAG, DIST_DFLT), "nouns", &nouns);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "verbs", &verbs);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "adjectives", &adjectives);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "adverbs", &adverbs);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "auxillaries", &auxillaries);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "terminators", &terminators);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "articles", &articles);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "prepositions", &prepositions);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "grammar", &grammar);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "np", &np);
  read_dist (env_config (DIST_TAG, DIST_DFLT), "vp", &vp);
	
}

/*
 * generate a particular table
 */
void
gen_tbl (int tnum, DSS_HUGE start, DSS_HUGE count, long upd_num)
{
  static order_t o;
  supplier_t supp;
  customer_t cust;
  part_t part;
  code_t code;
  static int completed = 0;
  DSS_HUGE i;

  DSS_HUGE rows_per_segment=0;
  DSS_HUGE rows_this_segment=-1;
  DSS_HUGE residual_rows=0;

  if (insert_segments)
    {
      rows_per_segment = count / insert_segments;
      residual_rows = count - (rows_per_segment * insert_segments);
    }

  for (i = start; count; count--, i++)
    {
      LIFENOISE (1000, i);
      row_start(tnum);

      switch (tnum)
        {
        case LINE:
        case ORDER:
        case ORDER_LINE: 
          mk_order (i, &o, upd_num % 10000);

          if (insert_segments  && (upd_num > 0)) {
            if((upd_num / 10000) < residual_rows)
              {
                if((++rows_this_segment) > rows_per_segment) 
                  {						
                    rows_this_segment=0;
                    upd_num += 10000;					
                  }
              }
            else
              {
                if((++rows_this_segment) >= rows_per_segment) 
                  {
                    rows_this_segment=0;
                    upd_num += 10000;
                  }
              }
          }

          if (set_seeds == 0) {
            if (validate) {
              tdefs[tnum].verify(&o, 0);
            }
            else {
              tdefs[tnum].loader[direct] (&o, upd_num);
            }
          }
          break;

        case SUPP:
          mk_supp (i, &supp);
          if (set_seeds == 0) {
            if (validate) {
              tdefs[tnum].verify(&supp, 0);
            }
            else {
              tdefs[tnum].loader[direct] (&supp, upd_num);
            }
          }
          break;

        case CUST:
          mk_cust (i, &cust);
          if (set_seeds == 0) {
            if (validate) {
              tdefs[tnum].verify(&cust, 0);
            }
            else {
              tdefs[tnum].loader[direct] (&cust, upd_num);
            }
          }
          break;

        case PSUPP:
        case PART:
        case PART_PSUPP: 
          mk_part (i, &part);
          if (set_seeds == 0) {
            if (validate) {
              tdefs[tnum].verify(&part, 0);
            }
            else {
              tdefs[tnum].loader[direct] (&part, upd_num);
            }
          }
          break;

        case NATION:
          mk_nation (i, &code);
          if (set_seeds == 0) {
            if (validate) {
              tdefs[tnum].verify(&code, 0);
            }
            else {
              tdefs[tnum].loader[direct] (&code, 0);
            }
          }
          break;

        case REGION:
          mk_region (i, &code);
          if (set_seeds == 0) {
            if (validate) {
              tdefs[tnum].verify(&code, 0);
            }
            else {
              tdefs[tnum].loader[direct] (&code, 0);
            }
          }
          break;
        }
      row_stop(tnum);
      if (set_seeds && (i % tdefs[tnum].base) < 2)
        {
          printf("\nSeeds for %s at rowcount %lld\n", tdefs[tnum].comment, i);
          dump_seeds(tnum);
        }
    }
  completed |= 1 << tnum;
}



void
usage (void)
{
  fprintf (stderr, "%s\n%s\n\t%s\n%s %s\n\n",
           "USAGE:",
           "dbgen [-{vfFD}] [-O {fhmsv}][-T {pcsoPSOL}]",
           "[-s <scale>][-C <procs>][-S <step>]",
           "dbgen [-v] [-O {dfhmr}] [-s <scale>]",
           "[-U <updates>] [-r <percent>]");
  fprintf (stderr, "-b <s> -- load distributions for <s>\n");
  fprintf (stderr, "-C <n> -- use <n> processes to generate data\n");
  fprintf (stderr, "          [Under DOS, must be used with -S]\n");
  fprintf (stderr, "-D     -- do database load in line\n");
  fprintf (stderr, "-d <n> -- split deletes between <n> files\n");
  fprintf (stderr, "-f     -- force. Overwrite existing files\n");
  fprintf (stderr, "-F     -- generate flat files output\n");
  fprintf (stderr, "-h     -- display this message\n");
  fprintf (stderr, "-i <n> -- split inserts between <n> files\n");
  fprintf (stderr, "-n <s> -- inline load into database <s>\n");
  fprintf (stderr, "-O d   -- generate SQL syntax for deletes\n");
  fprintf (stderr, "-O f   -- over-ride default output file names\n");
  fprintf (stderr, "-O h   -- output files with headers\n");
  fprintf (stderr, "-O m   -- produce columnar output\n");
  fprintf (stderr, "-O r   -- generate key ranges for deletes.\n");
  fprintf (stderr, "-O v   -- Verify data set without generating it.\n");
  fprintf (stderr, "-q     -- enable QUIET mode\n");
  fprintf (stderr, "-r <n> -- updates refresh (n/100)%% of the\n");
  fprintf (stderr, "          data set\n");
  fprintf (stderr, "-s <n> -- set Scale Factor (SF) to  <n> \n");
  fprintf (stderr, "-S <n> -- build the <n>th step of the data/update set\n");
  fprintf (stderr, "-T c   -- generate cutomers ONLY\n");
  fprintf (stderr, "-T l   -- generate nation/region ONLY\n");
  fprintf (stderr, "-T L   -- generate lineitem ONLY\n");
  fprintf (stderr, "-T n   -- generate nation ONLY\n");
  fprintf (stderr, "-T o   -- generate orders/lineitem ONLY\n");
  fprintf (stderr, "-T O   -- generate orders ONLY\n");
  fprintf (stderr, "-T p   -- generate parts/partsupp ONLY\n");
  fprintf (stderr, "-T P   -- generate parts ONLY\n");
  fprintf (stderr, "-T r   -- generate region ONLY\n");
  fprintf (stderr, "-T s   -- generate suppliers ONLY\n");
  fprintf (stderr, "-T S   -- generate partsupp ONLY\n");
  fprintf (stderr, "-U <s> -- generate <s> update sets\n");
  fprintf (stderr, "-v     -- enable VERBOSE mode\n");
  fprintf (stderr,
           "\nTo generate the SF=1 (1GB), validation database population, use:\n");
  fprintf (stderr, "\tdbgen -vfF -s 1\n");
  fprintf (stderr, "\nTo generate updates for a SF=1 (1GB), use:\n");
  fprintf (stderr, "\tdbgen -v -U 1 -s 1\n");
}

/*
 * MAIN
 *
 * assumes the existance of getopt() to clean up the command 
 * line handling
 */
int dbgen_init ()
{
	
  table = (1 << CUST) |
    (1 << SUPP) |
    (1 << NATION) |
    (1 << REGION) |
    (1 << PART_PSUPP) |
    (1 << ORDER_LINE);
  force = 0;
  insert_segments=0;
  delete_segments=0;
  insert_orders_segment=0;
  insert_lineitem_segment=0;
  delete_segment=0;
  verbose = 0;
  columnar = 0;
  set_seeds = 0;
  header = 0;
  direct = 0;
  scale = 1;
  flt_scale = 1.0;
  updates = 0;
  arefresh = UPD_PCT;
  step = -1;
  tdefs[ORDER].base *=
    ORDERS_PER_CUST;			/* have to do this after init */
  tdefs[LINE].base *=
    ORDERS_PER_CUST;			/* have to do this after init */
  tdefs[ORDER_LINE].base *=
    ORDERS_PER_CUST;			/* have to do this after init */
  fnames = 0;
  db_name = NULL;
  gen_sql = 0;
  gen_rng = 0;
  children = 1;
  d_path = NULL;
		
  load_dists ();

#ifdef RNG_TEST
  for (DSS_HUGE i=0; i <= MAX_STREAM; i++)
    Seed[i].nCalls = 0;
#endif

  // have to do this after init
  tdefs[NATION].base = nations.count;
  tdefs[REGION].base = regions.count;
			
  return (0);
}


EXIT_NAMESPACE(dbgentpch);

