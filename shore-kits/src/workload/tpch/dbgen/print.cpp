/*
 * $Id: print.c,v 1.3 2005/10/28 02:56:22 jms Exp $
 *
 * Revision History
 * ===================
 * $Log: print.c,v $
 * Revision 1.3  2005/10/28 02:56:22  jms
 * add platform-specific printf formats to allow for DSS_HUGE data type
 *
 * Revision 1.2  2005/01/03 20:08:59  jms
 * change line terminations
 *
 * Revision 1.1.1.1  2004/11/24 23:31:47  jms
 * re-establish external server
 *
 * Revision 1.4  2004/02/18 16:26:49  jms
 * 32/64 bit changes for overflow handling needed additional changes when ported back to windows
 *
 * Revision 1.3  2004/02/18 14:05:53  jms
 * porting changes for LINUX and 64 bit RNG
 *
 * Revision 1.2  2004/01/22 05:49:29  jms
 * AIX porting (AIX 5.1)
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
/* generate flat files for data load */
#include <stdio.h>
#ifndef VMS
#include <sys/types.h>
#endif
#if defined(SUN)
#include <unistd.h>
#endif
#include <math.h>

#include "workload/tpch/dbgen/dss.h"
#include "workload/tpch/dbgen/dsstypes.h"
#include <string.h>


ENTER_NAMESPACE(dbgentpch);


/*
 * Function Prototypes
 */
FILE *print_prep PROTO((int table, int update));
int pr_drange PROTO((int tbl, DSS_HUGE min, DSS_HUGE cnt, long num));

FILE *
print_prep(int table, int update)
{
  char upath[128];
  FILE *res;

  if (updates)
    {
      if (update > 0) /* updates */
        if ( insert_segments )
          {
            int this_segment;
            if(strcmp(tdefs[table].name,"orders.tbl"))
              this_segment=++insert_orders_segment;
            else 
              this_segment=++insert_lineitem_segment;
            sprintf(upath, "%s%c%s.u%d.%d", 
                    env_config(PATH_TAG, PATH_DFLT),
                    PATH_SEP, tdefs[table].name, update%10000,this_segment);
          }
        else
          {
            sprintf(upath, "%s%c%s.u%d",
                    env_config(PATH_TAG, PATH_DFLT),
                    PATH_SEP, tdefs[table].name, update);
          }
      else /* deletes */
        if ( delete_segments )
          {
            ++delete_segment;
            sprintf(upath, "%s%cdelete.u%d.%d",
                    env_config(PATH_TAG, PATH_DFLT), PATH_SEP, -update%10000,
                    delete_segment);
          }
        else
          {
            sprintf(upath, "%s%cdelete.%d",
                    env_config(PATH_TAG, PATH_DFLT), PATH_SEP, -update);
          }
      return(fopen(upath, "w"));
    }
  res = tbl_open(table, "w");
  OPEN_CHECK(res, tdefs[table].name);
  return(res);
}

int dbg_print(int format, FILE *target, void *data, int len, int 
#ifdef EOL_HANDLING
          sep
#endif
          )
{
  int dollars,
    cents;

  switch(format)
    {
    case DT_STR:
      if (columnar)
        fprintf(target, "%-*s", len, (char *)data);
      else
        fprintf(target, "%s", (char *)data);
      break;
#ifdef MVS
    case DT_VSTR:
      /* note: only used in MVS, assumes columnar output */
      fprintf(target, "%c%c%-*s", 
              (len >> 8) & 0xFF, len & 0xFF, len, (char *)data);
      break;
#endif /* MVS */
    case DT_INT:
      if (columnar)
        fprintf(target, "%12ld", (long)data);
      else
        fprintf(target, "%ld", (long)data);
      break;
    case DT_HUGE:
      fprintf(target, HUGE_FORMAT, *(DSS_HUGE *)data);
      break;
    case DT_KEY:
      fprintf(target, "%ld", (long)data);
      break;
    case DT_MONEY:
      cents = (int)*(DSS_HUGE *)data;
      if (cents < 0)
        {
          fprintf(target, "-");
          cents = -cents;
        }
      dollars = cents / 100;
      cents %= 100;
      if (columnar)
        fprintf(target, "%12d.%02d", dollars, cents);
      else
        fprintf(target, "%d.%02d", dollars, cents);
      break;
    case DT_CHR:
      if (columnar)
        fprintf(target, "%c ", *(char *)data);
      else
        fprintf(target, "%c", *(char *)data);
      break;
    }

#ifdef EOL_HANDLING
  if (sep)
#endif /* EOL_HANDLING */
    if (!columnar)
      fprintf(target, "%c", SEPARATOR);
	
  return(0);
}

int pr_cust(void *vc, int /* mode */)
{
  customer_t* c = (customer_t*)vc;

  static FILE *fp = NULL;
        
  if (fp == NULL)
    fp = print_prep(CUST, 0);

  PR_STRT(fp);
  PR_HUGE(fp, &c->custkey);
  if (scale <= 3000)
    PR_VSTR(fp, c->name, C_NAME_LEN);
  else
    PR_VSTR(fp, c->name, C_NAME_LEN + 3);
  PR_VSTR(fp, c->address, 
          (columnar)?(long)(ceil(C_ADDR_LEN * V_STR_HGH)):c->alen);
  PR_HUGE(fp, &c->nation_code);
  PR_STR(fp, c->phone, PHONE_LEN);
  PR_MONEY(fp, &c->acctbal);
  PR_STR(fp, c->mktsegment, C_MSEG_LEN);
  PR_VSTR_LAST(fp, c->comment, 
               (columnar)?(long)(ceil(C_CMNT_LEN * V_STR_HGH)):c->clen);
  PR_END(fp);

  return(0);
}

/*
 * print the numbered order 
 */
int
pr_order(void *vo, int mode)
{
  order_t* o = (order_t*)vo;
  static FILE *fp_o = NULL;
  static int last_mode = 0;
        
  if (fp_o == NULL || mode != last_mode)
    {
      if (fp_o) 
        fclose(fp_o);
      fp_o = print_prep(ORDER, mode);
      last_mode = mode;
    }
  PR_STRT(fp_o);
  PR_HUGE(fp_o, &o->okey);
  PR_HUGE(fp_o, &o->custkey);
  PR_CHR(fp_o, &o->orderstatus);
  PR_MONEY(fp_o, &o->totalprice);
  PR_STR(fp_o, o->odate, DATE_LEN);
  PR_STR(fp_o, o->opriority, O_OPRIO_LEN);
  PR_STR(fp_o, o->clerk, O_CLRK_LEN);
  PR_INT(fp_o, o->spriority);
  PR_VSTR_LAST(fp_o, o->comment, 
               (columnar)?(long)(ceil(O_CMNT_LEN * V_STR_HGH)):o->clen);
  PR_END(fp_o);

  return(0);
}

/*
 * print an order's lineitems
 */
int
pr_line(void *vo, int mode)
{
  order_t* o = (order_t*)vo;
  static FILE *fp_l = NULL;
  static int last_mode = 0;
  long      i;
        
  if (fp_l == NULL || mode != last_mode)
    {
      if (fp_l) 
        fclose(fp_l);
      fp_l = print_prep(LINE, mode);
      last_mode = mode;
    }

  for (i = 0; i < o->lines; i++)
    {
      PR_STRT(fp_l);
      PR_HUGE(fp_l, &o->l[i].okey);
      PR_HUGE(fp_l, &o->l[i].partkey);
      PR_HUGE(fp_l, &o->l[i].suppkey);
      PR_HUGE(fp_l, &o->l[i].lcnt);
      PR_HUGE(fp_l, &o->l[i].quantity);
      PR_MONEY(fp_l, &o->l[i].eprice);
      PR_MONEY(fp_l, &o->l[i].discount);
      PR_MONEY(fp_l, &o->l[i].tax);
      //PR_CHR(fp_l, &o->l[i].rflag[0]);
      PR_CHR(fp_l, &o->l[i].rflag);
      //PR_CHR(fp_l, &o->l[i].lstatus[0]);
      PR_CHR(fp_l, &o->l[i].lstatus);
      PR_STR(fp_l, o->l[i].sdate, DATE_LEN);
      PR_STR(fp_l, o->l[i].cdate, DATE_LEN);
      PR_STR(fp_l, o->l[i].rdate, DATE_LEN);
      PR_STR(fp_l, o->l[i].shipinstruct, L_INST_LEN);
      PR_STR(fp_l, o->l[i].shipmode, L_SMODE_LEN);
      PR_VSTR_LAST(fp_l, o->l[i].comment, 
                   (columnar)?(long)(ceil(L_CMNT_LEN * V_STR_HGH)):o->l[i].clen);
      PR_END(fp_l);
    }

  return(0);
}

/*
 * print the numbered order *and* its associated lineitems
 */
int
pr_order_line(void *vo, int mode)
{
  order_t* o = (order_t*)vo;
  tdefs[ORDER].name = tdefs[ORDER_LINE].name;
  pr_order(o, mode);
  pr_line(o, mode);

  return(0);
}

/*
 * print the given part
 */
int pr_part(void *vp, int /* mode */)
{
  part_t* part = (part_t*)vp;
  static FILE *p_fp = NULL;

  if (p_fp == NULL)
    p_fp = print_prep(PART, 0);

  PR_STRT(p_fp);
  PR_HUGE(p_fp, &part->partkey);
  PR_VSTR(p_fp, part->name,
          (columnar)?(long)P_NAME_LEN:part->nlen);
  PR_STR(p_fp, part->mfgr, P_MFG_LEN);
  PR_STR(p_fp, part->brand, P_BRND_LEN);
  PR_VSTR(p_fp, part->type,
          (columnar)?(long)P_TYPE_LEN:part->tlen);
  PR_HUGE(p_fp, &part->size);
  PR_STR(p_fp, part->container, P_CNTR_LEN);
  PR_MONEY(p_fp, &part->retailprice);
  PR_VSTR_LAST(p_fp, part->comment, 
               (columnar)?(long)(ceil(P_CMNT_LEN * V_STR_HGH)):part->clen);
  PR_END(p_fp);

  return(0);
}

/*
 * print the given part's suppliers
 */
int
pr_psupp(void *vp, int mode)
{
  part_t* part = (part_t*)vp;
  static FILE *ps_fp = NULL;
  long      i;

  if (ps_fp == NULL)
    ps_fp = print_prep(PSUPP, mode);

  for (i = 0; i < SUPP_PER_PART; i++)
    {
      PR_STRT(ps_fp);
      PR_HUGE(ps_fp, &part->s[i].partkey);
      PR_HUGE(ps_fp, &part->s[i].suppkey);
      PR_HUGE(ps_fp, &part->s[i].qty);
      PR_MONEY(ps_fp, &part->s[i].scost);
      PR_VSTR_LAST(ps_fp, part->s[i].comment, 
                   (columnar)?(long)(ceil(PS_CMNT_LEN * V_STR_HGH)):part->s[i].clen);
      PR_END(ps_fp);
    }

  return(0);
}

/*
 * print the given part *and* its suppliers
 */
int
pr_part_psupp(void *vp, int mode)
{
  part_t* part = (part_t*)vp;
  tdefs[PART].name = tdefs[PART_PSUPP].name;
  pr_part(part, mode);
  pr_psupp(part, mode);

  return(0);
}

int
pr_supp(void *vs, int mode)
{
  supplier_t* supp = (supplier_t*)vs;
  static FILE *fp = NULL;
        
  if (fp == NULL)
    fp = print_prep(SUPP, mode);

  PR_STRT(fp);
  PR_HUGE(fp, &supp->suppkey);
  PR_STR(fp, supp->name, S_NAME_LEN);
  PR_VSTR(fp, supp->address, 
          (columnar)?(long)(ceil(S_ADDR_LEN * V_STR_HGH)):supp->alen);
  PR_HUGE(fp, &supp->nation_code);
  PR_STR(fp, supp->phone, PHONE_LEN);
  PR_MONEY(fp, &supp->acctbal);
  PR_VSTR_LAST(fp, supp->comment, 
               (columnar)?(long)(ceil(S_CMNT_LEN * V_STR_HGH)):supp->clen);
  PR_END(fp);

  return(0);
}

int
pr_nation(void *vc, int mode)
{
  code_t* c = (code_t*)vc;
  static FILE *fp = NULL;
        
  if (fp == NULL)
    fp = print_prep(NATION, mode);

  PR_STRT(fp);
  PR_HUGE(fp, &c->code);
  PR_STR(fp, c->text, NATION_LEN);
  PR_INT(fp, c->join);
  PR_VSTR_LAST(fp, c->comment, 
               (columnar)?(long)(ceil(N_CMNT_LEN * V_STR_HGH)):c->clen);
  PR_END(fp);

  return(0);
}

int
pr_region(void *vc, int mode)
{
  code_t* c = (code_t*)vc;
  static FILE *fp = NULL;
        
  if (fp == NULL)
    fp = print_prep(REGION, mode);

  PR_STRT(fp);
  PR_HUGE(fp, &c->code);
  PR_STR(fp, c->text, REGION_LEN);
  PR_VSTR_LAST(fp, c->comment, 
               (columnar)?(long)(ceil(R_CMNT_LEN * V_STR_HGH)):c->clen);
  PR_END(fp);

  return(0);
}

/* 
 * NOTE: this routine does NOT use the BCD2_* routines. As a result,
 * it WILL fail if the keys being deleted exceed 32 bits. Since this
 * would require ~660 update iterations, this seems an acceptable
 * oversight
 */
int
pr_drange(int tbl, DSS_HUGE min, DSS_HUGE cnt, long num)
{
  static int  last_num = 0;
  static FILE *dfp = NULL;
  DSS_HUGE child = -1;
  DSS_HUGE start, last, anew;

  static DSS_HUGE rows_per_segment=0;
  static DSS_HUGE rows_this_segment=0;

  if (last_num != num)
    {
      if (dfp)
        fclose(dfp);
      dfp = print_prep(tbl, -num);
      if (dfp == NULL)
        return(-1);
      last_num = num;
      rows_this_segment=0;
    }

  start = MK_SPARSE(min, num/ (10000 / arefresh));
  last = start - 1;
  for (child=min; cnt > 0; child++, cnt--)
    {
      anew = MK_SPARSE(child, num/ (10000 / arefresh));
      if (gen_rng == 1 && anew - last == 1)
        {
          last = anew;
          continue;
        }
      if (gen_sql)
        {
          fprintf(dfp, 
                  "delete from %s where %s between %lld and %lld;\n",
                  tdefs[ORDER].name, "o_orderkey", start, last);
          fprintf(dfp, 
                  "delete from %s where %s between %lld and %lld;\n",
                  tdefs[LINE].name, "l_orderkey", start, last);
          fprintf(dfp, "commit work;\n");
        }
      else 
        if (gen_rng)
          {
            PR_STRT(dfp);
            PR_HUGE(dfp, &start);
            PR_HUGE(dfp, &last);
            PR_END(dfp);
          }
        else
          {
            if (delete_segments)
              {

                if(rows_per_segment==0) 
                  rows_per_segment = (cnt / delete_segments) + 1;
                if((++rows_this_segment) > rows_per_segment)
                  {
                    fclose(dfp);
                    dfp = print_prep(tbl, -num);
                    if (dfp == NULL) return(-1);
                    last_num = num;
                    rows_this_segment=1;
                  }
              }
            PR_STRT(dfp);
            PR_HUGE(dfp, &anew);
            PR_END(dfp);
          }
      start = anew;
      last = anew;
    }
  if (gen_rng)
    {
      PR_STRT(dfp);
      PR_HUGE(dfp, &start);
      PR_HUGE(dfp, &last);
      PR_END(dfp);
    }
    
  return(0);
}

/*
 * verify functions: routines which replace the pr_routines and generate a pseudo checksum 
 * instead of generating the actual contents of the tables. Meant to allow large scale data 
 * validation without requiring a large amount of storage
 */
int vrf_cust(void *vc, int /* mode */)
{
  customer_t* c = (customer_t*)vc;

  VRF_STRT(CUST);
  VRF_INT(CUST, c->custkey);
  VRF_STR(CUST, c->name);
  VRF_STR(CUST, c->address);
  VRF_INT(CUST, c->nation_code);
  VRF_STR(CUST, c->phone);
  VRF_MONEY(CUST, c->acctbal);
  VRF_STR(CUST, c->mktsegment);
  VRF_STR(CUST, c->comment);
  VRF_END(CUST);

  return(0);
}

/*
 * print the numbered order 
 */
int vrf_order(void *vo, int /* mode */)
{
  order_t* o = (order_t*)vo;

  VRF_STRT(ORDER);
  VRF_HUGE(ORDER, o->okey);
  VRF_INT(ORDER, o->custkey);
  VRF_CHR(ORDER, o->orderstatus);
  VRF_MONEY(ORDER, o->totalprice);
  VRF_STR(ORDER, o->odate);
  VRF_STR(ORDER, o->opriority);
  VRF_STR(ORDER, o->clerk);
  VRF_INT(ORDER, o->spriority);
  VRF_STR(ORDER, o->comment);
  VRF_END(ORDER);

  return(0);
}

/*
 * print an order's lineitems
 */
int vrf_line(void *vo, int /* mode */)
{
  order_t* o = (order_t*)vo;
  int i;

  for (i = 0; i < o->lines; i++)
    {
      VRF_STRT(LINE);
      VRF_HUGE(LINE, o->l[i].okey);
      VRF_INT(LINE, o->l[i].partkey);
      VRF_INT(LINE, o->l[i].suppkey);
      VRF_INT(LINE, o->l[i].lcnt);
      VRF_INT(LINE, o->l[i].quantity);
      VRF_MONEY(LINE, o->l[i].eprice);
      VRF_MONEY(LINE, o->l[i].discount);
      VRF_MONEY(LINE, o->l[i].tax);
      //VRF_CHR(LINE, o->l[i].rflag[0]);
      VRF_CHR(LINE, o->l[i].rflag);
      //VRF_CHR(LINE, o->l[i].lstatus[0]);
      VRF_CHR(LINE, o->l[i].lstatus);
      VRF_STR(LINE, o->l[i].sdate);
      VRF_STR(LINE, o->l[i].cdate);
      VRF_STR(LINE, o->l[i].rdate);
      VRF_STR(LINE, o->l[i].shipinstruct);
      VRF_STR(LINE, o->l[i].shipmode);
      VRF_STR(LINE, o->l[i].comment);
      VRF_END(LINE);
    }

  return(0);
}

/*
 * print the numbered order *and* its associated lineitems
 */
int
vrf_order_line(void *vo, int mode)
{
  order_t* o = (order_t*)vo;

  vrf_order(o, mode);
  vrf_line(o, mode);

  return(0);
}

/*
 * print the given part
 */
int vrf_part(void *vp, int /* mode */)
{
  part_t* part = (part_t*)vp;

  VRF_STRT(PART);
  VRF_INT(PART, part->partkey);
  VRF_STR(PART, part->name);
  VRF_STR(PART, part->mfgr);
  VRF_STR(PART, part->brand);
  VRF_STR(PART, part->type);
  VRF_INT(PART, part->size);
  VRF_STR(PART, part->container);
  VRF_MONEY(PART, part->retailprice);
  VRF_STR(PART, part->comment);
  VRF_END(PART);

  return(0);
}

/*
 * print the given part's suppliers
 */
int vrf_psupp(void *vp, int /* mode */)
{
  part_t* part = (part_t*)vp;
  long      i;

  for (i = 0; i < SUPP_PER_PART; i++)
    {
      VRF_STRT(PSUPP);
      VRF_INT(PSUPP, part->s[i].partkey);
      VRF_INT(PSUPP, part->s[i].suppkey);
      VRF_INT(PSUPP, part->s[i].qty);
      VRF_MONEY(PSUPP, part->s[i].scost);
      VRF_STR(PSUPP, part->s[i].comment);
      VRF_END(PSUPP);
    }

  return(0);
}

/*
 * print the given part *and* its suppliers
 */
int
vrf_part_psupp(void *vp, int mode)
{
  part_t* part = (part_t*)vp;

  vrf_part(part, mode);
  vrf_psupp(part, mode);

  return(0);
}

int vrf_supp(void *vs, int /* mode */)
{
  supplier_t* supp = (supplier_t*)vs;

  VRF_STRT(SUPP);
  VRF_INT(SUPP, supp->suppkey);
  VRF_STR(SUPP, supp->name);
  VRF_STR(SUPP, supp->address);
  VRF_INT(SUPP, supp->nation_code);
  VRF_STR(SUPP, supp->phone);
  VRF_MONEY(SUPP, supp->acctbal);
  VRF_STR(SUPP, supp->comment); 
  VRF_END(SUPP);

  return(0);
}

int vrf_nation(void *vc, int /* mode */)
{
  code_t* c = (code_t*)vc;

  VRF_STRT(NATION);
  VRF_INT(NATION, c->code);
  VRF_STR(NATION, c->text);
  VRF_INT(NATION, c->join);
  VRF_STR(NATION, c->comment);
  VRF_END(NATION);

  return(0);
}

int vrf_region(void *vc, int /* mode */)
{
  code_t* c = (code_t*)vc;  
  
  VRF_STRT(REGION);
  VRF_INT(REGION, c->code);
  VRF_STR(REGION, c->text);
  VRF_STR(REGION, c->comment);
  VRF_END(fp);

  return(0);
}



EXIT_NAMESPACE(dbgentpch);
