/* @(#)print.c	2.1.8.2 */
/* generate flat files for data load */
#include <stdio.h>
#ifndef VMS
#include <sys/types.h>
#endif

#if defined(SUN)
#include <unistd.h>
#endif

#if defined(LINUX)
#include <unistd.h>
#endif /*LINUX*/

#include <math.h>

#include "workload/ssb/dbgen/dss.h"
#include "workload/ssb/dbgen/dsstypes.h"
#include <string.h>

#include <stdio.h>
#include <time.h>


ENTER_NAMESPACE(dbgenssb)


/*
 * Function Prototypes
 */
FILE *print_prep PROTO((int table, int update));
int pr_drange PROTO((int tbl, long min, long cnt, long num));

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
					env_config((char*)PATH_TAG, (char*)PATH_DFLT),
					PATH_SEP, tdefs[table].name, update%10000,this_segment);
				}
			else
				{
				sprintf(upath, "%s%c%s.u%d",
				env_config((char*)PATH_TAG, (char*)PATH_DFLT),
				PATH_SEP, tdefs[table].name, update);
				}
		else /* deletes */
			if ( delete_segments )
				{
				++delete_segment;
				sprintf(upath, "%s%cdelete.u%d.%d",
				env_config((char*)PATH_TAG, (char*)PATH_DFLT), PATH_SEP, -update%10000,
					delete_segment);
				}
			else
				{
				sprintf(upath, "%s%cdelete.%d",
				env_config((char*)PATH_TAG, (char*)PATH_DFLT), PATH_SEP, -update);
				}
		return(fopen(upath, "w"));
        }
    res = tbl_open(table, (char*)"w");
    OPEN_CHECK(res, tdefs[table].name);
    return(res);
}

int
dbg_print(int format, FILE *target, void *data, int len, int sep)
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
#ifndef SUPPORT_64BITS
        if (*(long *)((long *)data + 1) == 0) \
           if (columnar) fprintf(target, "%12ld", *(long *)data);
           else fprintf(target, "%ld", *(long *)data);
        else
           if (columnar) fprintf(target, "%5ld%07ld", 
				*(long *)((long *)data + 1), *(long *)data);
           else fprintf(target,"%ld%07ld", 
				*(long *)((long *)data + 1), *(long *)data);
#else
		fprintf(target, HUGE_FORMAT, *(DSS_HUGE *)data);
#endif /* SUPPORT_64BITS */
		break;
	case DT_KEY:
		fprintf(target, "%ld", (long)data);
		break;
	case DT_MONEY:
		cents = (long)data;
		if (cents < 0)
			{
			fprintf(target, "-");
			cents = -cents;
			}
		dollars = cents / 100;
		cents %= 100;
		if (columnar)
			fprintf(target, "%12ld.%02ld", dollars, cents);
		else
			fprintf(target, "%ld.%02ld", dollars, cents);
		break;
	case DT_CHR:
		if (columnar)
			fprintf(target, "%c ", *(char*)data);
		else
			fprintf(target, "%c", *(char*)data);
		break;
	}

#ifdef EOL_HANDLING
	if (sep)
#endif /* EOL_HANDLING */
	if (!columnar && (sep != -1))
		fprintf(target, "%c", SEPARATOR);
	
	return(0);
}

#ifdef SSBM
int
pr_cust(void *c, int /*mode*/)
{
static FILE *fp = NULL;
 customer_t *cp=(customer_t *)c;
   if (fp == NULL)
        fp = print_prep(CUST, 0);

   PR_STRT(fp);
   PR_INT(fp, cp->custkey);
   PR_VSTR(fp, cp->name, C_NAME_LEN);
   PR_VSTR(fp, cp->address, 
       (columnar)?(long)(ceil(C_ADDR_LEN * V_STR_HGH)):cp->alen);
   PR_STR(fp, cp->city,CITY_FIX);
   PR_STR(fp, cp->nation_name, C_NATION_NAME_LEN);
   PR_STR(fp, cp->region_name, C_REGION_NAME_LEN);
   PR_STR(fp, cp->phone, PHONE_LEN);
   PR_STR(fp, cp->mktsegment,MAXAGG_LEN);
   PR_END(fp);

   return(0);
}

#else
int
pr_cust(void *c, int mode)
{
static FILE *fp = NULL;
 customer_t *cp=(customer_t *)c;        
   if (fp == NULL)
        fp = print_prep(CUST, 0);

   PR_STRT(fp);
   PR_INT(fp, cp->custkey);
   PR_VSTR(fp, cp->name, C_NAME_LEN);
   PR_VSTR(fp, cp->address, 
       (columnar)?(long)(ceil(C_ADDR_LEN * V_STR_HGH)):cp->alen);
   PR_INT(fp, cp->nation_code);
   PR_STR(fp, cp->phone, PHONE_LEN);
   PR_MONEY(fp, cp->acctbal);
   PR_STR(fp, cp->mktsegment, C_MSEG_LEN);
   PR_VSTR_LAST(fp, cp->comment, 
       (columnar)?(long)(ceil(C_CMNT_LEN * V_STR_HGH)):cp->clen);
   PR_END(fp);

   return(0);
}
#endif

/*
 * print the numbered order 
 */
#ifdef SSBM

#else
int
pr_order(void *o, int mode)
{
    static FILE *fp_o = NULL;
    static int last_mode = 0;
    order_t *op=(order_t *)o;
    if (fp_o == NULL || mode != last_mode)
        {
        if (fp_o) 
            fclose(fp_o);
        fp_o = print_prep(ORDER, mode);
        last_mode = mode;
        }
    PR_STRT(fp_o);
    PR_HUGE(fp_o, op->okey);
    PR_INT(fp_o, op->custkey);
    PR_CHR(fp_o, op->orderstatus);
    PR_MONEY(fp_o, op->totalprice);
    PR_STR(fp_o, op->odate, DATE_LEN);
    PR_STR(fp_o, op->opriority, O_OPRIO_LEN);
    PR_STR(fp_o, op->clerk, O_CLRK_LEN);
    PR_INT(fp_o, op->spriority);
    PR_VSTR_LAST(fp_o, op->comment, 
       (columnar)?(long)(ceil(O_CMNT_LEN * V_STR_HGH)):op->clen);
    PR_END(fp_o);

    return(0);
}
#endif

/*
 * print an order's lineitems
 */
#ifdef SSBM
int
pr_line(void *o, int mode)
{

    static FILE *fp_l = NULL;
    static int last_mode = 0;

    order_t *op=(order_t *)o;

    long      i;
    //int days;
    //char buf[100];

    if (fp_l == NULL || mode != last_mode)
        {
        if (fp_l) 
            fclose(fp_l);
        fp_l = print_prep(LINE, mode);
        last_mode = mode;
        }

    for (i = 0; i < op->lines; i++)
        {
        PR_STRT(fp_l);
        PR_HUGE(fp_l, op->lineorders[i].okey);
        PR_INT(fp_l, op->lineorders[i].linenumber);
	PR_INT(fp_l, op->lineorders[i].custkey);
	PR_INT(fp_l, op->lineorders[i].partkey);
        PR_INT(fp_l, op->lineorders[i].suppkey);
        PR_STR(fp_l, op->lineorders[i].orderdate, DATE_LEN);
	PR_STR(fp_l, op->lineorders[i].opriority, O_OPRIO_LEN);
	PR_INT(fp_l, op->lineorders[i].ship_priority);
        PR_INT(fp_l, op->lineorders[i].quantity);
        PR_INT(fp_l, op->lineorders[i].extended_price);
        PR_INT(fp_l, op->lineorders[i].order_totalprice);
        PR_INT(fp_l, op->lineorders[i].discount);
        PR_INT(fp_l, op->lineorders[i].revenue);
	PR_INT(fp_l, op->lineorders[i].supp_cost);
	PR_INT(fp_l, op->lineorders[i].tax);
	PR_STR(fp_l, op->lineorders[i].commit_date, DATE_LEN);
	PR_STR(fp_l, op->lineorders[i].shipmode, O_SHIP_MODE_LEN);
        PR_END(fp_l);
        }

   return(0);
}
#else
int
pr_line(void *o, int mode)
{
    static FILE *fp_l = NULL;
    static int last_mode = 0;
    order_t *op=(order_t *)o;

    long      i;
    int days;
    char buf[100];

    if (fp_l == NULL || mode != last_mode)
        {
        if (fp_l) 
            fclose(fp_l);
        fp_l = print_prep(LINE, mode);
        last_mode = mode;
        }

    for (i = 0; i < op->lines; i++)
        {
        PR_STRT(fp_l);
        PR_HUGE(fp_l, op->l[i].okey);
        PR_INT(fp_l, op->l[i].partkey);
        PR_INT(fp_l, op->l[i].suppkey);
        PR_INT(fp_l, op->l[i].lcnt);
        PR_INT(fp_l, op->l[i].quantity);
        PR_MONEY(fp_l, op->l[i].eprice);
        PR_MONEY(fp_l, op->l[i].discount);
        PR_MONEY(fp_l, op->l[i].tax);
        PR_CHR(fp_l, op->l[i].rflag[0]);
        PR_CHR(fp_l, op->l[i].lstatus[0]);
        PR_STR(fp_l, op->l[i].sdate, DATE_LEN);
        PR_STR(fp_l, op->l[i].cdate, DATE_LEN);
        PR_STR(fp_l, op->l[i].rdate, DATE_LEN);
        PR_STR(fp_l, op->l[i].shipinstruct, L_INST_LEN);
        PR_STR(fp_l, op->l[i].shipmode, L_SMODE_LEN);
        PR_VSTR_LAST(fp_l, op->l[i].comment, 
            (columnar)?(long)(ceil(L_CMNT_LEN *
        V_STR_HGH)):op->l[i].clen);
        PR_END(fp_l);
        }

   return(0);
}
#endif

/*
 * print the numbered order *and* its associated lineitems
 */
#ifdef SSBM
#else
int
pr_order_line(void *o, int mode)
{
    order_t *op=(order_t *)o;
    tdefs[ORDER].name = tdefs[ORDER_LINE].name;
    pr_order(op, mode);
    pr_line(op, mode);

    return(0);
}
#endif

/*
 * print the given part
 */
#ifdef SSBM
int
pr_part(void *part, int /*mode*/)
{
    static FILE *p_fp = NULL;

    part_t *part_p=(part_t *)part;

    if (p_fp == NULL)
	p_fp = print_prep(PART, 0);

    PR_STRT(p_fp);
    PR_INT(p_fp, part_p->partkey);
    PR_VSTR(p_fp, part_p->name,
            (columnar)?(long)P_NAME_LEN:part_p->nlen);
    PR_STR(p_fp, part_p->mfgr, P_MFG_LEN);
    PR_STR(p_fp, part_p->category, P_CAT_LEN);
    PR_STR(p_fp, part_p->brand, P_BRND_LEN);

    /*need to handle color*/
    PR_VSTR(p_fp, part_p->color,(columnar)?(long)P_COLOR_LEN:part_p->clen);
    PR_VSTR(p_fp, part_p->type,
	    (columnar)?(long)P_TYPE_LEN:part_p->tlen);
    PR_INT(p_fp, part_p->size);
    PR_STR(p_fp, part_p->container, P_CNTR_LEN);
    PR_END(p_fp);
    return(0);
}

#else
int
pr_part(void *part, int mode)
{
static FILE *p_fp = NULL;
    part_t *part_p=(part_t *)part;

    if (p_fp == NULL)
        p_fp = print_prep(PART, 0);

   PR_STRT(p_fp);
   PR_INT(p_fp, part_p->partkey);
   PR_VSTR(p_fp, part_p->name,
       (columnar)?(long)P_NAME_LEN:part_p->nlen);
   PR_STR(p_fp, part_p->mfgr, P_MFG_LEN);
   PR_STR(p_fp, part_p->brand, P_BRND_LEN);
   PR_VSTR(p_fp, part_p->type,
       (columnar)?(long)P_TYPE_LEN:part_p->tlen);
   PR_INT(p_fp, part_p->size);
   PR_STR(p_fp, part_p->container, P_CNTR_LEN);
   PR_MONEY(p_fp, part_p->retailprice);
   PR_VSTR_LAST(p_fp, part_p->comment, 
       (columnar)?(long)(ceil(P_CMNT_LEN * V_STR_HGH)):part_p->clen);
   PR_END(p_fp);

   return(0);
}
#endif

/*
 * print the given part's suppliers
 */
#ifdef SSBM
/*SSBM don't have partsupplier table*/       
#else
int
pr_psupp(void *part, int mode)
{
    static FILE *ps_fp = NULL;
    part_t *part_p=(part_t *)part;

    long      i;

    if (ps_fp == NULL)
        ps_fp = print_prep(PSUPP, mode);

   for (i = 0; i < SUPP_PER_PART; i++)
      {
      PR_STRT(ps_fp);
      PR_INT(ps_fp, part_p->s[i].partkey);
      PR_INT(ps_fp, part_p->s[i].suppkey);
      PR_INT(ps_fp, part_p->s[i].qty);
      PR_MONEY(ps_fp, part_p->s[i].scost);
      PR_VSTR_LAST(ps_fp, part_p->s[i].comment, 
       (columnar)?(long)(ceil(PS_CMNT_LEN * V_STR_HGH)):part_p->s[i].clen);
      PR_END(ps_fp);
      }

   return(0);
}
#endif

/*
 * print the given part *and* its suppliers
 */
#ifdef SSBM
/*SSBM don't have partsupplier table*/       
#else
int
pr_part_psupp(void *part, int mode)
{
    part_t *part_p=(part_t *)part;

    tdefs[PART].name = tdefs[PART_PSUPP].name;
    pr_part(part_p, mode);
    pr_psupp(part_p, mode);

    return(0);
}
#endif


#ifdef SSBM
int
pr_supp(void *supp, int mode)
{
    static FILE *fp = NULL;
    supplier_t *supp_p=(supplier_t *)supp;
    if (fp == NULL)
        fp = print_prep(SUPP, mode);

    PR_STRT(fp);
    PR_INT(fp, supp_p->suppkey);
    PR_STR(fp, supp_p->name, S_NAME_LEN);
    
    PR_VSTR(fp, supp_p->address,
	    (columnar)?(long)(ceil(S_ADDR_LEN * V_STR_HGH)):supp_p->alen);
    PR_STR(fp, supp_p->city, CITY_FIX);
    PR_STR(fp, supp_p->nation_name, C_NATION_NAME_LEN);
    PR_STR(fp, supp_p->region_name, C_REGION_NAME_LEN);
    PR_STR(fp, supp_p->phone, PHONE_LEN);
    PR_END(fp);

    return(0);
}
#else
int
pr_supp(void *supp, int mode)
{
static FILE *fp = NULL;
    supplier_t *supp_p=(supplier_t *)supp;
        
   if (fp == NULL)
        fp = print_prep(SUPP, mode);

   PR_STRT(fp);
   PR_INT(fp, supp_p->suppkey);
   PR_STR(fp, supp_p->name, S_NAME_LEN);
   PR_VSTR(fp, supp_p->address, 
       (columnar)?(long)(ceil(S_ADDR_LEN * V_STR_HGH)):supp_p->alen);
   PR_INT(fp, supp_p->nation_code);
   PR_STR(fp, supp_p->phone, PHONE_LEN);
   PR_MONEY(fp, supp_p->acctbal);
   PR_VSTR_LAST(fp, supp_p->comment, 
       (columnar)?(long)(ceil(S_CMNT_LEN * V_STR_HGH)):supp_p->clen);
   PR_END(fp);

   return(0);
}
#endif

#ifdef SSBM
#else
int
pr_nation(void *c, int mode)
{
static FILE *fp = NULL;
 code_t *cp=(code_t *)c;
   if (fp == NULL)
        fp = print_prep(NATION, mode);

   PR_STRT(fp);
   PR_INT(fp, cp->code);
   PR_STR(fp, cp->text, NATION_LEN);
   PR_INT(fp, cp->join);
   PR_VSTR_LAST(fp, cp->comment,  code_t *cp=(code_t *)c;

       (columnar)?(long)(ceil(N_CMNT_LEN * V_STR_HGH)):cp->clen);
   PR_END(fp);

   return(0);
}

int
pr_region(void *c, int mode)
{
static FILE *fp = NULL;
   code_t *cp=(code_t *)c;
    
   if (fp == NULL)
        fp = print_prep(REGION, mode);

   PR_STRT(fp);
   PR_INT(fp, cp->code);
   PR_STR(fp, cp->text, REGION_LEN);
   PR_VSTR_LAST(fp, cp->comment, 
       (columnar)?(long)(ceil(R_CMNT_LEN * V_STR_HGH)):cp->clen);
   PR_END(fp);

   return(0);
}
#endif

/* 
 * NOTE: this routine does NOT use the BCD2_* routines. As a result,
 * it WILL fail if the keys being deleted exceed 32 bits. Since this
 * would require ~660 update iterations, this seems an acceptable
 * oversight
 */
int
pr_drange(int tbl, long min, long cnt, long num)
{
    static int  last_num = 0;
    static FILE *dfp = NULL;
    int child = -1;
    long start, last, anew;

	static int rows_per_segment=0;
	static int rows_this_segment=0;
	static int residual_rows=0;

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

    start = MK_SPARSE(min, (num - 1)/ (10000 / refresh));
    last = start - 1;
    for (child=min; cnt > 0; child++, cnt--)
        {
        anew = MK_SPARSE(child, (num - 1) / (10000 / refresh));
        if (gen_rng == 1 && anew - last == 1)
            {
            last = anew;
            continue;
            }
	if (gen_sql)
	    {
	    fprintf(dfp, 
		"delete from %s where %s between %ld and %ld;\n",
		    tdefs[ORDER].name, "o_orderkey", start, last);
	    fprintf(dfp, 
		"delete from %s where %s between %ld and %ld;\n",
		    tdefs[LINE].name, "l_orderkey", start, last);
	    fprintf(dfp, "commit work;\n");
	    }
	else 
	    if (gen_rng)
                {
                PR_STRT(dfp);
                PR_INT(dfp, start);
                PR_INT(dfp, last);
                PR_END(dfp);
                }
            else
                {
				if (delete_segments)
					{
					if(rows_per_segment==0)
						{
						rows_per_segment = (cnt / delete_segments);
						residual_rows = (cnt % delete_segments);
						rows_per_segment++;
						}
					if(delete_segment <= residual_rows)
						{
						if((++rows_this_segment) > rows_per_segment)
							{
							fclose(dfp);
							dfp = print_prep(tbl, -num);
							if (dfp == NULL) return(-1);
							last_num = num;
							rows_this_segment=1;
							}
						}
					else
						{
						if((++rows_this_segment) >= rows_per_segment)
							{
							fclose(dfp);
							dfp = print_prep(tbl, -num);
							if (dfp == NULL) return(-1);
							last_num = num;
							rows_this_segment=1;
							}
						}
					}
                PR_STRT(dfp);
                PR_KEY(dfp, anew);
                PR_END(dfp);
                }
	start = anew;
	last = anew;
        }
    if (gen_rng)
	{
	PR_STRT(dfp);
	PR_INT(dfp, start);
	PR_INT(dfp, last);
	PR_END(dfp);
	}
    
    return(0);
}

#ifdef SSBM
int pr_date(void *d, int /*mode*/){
    static FILE *d_fp = NULL;
    date_t *dp=(date_t *)d;
    if (d_fp == NULL)
	d_fp = print_prep(DATE, 0);

    PR_STRT(d_fp);
    PR_INT(d_fp, dp->datekey);
    PR_STR(d_fp, dp->date,D_DATE_LEN);
    PR_STR(d_fp, dp->dayofweek,D_DAYWEEK_LEN);
    PR_STR(d_fp, dp->month,D_MONTH_LEN);
    PR_INT(d_fp, dp->year);
    PR_INT(d_fp, dp->yearmonthnum);
    PR_STR(d_fp, dp->yearmonth,D_YEARMONTH_LEN);
    PR_INT(d_fp, dp->daynuminweek);
    PR_INT(d_fp, dp->daynuminmonth);
    PR_INT(d_fp, dp->daynuminyear);
    PR_INT(d_fp, dp->monthnuminyear);
    PR_INT(d_fp, dp->weeknuminyear);
    PR_VSTR(d_fp, 
	    dp->sellingseason,(columnar)?(long)D_SEASON_LEN:dp->slen);
    PR_STR(d_fp,dp->lastdayinweekfl,2);
    PR_STR(d_fp,dp->lastdayinmonthfl,2);
    PR_STR(d_fp,dp->holidayfl,2);
    PR_STR(d_fp,dp->weekdayfl,2);

    PR_END(d_fp);
    return(0);

}

#endif
/*
 * verify functions: routines which replace the pr_routines and generate a pseudo checksum 
 * instead of generating the actual contents of the tables. Meant to allow large scale data 
 * validation without requiring a large amount of storage
 */
#ifdef SSBM
int
vrf_cust(void *c, int /*mode*/)
{
  customer_t *cp=(customer_t *)c;
   VRF_STRT(CUST);
   VRF_INT(CUST, cp->custkey);
   VRF_STR(CUST, cp->name);
   VRF_STR(CUST, cp->address);
   VRF_STR(CUST, cp->city);
   VRF_STR(CUST, cp->nation_name);
   VRF_STR(CUST, cp->region_name);
   VRF_STR(CUST, cp->phone);
   VRF_STR(CUST, cp->mktsegment);
   VRF_END(CUST);

   return(0);
}

#else
int
vrf_cust(void *c, int mode)
{
  customer_t *cp=(customer_t *)c;
   VRF_STRT(CUST);
   VRF_INT(CUST, cp->custkey);
   VRF_STR(CUST, cp->name);
   VRF_STR(CUST, cp->address);
   VRF_INT(CUST, cp->nation_code);
   VRF_STR(CUST, cp->phone);
   VRF_MONEY(CUST, cp->acctbal);
   VRF_STR(CUST, cp->mktsegment);
   VRF_STR(CUST, cp->comment);
   VRF_END(CUST);

   return(0);
}
#endif

/*
 * print the numbered order 
 */
#ifdef SSBM
#else
int
vrf_order(void *o, int mode)
{
    order_t *o=(order_t *)o;
    VRF_STRT(ORDER);
    VRF_HUGE(ORDER, op->okey);
    VRF_INT(ORDER, op->custkey);
    VRF_CHR(ORDER, op->orderstatus);
    VRF_MONEY(ORDER, op->totalprice);
    VRF_STR(ORDER, op->odate);
    VRF_STR(ORDER, op->opriority);
    VRF_STR(ORDER, op->clerk);
    VRF_INT(ORDER, op->spriority);
    VRF_STR(ORDER, op->comment);
    VRF_END(ORDER);

    return(0);
}
#endif

/*
 * print an order's lineitems
 */
#ifdef SSBM
int
vrf_line(void *o, int /*mode*/)
{
    order_t * op =(order_t *)o;
    int i;

    for (i = 0; i < op->lines; i++)
        {
	    VRF_STRT(LINE);
	    VRF_HUGE(LINE, op->lineorders[i].okey);
	    VRF_INT(LINE, op->lineorders[i].linenumber);
	    VRF_INT(LINE, op->lineorders[i].custkey);
	    VRF_INT(LINE, op->lineorders[i].partkey);
	    VRF_INT(LINE, op->lineorders[i].suppkey);
	    VRF_STR(LINE, op->lineorders[i].orderdate);
	    VRF_STR(LINE, op->lineorders[i].opriority);
	    VRF_INT(LINE, op->lineorders[i].ship_priority);
	    VRF_INT(LINE, op->lineorders[i].quantity);
	    VRF_INT(LINE, op->lineorders[i].extended_price);
	    VRF_INT(LINE, op->lineorders[i].order_totalprice);
	    VRF_INT(LINE, op->lineorders[i].discount);
	    VRF_INT(LINE, op->lineorders[i].revenue);
	    VRF_INT(LINE, op->lineorders[i].supp_cost);
	    VRF_INT(LINE, op->lineorders[i].tax);
	    VRF_STR(LINE, op->lineorders[i].commit_date);
	    VRF_STR(LINE, op->lineorders[i].shipmode);
	    VRF_END(LINE);
        }

    return(0);
}

#else
int
vrf_line(void *o, int mode)
{
  order_t *op=(order_t *)o;
	int i;

    for (i = 0; i < op->lines; i++)
        {
        VRF_STRT(LINE);
        VRF_HUGE(LINE, op->l[i].okey);
        VRF_INT(LINE, op->l[i].partkey);
        VRF_INT(LINE, op->l[i].suppkey);
        VRF_INT(LINE, op->l[i].lcnt);
        VRF_INT(LINE, op->l[i].quantity);
        VRF_MONEY(LINE, op->l[i].eprice);
        VRF_MONEY(LINE, op->l[i].discount);
        VRF_MONEY(LINE, op->l[i].tax);
        VRF_CHR(LINE, op->l[i].rflag[0]);
        VRF_CHR(LINE, op->l[i].lstatus[0]);
        VRF_STR(LINE, op->l[i].sdate);
        VRF_STR(LINE, op->l[i].cdate);
        VRF_STR(LINE, op->l[i].rdate);
        VRF_STR(LINE, op->l[i].shipinstruct);
        VRF_STR(LINE, op->l[i].shipmode);
        VRF_STR(LINE, op->l[i].comment);
        VRF_END(LINE);
        }

   return(0);
}
#endif

/*
 * print the numbered order *and* its associated lineitems
 */
#ifdef SSBM
#else
int
vrf_order_line(void *o, int mode)
{
  order_t *op=(order_t *)o;
    vrf_order(op, mode);
    vrf_line(op, mode);

    return(0);
}
#endif

/*
 * print the given part
 */
#ifdef SSBM
int
vrf_part(void *part, int /*mode*/)
{
    part_t *part_p=(part_t*)part;
    VRF_STRT(PART);
    VRF_INT(PART, part_p->partkey);
    VRF_STR(PART, part_p->name);
    VRF_STR(PART, part_p->mfgr);
    VRF_STR(PART, part_p->brand);
    VRF_STR(PART, part_p->type);
    VRF_INT(PART, part_p->size);
    VRF_STR(PART, part_p->container);
    VRF_STR(PART, part_p->category);
    VRF_END(PART);

    return(0);
}

#else
int
vrf_part(void *part, int mode)
{
   part_t *part_p=(part_t*)part;
   VRF_STRT(PART);
   VRF_INT(PART, part_p->partkey);
   VRF_STR(PART, part_p->name);
   VRF_STR(PART, part_p->mfgr);
   VRF_STR(PART, part_p->brand);
   VRF_STR(PART, part_p->type);
   VRF_INT(PART, part_p->size);
   VRF_STR(PART, part_p->container);
   VRF_MONEY(PART, part_p->retailprice);
   VRF_STR(PART, part_p->comment);
   VRF_END(PART);

   return(0);
}
#endif

/*
 * print the given part's suppliers
 */
#ifdef SSBM
#else
int
vrf_psupp(void *part, int mode)
{
  part_t *part_p=(part_t*)part;
   long      i;

   for (i = 0; i < SUPP_PER_PART; i++)
      {
      VRF_STRT(PSUPP);
      VRF_INT(PSUPP, part_p->s[i].partkey);
      VRF_INT(PSUPP, part_p->s[i].suppkey);
      VRF_INT(PSUPP, part_p->s[i].qty);
      VRF_MONEY(PSUPP, part_p->s[i].scost);
      VRF_STR(PSUPP, part_p->s[i].comment);
      VRF_END(PSUPP);
      }

   return(0);
}
#endif

/*
 * print the given part *and* its suppliers
 */
#ifdef SSBM
#else
int
vrf_part_psupp(void *part, int mode)
{
    part_t *part=(part_t *)part_p;
    vrf_part(part_p, mode);
    vrf_psupp(part_p, mode);

    return(0);
}
#endif

#ifdef SSBM
int
vrf_supp(void *supp, int /*mode*/)
{
    supplier_t* supp_p=(supplier_t*)supp;
    VRF_STRT(SUPP);
    VRF_INT(SUPP, supp_p->suppkey);
    VRF_STR(SUPP, supp_p->name);
    
    VRF_STR(CUST, supp_p->address);
    VRF_STR(CUST, supp_p->nation_name);
    VRF_INT(CUST, supp_p->region_key);
    VRF_STR(CUST, supp_p->region_name);
    VRF_STR(CUST, supp_p->phone);
    VRF_END(SUPP);

    return(0);
}

#else
int
vrf_supp(void *supp, int mode)
{
   supplier_t *supp_p=(supplier_t*)supp;   
   VRF_STRT(SUPP);
   VRF_INT(SUPP, supp_p->suppkey);
   VRF_STR(SUPP, supp_p->name);
   VRF_STR(SUPP, supp_p->address);
   VRF_INT(SUPP, supp_p->nation_code);
   VRF_STR(SUPP, supp_p->phone);
   VRF_MONEY(SUPP, supp_p->acctbal);
   VRF_STR(SUPP, supp_p->comment); 
   VRF_END(SUPP);

   return(0);
}
#endif

#ifdef SSBM
#else
int
vrf_nation(void *c, int mode)
{
   code_t*cp=(code_t*)c;
   VRF_STRT(NATION);
   VRF_INT(NATION, cp->code);
   VRF_STR(NATION, cp->text);
   VRF_INT(NATION, cp->join);
   VRF_STR(NATION, cp->comment);
   VRF_END(NATION);

   return(0);
}

int
vrf_region(void *c, int mode)
{
   code_t*cp=(code_t*)c;
   VRF_STRT(REGION);
   VRF_INT(REGION, cp->code);
   VRF_STR(REGION, cp->text);
   VRF_STR(REGION, cp->comment);
   VRF_END(fp);

   return(0);
}
#endif


#ifdef SSBM
int vrf_date(void * d, int /*mode*/)
{
    date_t * dp = (date_t*)d;
    VRF_STRT(DATE);
    VRF_INT(DATE, dp->datekey);
    VRF_STR(DATE, dp->date);
    VRF_STR(DATE, dp->dayofweek);
    VRF_STR(DATE, dp->month);
    VRF_INT(DATE, dp->year);
    VRF_INT(DATE, dp->yearmonthnum);
    VRF_STR(DATE, dp->yearmonth);
    VRF_INT(DATE, dp->daynuminweek);
    VRF_INT(DATE, dp->daynuminmonth);
    VRF_INT(DATE, dp->daynuminyear);
    VRF_INT(DATE, dp->monthnuminyear);
    VRF_INT(DATE, dp->weeknuminyear);
    VRF_STR(DATE, dp->sellingseason);
    VRF_STR(DATE, dp->lastdayinweekfl);
    VRF_STR(DATE, dp->lastdayinmonthfl);
    VRF_STR(DATE, dp->weekdayfl);
    VRF_END(DATE);
    return(0);

}
#endif

EXIT_NAMESPACE(dbgenssb)
