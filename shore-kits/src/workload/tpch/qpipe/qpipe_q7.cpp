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

/** @file:   qpipe_q7.cpp
 *
 *  @brief:  Implementation of QPIPE TPCH Q7 over Shore-MT
 *
 *  @author:    Andreas Schädeli
 *  @date:      2011-10-31
 */


#include "workload/tpch/shore_tpch_env.h"
#include "workload/tpch/tpch_util.h"
#include "qpipe.h"

using namespace shore;
using namespace qpipe;



ENTER_NAMESPACE(tpch);


/********************************************************************
 *
 * QPIPE Q7 - Structures needed by operators
 *
 ********************************************************************/

/*
select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) as revenue
from
    (select
    	n1.n_name as supp_nation,
    	n2.n_name as cust_nation,
    	extract(year) from l_shipdate as l_year,
    	l_extendedprice * (1-l_discount) as volume
    from
    	supplier,
    	lineitem,
    	orders,
    	customer,
    	nation n1,
    	nation n2
    where
    	s_suppkey = l_suppkey
    	and o_orderkey = l_orderkey
    	and c_custkey = o_custkey
    	and s_nationkey = n1.n_nationkey
    	and c_nationkey = n2.n_nationkey
    	and (
    		(n1.n_name = '[NATION1]' and n2.n_name = '[NATION2]')
    		or (n1.n_name = '[NATION2]' and n2.n_name = '[NATION1])
    		)
    	and l_shipdate between date '1995-01-01' and date '1996-12-31'
    ) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year;
*/

struct q7_projected_nation_tuple {
	int N_NATIONKEY;
	char N_NAME[STRSIZE(25)];
};

struct q7_projected_customer_tuple {
	int C_CUSTKEY;
	int C_NATIONKEY;
};

struct q7_projected_orders_tuple {
	int O_ORDERKEY;
	int O_CUSTKEY;
};

struct q7_projected_lineitem_tuple {
	int L_ORDERKEY;
	int L_SUPPKEY;
	int L_YEAR;
	decimal L_EXTENDEDPRICE;
	decimal L_DISCOUNT;
};

struct q7_projected_supplier_tuple {
	int S_SUPPKEY;
	int S_NATIONKEY;
};


struct q7_c_join_n2_tuple {
	int C_CUSTKEY;
	char CUST_NATION[STRSIZE(25)];
};

struct q7_o_join_c_n2_tuple {
	int O_ORDERKEY;
	char CUST_NATION[STRSIZE(25)];
};

struct q7_l_join_o_c_n2_tuple {
	int L_SUPPKEY;
	char CUST_NATION[STRSIZE(25)];
	int L_YEAR;
	decimal L_EXTENDEDPRICE;
	decimal L_DISCOUNT;
};

struct q7_l_o_c_n2_join_s_tuple {
	int S_NATIONKEY;
	char CUST_NATION[STRSIZE(25)];
	int L_YEAR;
	decimal L_EXTENDEDPRICE;
	decimal L_DISCOUNT;
};

struct q7_temp_agg_key {
	int S_NATIONKEY;
	char CUST_NATION[STRSIZE(25)];
	int L_YEAR;
};

struct q7_temp_aggregate_tuple {
	int S_NATIONKEY;
	char CUST_NATION[STRSIZE(25)];
	int L_YEAR;
	decimal VOLUME;
};

struct q7_final_tuple {
	char SUPP_NATION[STRSIZE(25)];
	char CUST_NATION[STRSIZE(25)];
	int L_YEAR;
	decimal REVENUE;
};


class q7_nation_tscan_filter_t : public tuple_filter_t
{
private:
    ShoreTPCHEnv* _tpchdb;
    table_row_t* _prnation;
    rep_row_t _rr;

    /*One nation tuple*/
    tpch_nation_tuple _nation;

    char _name1[STRSIZE(25)];
    char _name2[STRSIZE(25)];
    q7_input_t* q7_input;

public:
    q7_nation_tscan_filter_t(ShoreTPCHEnv* tpchdb, q7_input_t &in)
        : tuple_filter_t(tpchdb->nation_desc()->maxsize()), _tpchdb(tpchdb)
        {
        _prnation = _tpchdb->nation_man()->get_tuple();
        _rr.set_ts(_tpchdb->nation_man()->ts(),
            _tpchdb->nation_desc()->maxsize());
        _prnation->_rep = &_rr;

        q7_input = &in;
        nation_to_str(q7_input->n_name1, _name1);
        nation_to_str(q7_input->n_name2, _name2);

        TRACE(TRACE_ALWAYS, "Random Predicates:\nNATION.N_NAME = '%s' or NATION.N_NAME = '%s'\n", _name1, _name2);
        }

    virtual ~q7_nation_tscan_filter_t()
    {
        _tpchdb->nation_man()->give_tuple(_prnation);
    }

    bool select(const tuple_t &input) {

        // Get next nation tuple and read its size and type
        if (!_tpchdb->nation_man()->load(_prnation, input.data)) {
            assert(false); // RC(se_WRONG_DISK_DATA)
        }

        _prnation->get_value(1, _nation.N_NAME, 25);

        return (strcmp(_nation.N_NAME, _name1) == 0 || strcmp(_nation.N_NAME, _name2) == 0);
    }


    void project(tuple_t &d, const tuple_t &s) {

        q7_projected_nation_tuple *dest;
        dest = aligned_cast<q7_projected_nation_tuple>(d.data);

        _prnation->get_value(0, _nation.N_NATIONKEY);
        _prnation->get_value(1, _nation.N_NAME, 25);

        //TRACE( TRACE_RECORD_FLOW, "%d|%s\n",
        //       _nation.N_NATIONKEY, _nation.N_NAME);

        dest->N_NATIONKEY = _nation.N_NATIONKEY;
        memcpy(dest->N_NAME, _nation.N_NAME, sizeof(dest->N_NAME));
    }

    q7_nation_tscan_filter_t* clone() const {
        return new q7_nation_tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("q7_nation_tscan_filter_t(%s or %s)", _name1, _name2);
    }
};

class q7_customer_tscan_filter_t : public tuple_filter_t
{
    private:
        ShoreTPCHEnv* _tpchdb;
        table_row_t* _prcust;
        rep_row_t _rr;

        tpch_customer_tuple _customer;

    public:
        q7_customer_tscan_filter_t(ShoreTPCHEnv* tpchdb, q7_input_t &in)
            : tuple_filter_t(tpchdb->customer_desc()->maxsize()), _tpchdb(tpchdb)
        {
            _prcust = _tpchdb->customer_man()->get_tuple();
            _rr.set_ts(_tpchdb->customer_man()->ts(),
                       _tpchdb->customer_desc()->maxsize());
            _prcust->_rep = &_rr;
        }

        virtual ~q7_customer_tscan_filter_t()
        {
            // Give back the customer tuple
            _tpchdb->customer_man()->give_tuple(_prcust);
        }

        bool select(const tuple_t &input) {
            // Get next customer tuple
            if (!_tpchdb->customer_man()->load(_prcust, input.data)) {
                assert(false); // RC(se_WRONG_DISK_DATA)
            }

            return true;
        }

        void project(tuple_t &d, const tuple_t &s) {

            q7_projected_customer_tuple *dest = aligned_cast<q7_projected_customer_tuple>(d.data);

            _prcust->get_value(0, _customer.C_CUSTKEY);
            _prcust->get_value(3, _customer.C_NATIONKEY);

            //TRACE(TRACE_RECORD_FLOW, "%d|%d\n", _customer.C_CUSTKEY, _customer.C_NATIONKEY);

            dest->C_CUSTKEY = _customer.C_CUSTKEY;
            dest->C_NATIONKEY = _customer.C_NATIONKEY;
        }

        virtual q7_customer_tscan_filter_t* clone() const {
            return new q7_customer_tscan_filter_t(*this);
        }

        virtual c_str to_string() const {
            return c_str("q7_customer_tscan_filter_t");
        }
};

class q7_orders_tscan_filter_t : public tuple_filter_t
{
    private:
        ShoreTPCHEnv* _tpchdb;
        table_row_t* _prorders;
        rep_row_t _rr;

        tpch_orders_tuple _orders;

    public:
        q7_orders_tscan_filter_t(ShoreTPCHEnv* tpchdb, q7_input_t &in)
            : tuple_filter_t(tpchdb->orders_desc()->maxsize()), _tpchdb(tpchdb)
        {
            _prorders = _tpchdb->orders_man()->get_tuple();
            _rr.set_ts(_tpchdb->orders_man()->ts(),
                       _tpchdb->orders_desc()->maxsize());
            _prorders->_rep = &_rr;
        }

        virtual ~q7_orders_tscan_filter_t()
        {
            // Give back the orders tuple
            _tpchdb->orders_man()->give_tuple(_prorders);
        }

        bool select(const tuple_t &input) {
            // Get next orders tuple and read its orderdate
            if (!_tpchdb->orders_man()->load(_prorders, input.data)) {
                assert(false); // RC(se_WRONG_DISK_DATA)
            }

           return true;
        }

        void project(tuple_t &d, const tuple_t &s) {

            q7_projected_orders_tuple *dest = aligned_cast<q7_projected_orders_tuple>(d.data);

            _prorders->get_value(0, _orders.O_ORDERKEY);
            _prorders->get_value(1, _orders.O_CUSTKEY);

            //TRACE(TRACE_RECORD_FLOW, "%d|%d\n", _orders.O_ORDERKEY, _orders.O_CUSTKEY);

            dest->O_ORDERKEY = _orders.O_ORDERKEY;
            dest->O_CUSTKEY = _orders.O_CUSTKEY;
        }

        q7_orders_tscan_filter_t* clone() const {
            return new q7_orders_tscan_filter_t(*this);
        }

        c_str to_string() const {
            return c_str("q7_orders_tscan_filter_t");
        }
};

class q7_lineitem_tscan_filter_t : public tuple_filter_t
{
    private:
        ShoreTPCHEnv* _tpchdb;
        table_row_t* _prline;
        rep_row_t _rr;

        tpch_lineitem_tuple _lineitem;
        time_t _shipdate;

        time_t _firstdate;
        time_t _lastdate;

    public:
        q7_lineitem_tscan_filter_t(ShoreTPCHEnv* tpchdb, q7_input_t &in)
            : tuple_filter_t(tpchdb->lineitem_desc()->maxsize()), _tpchdb(tpchdb)
        {
            _prline = _tpchdb->lineitem_man()->get_tuple();
            _rr.set_ts(_tpchdb->lineitem_man()->ts(),
                       _tpchdb->lineitem_desc()->maxsize());
            _prline->_rep = &_rr;

            _firstdate = str_to_timet("1995-01-01");
            _lastdate = str_to_timet("1996-12-31");
        }

        virtual ~q7_lineitem_tscan_filter_t()
        {
            // Give back the lineitem tuple
            _tpchdb->lineitem_man()->give_tuple(_prline);
        }

        bool select(const tuple_t &input) {
            // Get next lineitem tuple and read its shipdate
            if (!_tpchdb->lineitem_man()->load(_prline, input.data)) {
                assert(false); // RC(se_WRONG_DISK_DATA)
            }

            _prline->get_value(10, _lineitem.L_SHIPDATE, 15);
            _shipdate = str_to_timet(_lineitem.L_SHIPDATE);

            return (_shipdate >= _firstdate && _shipdate <= _lastdate);
        }

        void project(tuple_t &d, const tuple_t &s) {

            q7_projected_lineitem_tuple *dest = aligned_cast<q7_projected_lineitem_tuple>(d.data);

            _prline->get_value(0, _lineitem.L_ORDERKEY);
            _prline->get_value(2, _lineitem.L_SUPPKEY);
            _prline->get_value(5, _lineitem.L_EXTENDEDPRICE);
            _prline->get_value(6, _lineitem.L_DISCOUNT);
            _prline->get_value(10, _lineitem.L_SHIPDATE, 15);
            _shipdate = str_to_timet(_lineitem.L_SHIPDATE);
            struct tm *tm_shipdate = gmtime(&_shipdate);

            //TRACE(TRACE_RECORD_FLOW, "%d|%d|%.2f|%.2f|%d\n", _lineitem.L_ORDERKEY, _lineitem.L_SUPPKEY, _lineitem.L_EXTENDEDPRICE / 100.0, _lineitem.L_DISCOUNT / 100.0,
            //													tm_shipdate->tm_year + 1900);

            dest->L_ORDERKEY = _lineitem.L_ORDERKEY;
            dest->L_SUPPKEY = _lineitem.L_SUPPKEY;
            dest->L_EXTENDEDPRICE = _lineitem.L_EXTENDEDPRICE / 100.0;
#warning MA: Discount from TPCH dbgen is created between 0 and 100 instead between 0 and 1.
            dest->L_DISCOUNT = _lineitem.L_DISCOUNT / 100.0;
            dest->L_YEAR = tm_shipdate->tm_year + 1900;
            /*char ryear[5];
            strncpy(ryear, _lineitem.L_SHIPDATE, 4);
            ryear[4] = '\0';
            if(dest->L_YEAR != atoi(ryear)) TRACE(TRACE_RECORD_FLOW, "%d\t%d\t%s\t%s\n", dest->L_ORDERKEY, dest->L_YEAR, ryear, _lineitem.L_SHIPDATE);*/
        }

        q7_lineitem_tscan_filter_t* clone() const {
            return new q7_lineitem_tscan_filter_t(*this);
        }

        c_str to_string() const {
            return c_str("q7_lineitem_tscan_filter_t(between(%s, %s))", ctime(&(_firstdate)), ctime(&(_lastdate)));
        }
};

class q7_supplier_tscan_filter_t : public tuple_filter_t
{
private:
    ShoreTPCHEnv* _tpchdb;
    table_row_t* _prsupplier;
    rep_row_t _rr;

    /*One supplier tuple*/
    tpch_supplier_tuple _supplier;

public:
    q7_supplier_tscan_filter_t(ShoreTPCHEnv* tpchdb, q7_input_t &in)
        : tuple_filter_t(tpchdb->supplier_desc()->maxsize()), _tpchdb(tpchdb)
        {
        _prsupplier = _tpchdb->supplier_man()->get_tuple();
        _rr.set_ts(_tpchdb->supplier_man()->ts(),
            _tpchdb->supplier_desc()->maxsize());
        _prsupplier->_rep = &_rr;
        }

    virtual ~q7_supplier_tscan_filter_t()
    {
        _tpchdb->supplier_man()->give_tuple(_prsupplier);
    }

    bool select(const tuple_t &input) {

        // Get next supplier tuple
        if (!_tpchdb->supplier_man()->load(_prsupplier, input.data)) {
            assert(false); // RC(se_WRONG_DISK_DATA)
        }

        return true;
    }


    void project(tuple_t &d, const tuple_t &s) {

        q7_projected_supplier_tuple *dest = aligned_cast<q7_projected_supplier_tuple>(d.data);


        _prsupplier->get_value(0, _supplier.S_SUPPKEY);
        _prsupplier->get_value(3, _supplier.S_NATIONKEY);

        //TRACE( TRACE_RECORD_FLOW, "%d|%d\n",
        //       _supplier.S_SUPPKEY, _supplier.S_NATIONKEY);

        dest->S_SUPPKEY = _supplier.S_SUPPKEY;
        dest->S_NATIONKEY = _supplier.S_NATIONKEY;

    }

    q7_supplier_tscan_filter_t* clone() const {
        return new q7_supplier_tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("q7_supplier_tscan_filter_t");
    }
};


struct q7_c_join_n2_t : public tuple_join_t {

	q7_c_join_n2_t()
		: tuple_join_t(sizeof(q7_projected_customer_tuple),
						offsetof(q7_projected_customer_tuple, C_NATIONKEY),
						sizeof(q7_projected_nation_tuple),
						offsetof(q7_projected_nation_tuple, N_NATIONKEY),
						sizeof(int),
						sizeof(q7_c_join_n2_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q7_c_join_n2_tuple *dest = aligned_cast<q7_c_join_n2_tuple>(d.data);
		q7_projected_customer_tuple *cust = aligned_cast<q7_projected_customer_tuple>(l.data);
		q7_projected_nation_tuple *nation = aligned_cast<q7_projected_nation_tuple>(r.data);

		dest->C_CUSTKEY = cust->C_CUSTKEY;
		memcpy(dest->CUST_NATION, nation->N_NAME, sizeof(dest->CUST_NATION));

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d = %d: %d %s\n", cust->C_NATIONKEY, nation->N_NATIONKEY, cust->C_CUSTKEY, nation->N_NAME);
	}

	virtual q7_c_join_n2_t* clone() const {
		return new q7_c_join_n2_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("JOIN CUSTOMER, NATION n2; select C_CUSTKEY, n2.N_NAME as CUST_NAME");
	}
};

struct q7_o_join_c_n2_t : public tuple_join_t {

	q7_o_join_c_n2_t()
		: tuple_join_t(sizeof(q7_projected_orders_tuple),
						offsetof(q7_projected_orders_tuple, O_CUSTKEY),
						sizeof(q7_c_join_n2_tuple),
						offsetof(q7_c_join_n2_tuple, C_CUSTKEY),
						sizeof(int),
						sizeof(q7_o_join_c_n2_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q7_o_join_c_n2_tuple *dest = aligned_cast<q7_o_join_c_n2_tuple>(d.data);
		q7_projected_orders_tuple *orders = aligned_cast<q7_projected_orders_tuple>(l.data);
		q7_c_join_n2_tuple *right = aligned_cast<q7_c_join_n2_tuple>(r.data);

		dest->O_ORDERKEY = orders->O_ORDERKEY;
		memcpy(dest->CUST_NATION, right->CUST_NATION, sizeof(dest->CUST_NATION));

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d = %d: %d %s\n", orders->O_ORDERKEY, right->CUST_NATION);
	}

	virtual q7_o_join_c_n2_t* clone() const {
		return new q7_o_join_c_n2_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("JOIN ORDERS, CUSTOMER_NATION; select O_ORDERKEY, CUST_NAME");
	}
};

struct q7_l_join_o_c_n2_t : public tuple_join_t {

	q7_l_join_o_c_n2_t()
		: tuple_join_t(sizeof(q7_projected_lineitem_tuple),
						offsetof(q7_projected_lineitem_tuple, L_ORDERKEY),
						sizeof(q7_o_join_c_n2_tuple),
						offsetof(q7_o_join_c_n2_tuple, O_ORDERKEY),
						sizeof(int),
						sizeof(q7_l_join_o_c_n2_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q7_l_join_o_c_n2_tuple *dest = aligned_cast<q7_l_join_o_c_n2_tuple>(d.data);
		q7_projected_lineitem_tuple *line = aligned_cast<q7_projected_lineitem_tuple>(l.data);
		q7_o_join_c_n2_tuple *right = aligned_cast<q7_o_join_c_n2_tuple>(r.data);

		dest->L_SUPPKEY = line->L_SUPPKEY;
		memcpy(dest->CUST_NATION, right->CUST_NATION, sizeof(dest->CUST_NATION));
		dest->L_EXTENDEDPRICE = line->L_EXTENDEDPRICE;
		dest->L_DISCOUNT = line->L_DISCOUNT;
		dest->L_YEAR = line->L_YEAR;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d = %d: %d %s %.2f %.2f %d", line->L_SUPPKEY, right->CUST_NATION, line->L_EXTENDEDPRICE.to_double(),
		//																line->L_DISCOUNT.to_double(), line->L_YEAR);
	}

	virtual q7_l_join_o_c_n2_t* clone() const {
		return new q7_l_join_o_c_n2_t(*this);
	}

	virtual c_str to_string() const {
		return "JOIN LINEITEM, ORDERS_CUSTOMER_NATION; select L_SUPPKEY, CUST_NATION, L_EXTENDEDPRICE, L_DISCOUNT, L_YEAR";
	}
};

struct q7_l_o_c_n2_join_s_t : public tuple_join_t {

	q7_l_o_c_n2_join_s_t()
		: tuple_join_t(sizeof(q7_l_join_o_c_n2_tuple),
						offsetof(q7_l_join_o_c_n2_tuple, L_SUPPKEY),
						sizeof(q7_projected_supplier_tuple),
						offsetof(q7_projected_supplier_tuple, S_SUPPKEY),
						sizeof(int),
						sizeof(q7_l_o_c_n2_join_s_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q7_l_o_c_n2_join_s_tuple *dest = aligned_cast<q7_l_o_c_n2_join_s_tuple>(d.data);
		q7_l_join_o_c_n2_tuple *left = aligned_cast<q7_l_join_o_c_n2_tuple>(l.data);
		q7_projected_supplier_tuple *supplier = aligned_cast<q7_projected_supplier_tuple>(r.data);

		dest->S_NATIONKEY = supplier->S_NATIONKEY;
		memcpy(dest->CUST_NATION, left->CUST_NATION, sizeof(dest->CUST_NATION));
		dest->L_EXTENDEDPRICE = left->L_EXTENDEDPRICE;
		dest->L_DISCOUNT = left->L_DISCOUNT;
		dest->L_YEAR = left->L_YEAR;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d = %d: %d %s %.2f %.2f %d", left->L_SUPPKEY, supplier->S_SUPPKEY, left->L_EXTENDEDPRICE.to_double(),
		//																left->L_DISCOUNT.to_double(), left->L_YEAR);
	}

	virtual q7_l_o_c_n2_join_s_t* clone() const {
		return new q7_l_o_c_n2_join_s_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("JOIN LINEITEM_ORDERS_CUSTOMER_NATION, SUPPLIER; select S_NATIONKEY, CUST_NATION, L_EXTENDEDPRICE, L_DISCOUNT, L_YEAR");
	}
};

class q7_temp_aggregate_t : public tuple_aggregate_t {

private:

	default_key_extractor_t _extractor;

public:

	q7_temp_aggregate_t()
		: tuple_aggregate_t(sizeof(q7_temp_aggregate_tuple)), _extractor(2 * sizeof(int) + STRSIZE(25), offsetof(q7_temp_aggregate_tuple, S_NATIONKEY))
	{
	}

	virtual key_extractor_t* key_extractor() {return &_extractor;}

	virtual void aggregate(char *agg_data, const tuple_t &s) {
		q7_temp_aggregate_tuple *agg = aligned_cast<q7_temp_aggregate_tuple>(agg_data);
		q7_l_o_c_n2_join_s_tuple *src = aligned_cast<q7_l_o_c_n2_join_s_tuple>(s.data);

		agg->VOLUME += src->L_EXTENDEDPRICE * (1 - src->L_DISCOUNT);
	}

	virtual void finish(tuple_t &d, const char* agg_data) {
		memcpy(d.data, agg_data, tuple_size());
	}

	virtual q7_temp_aggregate_t* clone() const {
		return new q7_temp_aggregate_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("q7_temp_aggregate_t");
	}
};

struct q7_final_join_t : public tuple_join_t {

	q7_final_join_t()
		: tuple_join_t(sizeof(q7_projected_nation_tuple),
						offsetof(q7_projected_nation_tuple, N_NATIONKEY),
						sizeof(q7_temp_aggregate_tuple),
						offsetof(q7_temp_aggregate_tuple, S_NATIONKEY),
						sizeof(int),
						sizeof(q7_final_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q7_final_tuple *dest = aligned_cast<q7_final_tuple>(d.data);
		q7_projected_nation_tuple *nation = aligned_cast<q7_projected_nation_tuple>(l.data);
		q7_temp_aggregate_tuple *temp = aligned_cast<q7_temp_aggregate_tuple>(r.data);

		memcpy(dest->SUPP_NATION, nation->N_NAME, sizeof(dest->SUPP_NATION));
		memcpy(dest->CUST_NATION, temp->CUST_NATION, sizeof(dest->CUST_NATION));
		dest->L_YEAR = temp->L_YEAR;
		dest->REVENUE = temp->VOLUME;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d = %d: %s %s %d %.2f", nation->N_NATIONKEY, temp->S_NATIONKEY, nation->N_NAME, temp->CUST_NATION, temp->L_YEAR, temp->VOLUME.to_double());
	}

	virtual q7_final_join_t* clone() const {
		return new q7_final_join_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("JOIN NATION n1, LINEITEM_ORDERS_CUSTOMER_NATION_SUPPLIER; select n1.N_NAME as SUPP_NATION, CUST_NATION, L_YEAR, REVENUE");
	}
};

struct q7_join_nation_filter_t : public tuple_filter_t {

	q7_join_nation_filter_t()
	: tuple_filter_t(sizeof(q7_final_tuple))
	{
	}

	bool select(const tuple_t &input) {
		q7_final_tuple *in = aligned_cast<q7_final_tuple>(input.data);
		return (strcmp(in->CUST_NATION, in->SUPP_NATION) != 0);
	}

	virtual q7_join_nation_filter_t* clone() const {
		return new q7_join_nation_filter_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("q7_join_nation_filter_t");
	}
};

struct q7_temp_agg_key_compare_t : public key_compare_t {

	virtual int operator()(const void* key1, const void* key2) const {
		q7_temp_agg_key *k1 = aligned_cast<q7_temp_agg_key>(key1);
		q7_temp_agg_key *k2 = aligned_cast<q7_temp_agg_key>(key2);
		int diff_key = k1->S_NATIONKEY - k2->S_NATIONKEY;
		int diff_name = strcmp(k1->CUST_NATION, k2->CUST_NATION);
		int diff_year = k1->L_YEAR - k2->L_YEAR;
		return (diff_key != 0 ? diff_key : (diff_name != 0 ? diff_name : diff_year));
	}

	virtual q7_temp_agg_key_compare_t* clone() const {
		return new q7_temp_agg_key_compare_t(*this);
	}
};

struct q7_key_extractor_t : public key_extractor_t {

	q7_key_extractor_t()
	: key_extractor_t(sizeof(q7_final_tuple))
	{
	}

	virtual int extract_hint(const char* key) const {
		char *k;
		k = aligned_cast<char>(key);

		int result = (*k << 24) + (*(k + sizeof(char)) << 16) + (*(k + 2*sizeof(char)) << 8) + *(k + 3*sizeof(char));

		return result;
	}

	virtual q7_key_extractor_t* clone() const {
		return new q7_key_extractor_t(*this);
	}
};

struct q7_key_compare_t : public key_compare_t {

	virtual int operator()(const void* key1, const void* key2) const {
		q7_final_tuple* t1 = aligned_cast<q7_final_tuple>(key1);
		q7_final_tuple* t2 = aligned_cast<q7_final_tuple>(key2);
		int diff_n1 = strcmp(t1->SUPP_NATION, t2->SUPP_NATION);
		int diff_n2 = strcmp(t1->CUST_NATION, t2->CUST_NATION);
		int diff_year = t1->L_YEAR - t2->L_YEAR;

		return (diff_n1 != 0 ? diff_n1 : (diff_n2 != 0 ? diff_n2 : diff_year));
	}

	virtual q7_key_compare_t* clone() const {
		return new q7_key_compare_t(*this);
	}
};

class tpch_q7_process_tuple_t : public process_tuple_t {

public:

    virtual void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** Q7 %25s %25s %25s %25s\n",
              "SUPP_NATION", "CUST_NATION", "L_YEAR", "REVENUE");
    }

    virtual void process(const tuple_t& output) {
        q7_final_tuple *res = aligned_cast<q7_final_tuple>(output.data);

        TRACE(TRACE_QUERY_RESULTS, "*** Q7 %25s %25s %25d %25.2f\n",
	      res->SUPP_NATION, res->CUST_NATION, res->L_YEAR, res->REVENUE.to_double());
    }
};

/********************************************************************
 *
 * QPIPE Q5 - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_q7(const int xct_id,
                                  q7_input_t& in)
{

	TRACE( TRACE_ALWAYS, "********** Q7 *********\n");

	policy_t* dp = this->get_sched_policy();
	xct_t* pxct = smthread_t::me()->xct();

	//TSCAN NATION n2
	tuple_fifo* q7_nation_n2_buffer = new tuple_fifo(sizeof(q7_projected_nation_tuple));
	packet_t* q7_nation_n2_tscan_packet =
			new tscan_packet_t("nation n2 TSCAN",
								q7_nation_n2_buffer,
								new q7_nation_tscan_filter_t(this, in),
								this->db(),
								_pnation_desc.get(),
								pxct);

	//TSCAN CUSTOMER
	tuple_fifo* q7_customer_buffer = new tuple_fifo(sizeof(q7_projected_customer_tuple));
	packet_t* q7_customer_tscan_packet =
			new tscan_packet_t("customer TSCAN",
								q7_customer_buffer,
								new q7_customer_tscan_filter_t(this, in),
								this->db(),
								_pcustomer_desc.get(),
								pxct);

	//CUSTOMER JOIN NATION(n2)
	tuple_fifo* q7_c_join_n2_buffer = new tuple_fifo(sizeof(q7_c_join_n2_tuple));
	packet_t* q7_c_join_n2_packet =
			new hash_join_packet_t("customer - nation(n2) HJOIN",
									q7_c_join_n2_buffer,
									new trivial_filter_t(sizeof(q7_c_join_n2_tuple)),
									q7_customer_tscan_packet,
									q7_nation_n2_tscan_packet,
									new q7_c_join_n2_t());

	//TSCAN ORDERS
	tuple_fifo* q7_orders_buffer = new tuple_fifo(sizeof(q7_projected_orders_tuple));
	packet_t* q7_orders_tscan_packet =
			new tscan_packet_t("orders TSCAN",
								q7_orders_buffer,
								new q7_orders_tscan_filter_t(this, in),
								this->db(),
								_porders_desc.get(),
								pxct);

	//ORDERS JOIN CUSTOMER_NATION
	tuple_fifo* q7_o_join_c_n2_buffer = new tuple_fifo(sizeof(q7_o_join_c_n2_tuple));
	packet_t* q7_o_join_c_n2_packet =
			new hash_join_packet_t("orders - customer_nation HJOIN",
									q7_o_join_c_n2_buffer,
									new trivial_filter_t(sizeof(q7_o_join_c_n2_tuple)),
									q7_orders_tscan_packet,
									q7_c_join_n2_packet,
									new q7_o_join_c_n2_t());

	//TSCAN LINEITEM
	tuple_fifo* q7_lineitem_buffer = new tuple_fifo(sizeof(q7_projected_lineitem_tuple));
	packet_t* q7_lineitem_tscan_packet =
			new tscan_packet_t("lineitem TSCAN",
								q7_lineitem_buffer,
								new q7_lineitem_tscan_filter_t(this, in),
								this->db(),
								_plineitem_desc.get(),
								pxct);

	//LINEITEM JOIN ORDERS_CUSTOMER_NATION
	tuple_fifo* q7_l_join_o_c_n2_buffer = new tuple_fifo(sizeof(q7_l_join_o_c_n2_tuple));
	packet_t* q7_l_join_o_c_n2_packet =
			new hash_join_packet_t("lineitem - orders_customer_nation HJOIN",
									q7_l_join_o_c_n2_buffer,
									new trivial_filter_t(sizeof(q7_l_join_o_c_n2_tuple)),
									q7_lineitem_tscan_packet,
									q7_o_join_c_n2_packet,
									new q7_l_join_o_c_n2_t());

	//TSCAN SUPPLIER
	tuple_fifo* q7_supplier_buffer = new tuple_fifo(sizeof(q7_projected_supplier_tuple));
	packet_t* q7_supplier_tscan_packet =
			new tscan_packet_t("supplier TSCAN",
								q7_supplier_buffer,
								new q7_supplier_tscan_filter_t(this, in),
								this->db(),
								_psupplier_desc.get(),
								pxct);

	//LINEITEM_ORDERS_CUSTOMER_NATION JOIN SUPPLIER
	tuple_fifo* q7_l_o_c_n2_join_s_buffer = new tuple_fifo(sizeof(q7_l_o_c_n2_join_s_tuple));
	packet_t* q7_l_o_c_n2_join_s_packet =
			new hash_join_packet_t("lineitem_orders_customer_nation - supplier HJOIN",
									q7_l_o_c_n2_join_s_buffer,
									new trivial_filter_t(sizeof(q7_l_o_c_n2_join_s_tuple)),
									q7_l_join_o_c_n2_packet,
									q7_supplier_tscan_packet,
									new q7_l_o_c_n2_join_s_t());

	//TEMP AGGREGATE
	tuple_fifo* q7_temp_aggregate_buffer = new tuple_fifo(sizeof(q7_temp_aggregate_tuple));
	packet_t* q7_temp_aggregate_packet =
			new partial_aggregate_packet_t("SUM AGG",
											q7_temp_aggregate_buffer,
											new trivial_filter_t(sizeof(q7_temp_aggregate_tuple)),
											q7_l_o_c_n2_join_s_packet,
											new q7_temp_aggregate_t(),
											new default_key_extractor_t(2*sizeof(int) + STRSIZE(25) * sizeof(char), offsetof(q7_l_o_c_n2_join_s_tuple, S_NATIONKEY)),
											new q7_temp_agg_key_compare_t());

	//TSCAN NATION n1
	tuple_fifo* q7_nation_n1_buffer = new tuple_fifo(sizeof(q7_projected_nation_tuple));
	packet_t* q7_nation_n1_tscan_packet =
			new tscan_packet_t("nation n1 TSCAN",
								q7_nation_n1_buffer,
								new q7_nation_tscan_filter_t(this, in),
								this->db(),
								_pnation_desc.get(),
								pxct);

	//NATION JOIN LINEITEM_ORDERS_CUSTOMER_NATION_SUPPLIER
	tuple_fifo* q7_all_join_buffer = new tuple_fifo(sizeof(q7_final_tuple));
	packet_t* q7_all_join_packet =
			new hash_join_packet_t("nation(n1) - lineitem_orders_customer_nation_supplier HJOIN",
									q7_all_join_buffer,
									new q7_join_nation_filter_t(),
									q7_nation_n1_tscan_packet,
									q7_temp_aggregate_packet,
									new q7_final_join_t());

	//SORT
	tuple_fifo* q7_final_buffer = new tuple_fifo(sizeof(q7_final_tuple));
	packet_t* q7_sort_packet =
			new sort_packet_t("SORT",
								q7_final_buffer,
								new trivial_filter_t(sizeof(q7_final_tuple)),
								new q7_key_extractor_t(),
								new q7_key_compare_t(),
								q7_all_join_packet);

	qpipe::query_state_t* qs = dp->query_state_create();
	q7_nation_n2_tscan_packet->assign_query_state(qs);
	q7_customer_tscan_packet->assign_query_state(qs);
	q7_c_join_n2_packet->assign_query_state(qs);
	q7_orders_tscan_packet->assign_query_state(qs);
	q7_o_join_c_n2_packet->assign_query_state(qs);
	q7_lineitem_tscan_packet->assign_query_state(qs);
	q7_l_join_o_c_n2_packet->assign_query_state(qs);
	q7_supplier_tscan_packet->assign_query_state(qs);
	q7_l_o_c_n2_join_s_packet->assign_query_state(qs);
	q7_temp_aggregate_packet->assign_query_state(qs);
	q7_nation_n1_tscan_packet->assign_query_state(qs);
	q7_all_join_packet->assign_query_state(qs);
	q7_sort_packet->assign_query_state(qs);

	// Dispatch packet
	tpch_q7_process_tuple_t pt;
	//LAST PACKET
	process_query(q7_sort_packet, pt);//TODO

	dp->query_state_destroy(qs);


	return(RCOK);
}

EXIT_NAMESPACE(tpch)
