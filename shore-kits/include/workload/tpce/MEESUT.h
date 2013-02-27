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

/** @file:   MEESUT.h
 *
 *  @brief:  ??
 *
 *  @author: Djordje Jevdjic
 */


#ifndef MEE_SUT_H
#define MEE_SUT_H

#include "sm/shore/shore_env.h"
#include "sm/shore/shore_trx_worker.h"
#include "workload/tpce/egen/MEESUTInterface.h"

using namespace TPCE;

ENTER_NAMESPACE(tpce);
const int max_buffer = 512000;

template <typename T>
class InputBuffer 
{
    mcs_lock a_lock;
    T* buffer[max_buffer];
    int size, first, last;
    //    int flag;
public:
    InputBuffer():size(0), first(0), last(0)//, flag(0)
    {};	
    bool isEmpty(){
	{
	    CRITICAL_SECTION(cs, a_lock);
	    //	    if(flag==1) return true;    
	    if(size==0) return true;
	    else {
		//flag=1;
		return false;
	    }              
	}
    }
    T* get(){
	{
	    CRITICAL_SECTION(cs, a_lock);
	    if (size==0) return NULL;
	    T* tmp=buffer[first];
	    size--;
	    first=(first+1)%max_buffer;
	    //	    flag=0; 
	    return tmp;
	}
    }

    void put(T* tmp){
	{
	    CRITICAL_SECTION(cs, a_lock);
	    //assert(size<max_buffer);
	    if(size < max_buffer) {
		buffer[last]=tmp;
		last=(last+1)%max_buffer;
		size++;
	    }
	}
    }
};

class MFBuffer: public InputBuffer<TMarketFeedTxnInput>{
};

class TRBuffer: public InputBuffer<TTradeResultTxnInput>{
};

extern MFBuffer* MarketFeedInputBuffer;
extern TRBuffer* TradeResultInputBuffer;


class CMEESUT: public CMEESUTInterface
{
    MFBuffer* MFQueue;
    TRBuffer* TRQueue;
	
public:
    void setMFQueue(MFBuffer* p){ MFQueue = p;}
    void setTRQueue(TRBuffer* p){ TRQueue = p;}

    bool TradeResult( PTradeResultTxnInput pTxnInput ) {
	PTradeResultTxnInput trInput= new TTradeResultTxnInput();
	memcpy(trInput, pTxnInput, sizeof(TTradeResultTxnInput));
	TRQueue->put(trInput);
	return true;
    }

    bool MarketFeed( PMarketFeedTxnInput pTxnInput ){
	PMarketFeedTxnInput mfInput= new TMarketFeedTxnInput();
	memcpy(mfInput, pTxnInput, sizeof(TMarketFeedTxnInput));
	MFQueue->put(mfInput);
	return true;
    }

};

EXIT_NAMESPACE(tpce);


#endif //MEE_SUT
