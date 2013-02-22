#define private public
#define protected public

#include "w_defines.h"
#include "w_base.h"
#include "w_rc.h"

#include "sthread.h"

#include "latch.h"
#include "basics.h"
#include "stid_t.h"

#include "sm_base.h"
#include "sm_int_0.h"
#include "lid_t.h"
#include "sm_s.h"
#include "bf.h"
#include "bf_s.h"
#include "bf_core.h"
#include "page_s.h"

#include "page.h"
#include "page_h.h"
#include "sm_int_1.h"
#include "lock.h"
#include "lock_s.h"

#include "pmap.h"
#include "sm_io.h"
#include "log.h"
#include "logrec.h"
#include "xct.h"
#include "xct_impl.h"
#include "xct_dependent.h"
#include "lock_x.h"
#include "lock_core.h"
#include "kvl_t.h"
#include <sstream>
#include <map>
#include <stdio.h>

/*
  CC -DHAVE_CONFIG_H -I/raid/small/ryanjohn/projects/shore-elr-manos/config -g      -DSOLARIS2 -DSTHREAD_CORE_PTHREAD -DARCH_LP64 -DNO_FASTNEW -D_REENTRANT -mt -xtarget=native64 -xdebugformat=stabs -features=extensions   -DW_LARGEFILE -I/raid/small/ryanjohn/projects/shore-elr-manos/src/fc -I/raid/small/ryanjohn/projects/shore-elr-manos/src/sthread -I/raid/small/ryanjohn/projects/shore-elr-manos/src/common -I/raid/small/ryanjohn/projects/shore-elr-manos/src/sm -xcode=pic13 -G -o debug-shore.so debug-shore.cpp -I/raid/small/ryanjohn/projects/ppmcs -features=zla -library=stlport4
  
 */
// stolen from lock_core.cpp...
class bucket_t {
public:

#ifndef NOT_PREEMPTIVE
#ifndef ONE_MUTEX
  DEF_LOCK_X;
    lock_x			mutex;		// serialize access to lock_info_t
#endif
#endif
    w_list_t<lock_head_t> 	    chain;

    NORET			    bucket_t() :
#ifndef NOT_PREEMPTIVE
#ifndef ONE_MUTEX
      //		    mutex("lkbkt"),
#endif
#endif
		    chain(W_LIST_ARG(lock_head_t, chain)) {
    }

    private:
    // disabled
    NORET			    bucket_t(const bucket_t&);
    bucket_t& 		    operator=(const bucket_t&);
};

class page_mark_t {
public:
    int2_t idx;
    uint2_t len;
};

char const* page_tag_to_str(int page_tag) {
    switch (page_tag) {
    case page_p::t_extlink_p: 
	return "t_extlink_p";
    case page_p::t_stnode_p:
	return "t_stnode_p";
    case page_p::t_keyed_p:
	return "t_keyed_p";
    case page_p::t_btree_p:
	return "t_btree_p";
    case page_p::t_rtree_p:
	return "t_rtree_p";
    case page_p::t_file_p:
	return "t_file_p";
    default:
	return "<garbage>";
    }
}

char const* pretty_print(logrec_t const* rec, int i=0, char const* s=0) {
    static char tmp_data[1024];
    static char type_data[1024];

    switch(rec->type()) {
    case logrec_t::t_page_mark:
    case logrec_t::t_page_reclaim:
	{
	    page_mark_t* pm = (page_mark_t*) rec->_data;
	    snprintf(type_data, sizeof(type_data),
		     "        idx = %d\n"
		     "        len = %d\n",
		     pm->idx, pm->len);
	    break;
	}
    default:
	snprintf(type_data, sizeof(type_data), "");
    }
    
    // what categories?
    typedef char const* str;
    str undo=(rec->is_undo()? "undo " : ""),
	redo=(rec->is_redo()? "redo " : ""),
	cpsn=(rec->is_cpsn()? "cpsn " : ""),
	logical=(rec->is_logical()? "logical " : "");
    snprintf(tmp_data, sizeof(tmp_data),
	     "{\n"
	     "    _len = %d\n"
	     "    _type = %s\n"
	     "%s"
	     "    _cat = %s%s%s%s\n"
	     "    _tid = %d.%d\n"
	     "    _pid = %d.%d.%d\n"
	     "    _page_tag = %s\n"
	     "    _prev = %d.%ld\n"
	     "    _ck_lsn = %d.%ld\n"
	     "}",
	     rec->length(),
	     rec->type_str(), type_data,
	     logical, undo, redo, cpsn,
	     rec->tid().get_hi(), rec->tid().get_lo(),
	     rec->construct_pid().vol().vol, rec->construct_pid().store(), rec->construct_pid().page,
	     page_tag_to_str(rec->tag()),
	     rec->prev().hi(), rec->prev().lo(),
	     rec->get_lsn_ck().hi(), rec->get_lsn_ck().lo()
	     );
    return tmp_data;
}



void scan_lock_pool() {
  typedef std::map<int, int> lmap;
  lmap lengths;
  lock_core_m* core = smlevel_0::lm->_core;
  for(int i=0; i < core->_htabsz; i++)
    lengths[core->_htab[i].chain.num_members()]++;

  printf("Bucket density histogram:\n");
  for(lmap::iterator it=lengths.begin(); it != lengths.end(); ++it)
    printf("\t%d: %d\n", it->first, it->second);
  printf("\n");
}
#if 0
void debug_dump_waits_for(w_descend_list_t<xct_t, tid_t>* xlist=&xct_t::_xlist, bool acquire=false) {
  if(acquire) W_COERCE(xct_t::acquire_xlist_mutex());
  std::map<lock_head_t*, int> lockmap;
    w_list_i<xct_t> i(*xlist);
    while ( xct_t* me = i.next() )  {
      if( lock_request_t* mine = me->lock_info()->wait ) {
	lock_head_t* lock = mine->get_lock_head();
	ostringstream os;
	os << lock->name << ends << std::flush;
	std::string str = os.str();
	char const* lname = str.c_str();
	printf("\"0x%p\" -> \"%s\"\n", mine->thread, lname);

	if(++lockmap[lock] == 1) {
	  // print out all current lock holders as dependencies. This
	  // isn't perfect because other waiting requests ahead of me
	  // are also a problem, but it should be enough to get the job
	  // done.
	  w_list_i<lock_request_t> i2(lock->queue);
	  while(lock_request_t* theirs = i2.next()) {
	    if(mine->status() == lock_m::t_converting && theirs->status() == lock_m::t_waiting)
	      break;
	    if(mine->status() == lock_m::t_waiting && theirs == mine)
	      break;
	    
	    sthread_t* thread = theirs->thread;
	    printf("\"%s\" -> \"0x%p\" [ color=%s style=%s]\n",
		   lname, thread? thread : 0,
		   theirs->mode == EX? "red" : "blue",
		   theirs->status() == lock_m::t_waiting? "dotted" : "solid");
	  }
	}
      }
    }  
  
    if(acquire) xct_t::release_xlist_mutex();
}
#endif

bfcb_t* verify_page_lsn() {
  for(int i=0; i < bf_m::_core->_num_bufs; i++) {
    bfcb_t &b = bf_m::_core->_buftab[i];
    if(b.pid.page && b.dirty) {
      if(b.rec_lsn > b.frame->lsn1)
	return &b;
    }
  }
  return 0;
}
char const* db_pretty_print(logrec_t const* rec, int i=0, char const* s=0);

int vnum_to_match = 1;
int snum_to_match;
int pnum_to_match;
void match_page(logrec_t* rp) {
  lpid_t log_pid = rp->construct_pid();
  lpid_t test_pid(vnum_to_match, snum_to_match, pnum_to_match);
  if(log_pid == test_pid) {
      fprintf(stderr, "%s\n", pretty_print(rp));
  }
}

int tidhi_to_match = 0;
int tidlo_to_match;
void match_trx(logrec_t* rp) {
  tid_t log_tid = rp->tid();
  tid_t test_tid(tidlo_to_match, tidhi_to_match);
  if(log_tid == test_tid) {
      fprintf(stderr, "%s\n", pretty_print(rp));
  }
}
void match_all(logrec_t* rp) {
      fprintf(stderr, "%s\n", pretty_print(rp));
}
void match_n(logrec_t* rp) {
}

std::map<lpid_t, lsn_t> page_deps;
std::map<tid_t, lsn_t> tid_deps;

bool lpid_t::operator<(lpid_t const &other) const {
    return page < other.page;
}

struct log_dump_file {
    FILE* _f;
    log_dump_file() : _f(fopen("logdump.dot", "w")) {
	fprintf(_f,
		"digraph logdeps {\n"
		"    node [ width=0.1 height=0.1 fixedsize=true label=<> ]\n"
		"    edge [ arrowhead=normal arrowtail=none ]\n"
		"    node [ shape=oval ]\n"
		" 	node [ style=filled fillcolor=gray ]\n"
	   );
    }
    ~log_dump_file() { fclose(_f); }
    operator FILE*() { return _f; }
} __log_dump_file;

void match_all_and_dump(logrec_t* rp) {
    lsn_t lsn = rp->get_lsn_ck();
    tid_t tid = rp->tid();
    lpid_t pid = rp->construct_pid();
    if(!pid.valid() || tid.invalid())
	return;
    
    fprintf(__log_dump_file,
	    "    lr%lx [ tooltip=<LSN: %d.%09d TID: %d.%d PID: %d.%d.%d> ]\n",
	   lsn._data, lsn.hi(), lsn.lo(), tid.get_hi(), tid.get_lo(),
	   pid.vol().vol, pid.store(), pid.page,
	   lsn._data, rp->prev()._data);

    lsn_t &tid_prev = tid_deps[tid];
    if(tid_prev.valid()) {
	fprintf(__log_dump_file,
		"    lr%lx -> lr%lx [ color=blue tooltip=<TID: %d.%d> weight=100 ]\n" ,
		lsn._data, tid_prev._data, tid.get_hi(), tid.get_lo());
    }

    lsn_t &page_prev = page_deps[pid];
    if(page_prev.valid()) {
	fprintf(__log_dump_file,
		"    lr%lx -> lr%lx [ color=darkgreen tooltip=<PID: %d.%d.%d> weight=0 ]\n",
		lsn._data, page_prev._data, pid.vol().vol, pid.store(), pid.page);
    }
    
    tid_prev = page_prev = lsn;
}

void match_non_transactional(logrec_t* rp) {
    switch(rp->_type) {
    case logrec_t::t_skip:
    case logrec_t::t_chkpt_begin:
    case logrec_t::t_chkpt_bf_tab:
    case logrec_t::t_chkpt_xct_tab:
    case logrec_t::t_chkpt_dev_tab:
    case logrec_t::t_chkpt_end:
    case logrec_t::t_mount_vol:
    case logrec_t::t_dismount_vol:
	fprintf(stderr, "%s\n", pretty_print(rp));
	break;
    default:
	break;
    }
}
#include <map>

struct page_history {
    lpid_t pid;
    bool found_format;
    int last_slot;
    page_history(lpid_t const &p=lpid_t(), bool f=false, int l=-1) : pid(p), found_format(f), last_slot(l) { }
};
typedef std::map<long, page_history> phist_map;

phist_map page_histories;

typedef std::map<long, long> size_dist_map;
size_dist_map size_dist;

void print_size_dist() {
    // skip entry 0...
    size_dist_map::iterator it=size_dist.begin();
    fprintf(stderr, "Log record size distribution (%ld total):\n", it->second);
    while(++it != size_dist.end()) 
	fprintf(stderr, "	%ld %ld\n", it->first, it->second);

    size_dist.clear();
}

void match_all_measure_size_dist(logrec_t* rp) {
    size_dist[0]++; // total...
    size_dist[rp->length()]++;
}
			     
void match_slotted_pages(logrec_t* rp, char const* name=0) {
    if(rp->tag() != page_p::t_file_p)
	return;

    if(!name) name = "";
    lpid_t pid = rp->construct_pid();
    phist_map::iterator it = page_histories.find(pid.page);
    switch(rp->type()) {
    case logrec_t::t_page_format: {
	if(it != page_histories.end()) {
	    fprintf(stderr,
		    "%s Reformatting an existing page: %d.%d.%d (was %d.%d.%d with %d slots)\n",
		    name, pid.vol().vol, pid.store(), pid.page,
		    it->second.pid.vol().vol, it->second.pid.store(), it->second.pid.page, it->second.last_slot);
	}
	page_histories[pid.page] = page_history(pid, true, 0);
    } break;
    case logrec_t::t_page_reclaim: {
	short idx = *(short*) rp->data();
	if(it == page_histories.end()) {
	    // page existed before start of log
	    page_histories[pid.page] = page_history(pid, false, idx);
	}
	else {
	    // we've seen this page before
	    if(idx != it->second.last_slot + 1) {
		fprintf(stderr,
			"%s Missing log record(s) for page: %d.%d.%d (skips from %d to %d)\n",
			name, pid.vol().vol, pid.store(), pid.page, it->second.last_slot, idx);
	    }
	    it->second.last_slot = idx;
	}
    } break;
    default:
	break;
    }
}
