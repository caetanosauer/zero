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

/** @file:   logical_lock.cpp
 *
 *  @brief:  Logical lock class used by DORA
 *
 *  @author: Ippokratis Pandis, Oct 2008
 */


#include "dora/logical_lock.h"


ENTER_NAMESPACE(dora);





/******************************************************************** 
 *
 * Lock compatibility Matrix
 *
 ********************************************************************/

int DoraLockModeMatrix[DL_CC_MODES][DL_CC_MODES] = { {1, 1, 1},
                                                     {1, 1, 0},
                                                     {1, 0, 0} };


#undef LOCKDEBUG
#define LOCKDEBUG

typedef PooledList<ActionLockReq>::Type     ActionLockReqList;
typedef ActionLockReqList::iterator         ActionLockReqListIt;
typedef ActionLockReqList::const_iterator   ActionLockReqListCit;


/******************************************************************** 
 *
 * @struct: ActionLockReq
 *
 ********************************************************************/ 

//// Helper functions


std::ostream& 
operator<<(std::ostream& os, const ActionLockReq& rhs) 
{
    os << "(" << rhs._tid << ") (" << rhs._dlm << ") (" << rhs._action << ")";
    return (os);
}




struct pretty_printer 
{
    ostringstream _out;
    string _tmp;
    operator ostream&() { return _out; }
    operator char const*() { _tmp = _out.str(); _out.str(""); return _tmp.c_str(); }
};



static void _print_logical_lock_maps(std::ostream &out, LogicalLock& ll) 
{
    int o = ll.owners().size();
    //int w = ll.waiters().size();
    out << "Owners " << o << endl;
    int i=0;
    for (i=0; i<o; ++i) {
        out << i << ". " << ll.owners()[i] << endl;
    }
    out << "Waiters " << o << endl;
    i=0;
    for (ActionLockReqListCit it=ll.waiters().begin(); it!=ll.waiters().end(); ++it) {

        out << ++i << ". " << (*it) << endl;
    }
}


char const* db_pretty_print(LogicalLock* ll, int /* i=0 */, char const* /* s=0 */) 
{
    static pretty_printer pp;
    _print_logical_lock_maps(pp, *ll);
    return pp;
}


static void _print_key(std::ostream &out, key_wrapper_t<int> const &key) 
{    
    for (uint i=0; i<key._key_v.size(); ++i) {
        out << key._key_v.at(i) << endl;
    }
}


char const* db_pretty_print(key_wrapper_t<int> const* key, int /* i=0 */, char const* /* s=0 */) 
{
    static pretty_printer pp;
    _print_key(pp, *key);
    return pp;
}




/******************************************************************** 
 *
 * @struct: LogicalLock
 *
 ********************************************************************/ 

LogicalLock::LogicalLock(ActionLockReq& anowner)
    : _dlm(anowner.dlm())
{
    // construct a logical lock with an owner already
    _owners.reserve(1);
    _owners.push_back(anowner);
}




/******************************************************************** 
 *
 * @fn:     release()
 *
 * @brief:  Releases the lock on behalf of the particular action,
 *          and updates the vector of promoted actions.
 *          The lock manager should that receives this list should:
 *          (1) associate the promoted action with the particular key in the trx-to-key map
 *          (2) decide which of those are ready to run and return them to the worker
 *
 * @note:   The action must be in the list of owners
 *
 ********************************************************************/ 

int LogicalLock::release(BaseActionPtr anowner, 
                         BaseActionPtrList& promotedList)
{
    assert (anowner);
    bool found = false;    
    tid_t atid = anowner->tid();
    int ipromoted = 0;

    // 1. Loop over all Owners
    for (ActionLockReqVecIt it=_owners.begin(); it!=_owners.end(); ++it) {
        tid_t* ownertid = (*it).tid();
        assert (ownertid);
        TRACE( TRACE_TRX_FLOW, "Checking (%d) - Owner (%d)\n", 
               atid.get_lo(), ownertid->get_lo());

        // 2. Check if trx in the list of Owners
        if (atid==*ownertid) {
            found = true;

            // 3. Remove trx from list of Owners
            _owners.erase(it);

            // 4. Update the LockMode
            if (_upd_dlm()) {

                // 5. If indeed LockMode has changed, 
                //    check if can upgrade some of the waiters.
                TRACE( TRACE_TRX_FLOW, 
                       "Release of (%d). Onwers (%d). Updated dlm to (%d)\n", 
                       atid.get_lo(), _owners.size(), _dlm);
                BaseActionPtr action = NULL;

                // 6. Promote all the waiters that can be upgraded to owners.
                //    Iterates the list of waiters in a FIFO-fashion
                while (_head_can_acquire()) {
                    // 7. If head of waiters can be promoted
                    ActionLockReq head = _waiters.front();
                    action = head.action();
                    
                    // 7. Add head of waiters to the promoted list 
                    //    (which will be returned)
                    TRACE( TRACE_TRX_FLOW, "(%d) promoting (%d)\n", 
                           atid.get_lo(), action->tid().get_lo());
                    promotedList.push_back(action);

                    // 8. Add head of waiters to the owners list
                    _owners.push_back(head);
                    ++ipromoted;

                    // 9. Remove head from the waiters list
                    _waiters.pop_front();

                    // 10. Update the LockMode of the LogicalLock
                    _upd_dlm();
                    TRACE( TRACE_TRX_FLOW,
                           "Release of (%d). Owners (%d). Promoted (%d). New dlm is (%d)\n",
                           atid.get_lo(), _owners.size(), ipromoted, _dlm);
                }
            }

            TRACE( TRACE_TRX_FLOW,
                   "Release of (%d). Owners (%d). Promoted (%d). Final dlm is (%d)\n",
                   atid.get_lo(), _owners.size(), ipromoted, _dlm);
            break;
        }
    }    

    // 2. Assert if the trx is not in the list of Owners for this LogicalLock
    if (!found) assert (0);
    return (ipromoted);
}



/******************************************************************** 
 *
 * @fn:     acquire()
 *
 * @brief:  Acquire the lock on behalf of the particular trx.
 *
 * @return: Returns (true) if the lock acquire succeeded.
 *          If it returns (false) the trx is enqueued to the waiters list.
 *
 ********************************************************************/ 

bool LogicalLock::acquire(ActionLockReq& alr)
{
    assert (alr.action());

    // 1. Check if already possesing this lock
    for (ActionLockReqVecIt it=_owners.begin(); it!=_owners.end(); ++it) {
        if (alr.isSame((*it))) {

            // if it is the same
            if (_dlm == alr.dlm()) {
                // no need to do anything else
                return (true);
            }

            // if it is the only owner
            if (_owners.size()==1) {
                // update lock mode to the more restrictive
                if (_dlm < alr.dlm()) _dlm = alr.dlm();
                // no need to do anything else
                return (true); 
            }
            else {
                // !!! TODO: HANDLE UPGRADE REQUESTS
                TRACE (TRACE_ALWAYS, 
                       "Cannot handle upgrade requests (%d) (%d)!!!\n", 
                       _dlm, alr.dlm());
                assert (0);
                return (false);
            }
        }
    }
    // If we reached this point, then the trx is not already owner of this lock
    // so it needs to acquire from scratch


    // 2. Shortcut - check if not compatible with current LockMode
    
    // Note: If current LockMode not compatible with the request don't need
    //       to do anything else but to put the request in the list of waiters.
    if (!DoraLockModeMatrix[_dlm][alr.dlm()]) {
        assert (_owners.size());
        _waiters.push_back(alr);
        return (false);
    }

    // Until now we have been allowing SH requests to bypass any waiting EX. 
    // That has been working fine in workloads, like TM1-Mix, where
    // the SH requests where much more than the EX ones. 
    // However, in mixed workloads, like TPCC-Mix, where the SH and EX requests
    // are 50%-50% this leads to starvation and deadlocks!
    // The deadlocks are caused because the FIFO execution principle BREAKS! 
    
    // 3. Check list of waiters
    for (ActionLockReqListIt it=_waiters.begin(); it!=_waiters.end(); ++it) {

        // Note: The search should be from the head of the list of the
        //       waiters to the tail, because all the compatible waiters
        //       have already been promoted to owners.
        eDoraLockMode wdlm = (*it).dlm();
        if (!DoraLockModeMatrix[wdlm][alr.dlm()]) {
            TRACE( TRACE_TRX_FLOW,
                   "(%d) conflicting waiter. Waiter (%d). Me (%d)\n",
                   alr.tid()->get_lo(), wdlm, alr.dlm());
            
            // put it at the tail of the waiters
            _waiters.push_back(alr);
            return (false);
        }
    }
    // If we reached this point we know that we are compatible with the
    // current LockMode, and there is no conflicting waiter. Therefore,
    // we can go ahead and enqueue ourselves to the Owners.
 
    // 4. Enqueue to the owners
    _owners.push_back(alr);

    // update lock mode
    if (alr.dlm() != DL_CC_NOLOCK) _dlm = alr.dlm();

    TRACE( TRACE_TRX_FLOW, 
           "(%d) got it. Owners (%d). LM (%d)\n",
           alr.tid()->get_lo(), _owners.size(), _dlm);

    return (true);
}



/******************************************************************** 
 *
 * @fn:     _head_can_acquire()
 *
 * @brief:  Returns (true) if the action at the head of the waiting list
 *          can acquire the lock
 *
 ********************************************************************/ 

bool LogicalLock::_head_can_acquire()
{
    if (_waiters.empty()) return (false); // no waiters
    return (DoraLockModeMatrix[_dlm][_waiters.front().dlm()]);
}



/******************************************************************** 
 *
 * @fn:     _upd_dlm()
 *
 * @brief:  Iterates the current owners and updates the LockMode
 *
 * @return: Returns (true) if there was a change in the LockMode
 *
 ********************************************************************/ 

bool LogicalLock::_upd_dlm()
{
    // 1. Check if there are any Owners
    if (_owners.empty()) {
        // 2. If there are not Owners
        if (_dlm!=DL_CC_NOLOCK) {
            // 3. If LockMode not NoLock, update LockMode, and return (true) 
            _dlm=DL_CC_NOLOCK;
            return (true);
        }
        // 4. If LockMode already NoLock, do nothing, and return (false)
        return (false);        
    }
    // If reached this point there are some owners

    eDoraLockMode new_dlm = DL_CC_NOLOCK;
    eDoraLockMode odlm = DL_CC_NOLOCK;
    bool changed = false;    

    // 5. Iterate over all Onwers
    for (ActionLockReqVecIt it=_owners.begin(); it!=_owners.end(); ++it) {
        odlm = (*it).dlm();

        // 6. Assert if two owners have incompatible modes
        if (!DoraLockModeMatrix[new_dlm][odlm]) {
            assert (0); 
        }

        // 7. Update the LockMode watermark
        if (odlm!=DL_CC_NOLOCK) {
            new_dlm = odlm;
        }
    }
    // At this point all Owners have been checked

    // 8. Update the LockMode, and set the change flag 
    changed = (_dlm != new_dlm);
    _dlm = new_dlm;

    // 9. Return if the LockMode has changed
    return (changed);
}



/******************************************************************** 
 *
 * @fn:     is_clean()
 *
 * @brief:  Returns (true) if the lock has no owners or waiters
 *
 ********************************************************************/ 

bool LogicalLock::is_clean() const
{
    bool isClean = (_owners.empty()) && (_waiters.empty()) && (_dlm == DL_CC_NOLOCK);
    return (isClean);
}


/******************************************************************** 
 *
 * @fn:     abort_and_reset()
 *
 * @brief:  Adds the tid_t of the owner and waiter to the list of the
 *          xcts to aborts. Removes any entries from the owners and waiters, 
 *          and sets lockmode.
 *
 ********************************************************************/ 

void LogicalLock::abort_and_reset(vector<xct_t*>& toabort)
{
    // Push tids for abortion

    // Iterate over all Onwers
    for (ActionLockReqVecIt it=_owners.begin(); it!=_owners.end(); ++it) {
        xct_t* victim = (*it).action()->xct();
        cout << (*it) << endl;
        toabort.push_back(victim);
    }
    
    // Update local state
    _owners.clear();
    _waiters.clear();
    _dlm = DL_CC_NOLOCK;
}



//// Helper functions


std::ostream& 
operator<<(std::ostream& os, LogicalLock& rhs) 
{
    os << "lock:   " << rhs.dlm() << endl; 
    os << "owners: " << rhs.owners().size() << endl; 
    for (uint i=0; i<rhs.owners().size(); ++i) {
        os << rhs.owners()[i] << endl;
    }

    os << "waiters: " << rhs.waiters().size() << endl;
    for (ActionLockReqListCit it=rhs.waiters().begin(); it!=rhs.waiters().end(); ++it) {
        os << (*it) << endl;
    }
    return (os);
}


EXIT_NAMESPACE(dora);

