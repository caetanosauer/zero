#!/bin/bash

#
# Filtering strategy:
#
# Each pass peels off some samples, categorizing them and leaving the
# rest behind for further passes. This ensures no double-counting is
# possible.
#

DUMP_SPLIT_DAT="$*"
[ -n "$DUMP_SPLIT_DAT" ] && echo "Dumping .dat file for each category" >&2
function any() {
    sep='('
    while [ "x$1" != "x" ]; do
	echo -n "$sep$1"
	sep='|'
	shift
    done
    echo ')'
}

function blame() {
    echo -n "{ "
    [ -n "$DUMP_SPLIT_DAT" ] && echo -n "print \$0 > \"test-$1.dat\"; "
    echo -n "totals[\"$1\"]+=\$1; next }"
    echo -n "BEGIN { totals[\"$1\"]=0 }"
}

gawk -f <(cat <<EOF

# certain classes of sleep time are unimportant
/ pthread_cond_wait $(any __1cFshoreNbase_client_tIrun_xcts __1cEbf_mK_clean_buf __1cOchkpt_thread_tDrun __1cUpage_writer_thread_tDrun __1cFshoreJsrmwqueue __1cIlog_coreMflush_daemon __1cFshoreTshell_await_clients __1cGcondex)/ $(blame ignore)
/ pthread_cond_timedwait $(any __1cTsunos_procmonitor_t __1cTbf_cleaner_thread_t)/ $(blame ignore)
/ ___nanosleep/ $(blame ignore)

# snag CATALOG stuff first because we want to include any
# latching/locking it causes
/ __1cFdir_m/ $(blame catalog)

# latch contention
/ $(any __1cKmcs_rwlock __1cImcs_lock atomic).* __1cHlatch_t/ $(blame latch-c)

# latching
/ __1cHlatch_t/ $(blame latch)

# physical lock contention
/ $(any __1cKmcs_rwlock __1cImcs_lock atomic).* $(any __1cGlock_m __1cLlock_core_m)/ $(blame lock-pc)

# logical lock contention
/ $(any __lwp_park __lwp_unpark).* $(any __1cGlock_m __1cLlock_core_m)/ $(blame lock-lc)

# locking
/ $(any __1cGlock_m __1cLlock_core_m)/ $(blame lock)

# bpool
/ $(any __1cEbf_m __1cJbf_core_m __1cUpage_writer_thread_t __1cTbf_cleaner_thread_t)/ $(blame bpool)

# logging
/ $(any log_ __1cFlog_m __1cIlog_core  __1cFshoreJflusher_t)/ $(blame log)

# xct mgt
/ __1cFxct_t/ $(blame xct_mgt)

# btree
/ $(any __1cHbtree_m __1cKbtree_impl)/ $(blame btree)

# heap
/ $(any __1cGfile_m __1cFpin_i)/ $(blame heap)

# SSM
/ $(any __1cTsunos_procmonitor_t __1cEss_m __1cGpage_pF __1cJw_error_t)/ $(blame ssm)

# Client
/ $(any __1cFshoreNbase_client_t)/ $(blame client)

# DORA
/ $(any __1cEdoraKlock_man_t __1cEdoraLpartition_t __1cEdoraOdora_flusher_t __1cEdoraOterminal_rvp_t __1cEdoraPdora_notifier_t)/ $(blame dora)

# kits
/ __1cFshore/ $(blame kits)

# leftovers
$(blame misc)

END { for (c in totals) { n=totals[c]; print c,n; total+=n }; print "total",total; print "net-total",total - totals["ignore"]; }
EOF
)

