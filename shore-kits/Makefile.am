# -*- makefile -*-

# Shore related 
SHORE_SRC = $(SHORE_HOME)/src
SHORE_CXXFLAGS = -DSOLARIS2 -DSTHREAD_CORE_PTHREAD -DARCH_LP64 -DNO_FASTNEW -DW_LARGEFILE -D_POSIX_PTHREAD_SEMANTICS -D__STDC_FORMAT_MACROS -D__STDC_CONSTANT_MACROS -w

SHORE_INCLUDES = -I$(SHORE_SRC)/fc -I$(SHORE_SRC)/sm -I$(SHORE_SRC)/common -I$(SHORE_SRC)/sthread -I$(SHORE_SRC)/../config $(SHORE_CXXFLAGS)

if USE_SHORE_6
SHORE_INCLUDES += -I$(SHORE_SRC)/atomic_ops
else
SHORE_INCLUDES += -I$(SHORE_HOME)/../ppmcs
endif

SHORE_LDADD = \
	$(SHORE_SRC)/sm/libsm.a \
	$(SHORE_SRC)/common/libcommon.a \
	$(SHORE_SRC)/sthread/libsthread.a \
	$(SHORE_SRC)/fc/libfc.a

### -lmtmalloc <-- which malloc library will be included is responsibility of configure

if USE_SHORE_6
SHORE_LDADD += $(SHORE_SRC)/atomic_ops/libatomic_ops.a
else
SHORE_LDADD += $(SHORE_HOME)/../ppmcs/libppmcs.a
endif


if SPARC_MACHINE
# Readline includes
READLINE_INCLUDE = $(READLINE_HOME)/include
READLINE_LIBS = $(READLINE_HOME)/lib/sparcv9
# READLINE_INCLUDE = /opt/csw/include
# READLINE_LIBS = /opt/csw/lib/sparcv9

PROCSTAT_INCLUDES = 
PROCSTAT_LIBS = 
else
READLINE_INCLUDE = $(READLINE_HOME)/include
READLINE_LIBS = $(READLINE_HOME)/lib

PROCSTAT_INCLUDES = -I/usr/include/glib-2.0 -I/usr/lib/glib-2.0/include -I/usr/lib64/glib-2.0/include -I/usr/include/libgtop-2.0
PROCSTAT_LIBS = -lgtop-2.0
endif

if USE_VTUNE
VTUNE_INCLUDES = -I/opt/intel/vtune_amplifier_xe_2011/include
VTUNE_LIBS = -L/opt/intel/vtune_amplifier_xe_2011/lib64 -littnotify
else
VTUNE_INCLUDES =
VTUNE_LIBS =
endif

# GLOBAL includes/ld
INCLUDES = -I$(top_srcdir)/include $(SHORE_INCLUDES) -I/usr/local/include \
	-I$(READLINE_INCLUDE) $(PROCSTAT_INCLUDES) $(VTUNE_INCLUDES)
LDADD = $(lib_LIBRARIES) $(SHORE_LDADD) -L$(READLINE_LIBS) -lreadline $(PROCSTAT_LIBS) $(VTUNE_LIBS)

# List of libraries

if USE_DORA
DORA_LIBS = lib/libdoraworkload.a lib/libdora.a
else
DORA_LIBS = 
endif

if USE_QPIPE
QPIPE_LIBS = lib/libqpipe.a lib/libqpipeutil.a
else
QPIPE_LIBS =
endif

lib_LIBRARIES = $(DORA_LIBS) \
      lib/libworkload.a $(QPIPE_LIBS) lib/libsm.a lib/libutil.a

FE = \
   shore_kits

bin_PROGRAMS = $(FE)

debug-shore.so: debug-shore.cpp
	$(CXXCOMPILE) -xcode=pic13 -G -o $@ $<


################################################################################
#
# src/util/
#
################################################################################

SU=$(top_srcdir)/src/util

UTIL = \
	$(SU)/c_str.cpp \
	$(SU)/thread.cpp \
	$(SU)/time_util.cpp \
	$(SU)/trace.cpp \
	$(SU)/chomp.cpp \
        $(SU)/progress.cpp \
	$(SU)/pool_alloc.cpp \
        $(SU)/store_string.cpp \
        $(SU)/history.cpp \
        $(SU)/countdown.cpp \
        $(SU)/confparser.cpp \
        $(SU)/envvar.cpp \
        $(SU)/stl_pool.cpp \
        $(SU)/random_input.cpp \
        $(SU)/shell.cpp \
        $(SU)/tcp.cpp \
        $(SU)/w_strlcpy.cpp \
	$(SU)/procstat.cpp \
	$(SU)/skewer.cpp

if SPARC_MACHINE
UTIL +=	$(SU)/sunos_procstat.cpp \
        $(SU)/prcinfo.cpp
else
UTIL +=	$(SU)/linux_procstat.cpp \
        $(SU)/topinfo.cpp
endif

UTIL_CMD = \
        $(SU)/command/tracer.cpp \
        $(SU)/command/printer.cpp


lib_libutil_a_CXXFLAGS = $(AM_CXXFLAGS) 

lib_libutil_a_SOURCES = \
	$(UTIL) \
        $(UTIL_CMD)



################################################################################
#
# src/sm/
#
################################################################################

SM=$(top_srcdir)/src/sm
SMS=$(top_srcdir)/src/sm/shore

SM_SHORE = \
   $(SMS)/shore_file_desc.cpp \
   $(SMS)/shore_field.cpp \
   $(SMS)/shore_table.cpp \
   $(SMS)/shore_row.cpp \
   $(SMS)/shore_index.cpp \
   $(SMS)/shore_asc_sort_buf.cpp \
   $(SMS)/shore_desc_sort_buf.cpp \
   $(SMS)/shore_reqs.cpp \
   $(SMS)/shore_flusher.cpp \
   $(SMS)/shore_env.cpp \
   $(SMS)/shore_helper_loader.cpp \
   $(SMS)/shore_client.cpp \
   $(SMS)/shore_worker.cpp \
   $(SMS)/shore_trx_worker.cpp \
   $(SMS)/shore_iter.cpp \
   $(SMS)/shore_shell.cpp

lib_libsm_a_CXXFLAGS = $(AM_CXXFLAGS) $(SHORE_INCLUDES)

lib_libsm_a_SOURCES = \
	$(SM_SHORE)





################################################################################
#
# src/workload/
#
################################################################################



SW=$(top_srcdir)/src/workload

WL_TPCC_SHORE = \
   $(SW)/tpcc/tpcc_random.cpp \
   $(SW)/tpcc/tpcc_input.cpp \
   $(SW)/tpcc/shore_tpcc_schema.cpp \
   $(SW)/tpcc/shore_tpcc_schema_man.cpp \
   $(SW)/tpcc/shore_tpcc_env.cpp \
   $(SW)/tpcc/shore_tpcc_xct.cpp \
   $(SW)/tpcc/shore_tpcc_client.cpp

WL_TM1_SHORE = \
   $(SW)/tm1/tm1_input.cpp \
   $(SW)/tm1/shore_tm1_schema.cpp \
   $(SW)/tm1/shore_tm1_schema_man.cpp \
   $(SW)/tm1/shore_tm1_env.cpp \
   $(SW)/tm1/shore_tm1_xct.cpp \
   $(SW)/tm1/shore_tm1_client.cpp

WL_TPCB_SHORE = \
   $(SW)/tpcb/tpcb_input.cpp \
   $(SW)/tpcb/shore_tpcb_schema.cpp \
   $(SW)/tpcb/shore_tpcb_schema_man.cpp \
   $(SW)/tpcb/shore_tpcb_env.cpp \
   $(SW)/tpcb/shore_tpcb_xct.cpp \
   $(SW)/tpcb/shore_tpcb_client.cpp

WL_TPCH_SHORE = \
   $(SW)/tpch/tpch_random.cpp \
   $(SW)/tpch/tpch_input.cpp \
   $(SW)/tpch/tpch_util.cpp \
   $(SW)/tpch/shore_tpch_schema.cpp \
   $(SW)/tpch/shore_tpch_schema_man.cpp \
   $(SW)/tpch/shore_tpch_env.cpp \
   $(SW)/tpch/shore_tpch_xct.cpp \
   $(SW)/tpch/shore_tpch_client.cpp

WL_TPCH_DBGEN_SHORE = \
   $(SW)/tpch/dbgen/build.cpp \
   $(SW)/tpch/dbgen/driver.cpp \
   $(SW)/tpch/dbgen/bm_utils.cpp \
   $(SW)/tpch/dbgen/rnd.cpp \
   $(SW)/tpch/dbgen/print.cpp \
   $(SW)/tpch/dbgen/load_stub.cpp \
   $(SW)/tpch/dbgen/bcd2.cpp \
   $(SW)/tpch/dbgen/speed_seed.cpp \
   $(SW)/tpch/dbgen/text.cpp \
   $(SW)/tpch/dbgen/permute.cpp \
   $(SW)/tpch/dbgen/rng64.cpp

WL_TPCH_QPIPE = \
   $(SW)/tpch/qpipe_tpch_xct.cpp \
   $(SW)/tpch/qpipe/qpipe_tpch_qlineitem.cpp \
   $(SW)/tpch/qpipe/qpipe_tpch_qcustomer.cpp \
   $(SW)/tpch/qpipe/qpipe_tpch_qnation.cpp \
   $(SW)/tpch/qpipe/qpipe_tpch_qregion.cpp \
   $(SW)/tpch/qpipe/qpipe_tpch_qpart.cpp \
   $(SW)/tpch/qpipe/qpipe_tpch_qpartsupp.cpp \
   $(SW)/tpch/qpipe/qpipe_tpch_qsupplier.cpp \
   $(SW)/tpch/qpipe/qpipe_tpch_qorders.cpp \
   $(SW)/tpch/qpipe/qpipe_q1.cpp \
   $(SW)/tpch/qpipe/qpipe_q2.cpp \
   $(SW)/tpch/qpipe/qpipe_q3.cpp \
   $(SW)/tpch/qpipe/qpipe_q4.cpp \
   $(SW)/tpch/qpipe/qpipe_q5.cpp \
   $(SW)/tpch/qpipe/qpipe_q6.cpp \
   $(SW)/tpch/qpipe/qpipe_q7.cpp \
   $(SW)/tpch/qpipe/qpipe_q8.cpp \
   $(SW)/tpch/qpipe/qpipe_q9.cpp \
   $(SW)/tpch/qpipe/qpipe_q10.cpp \
   $(SW)/tpch/qpipe/qpipe_q11.cpp \
   $(SW)/tpch/qpipe/qpipe_q12.cpp \
   $(SW)/tpch/qpipe/qpipe_q13.cpp \
   $(SW)/tpch/qpipe/qpipe_q14.cpp \
   $(SW)/tpch/qpipe/qpipe_q15.cpp \
   $(SW)/tpch/qpipe/qpipe_q16.cpp \
   $(SW)/tpch/qpipe/qpipe_q17.cpp \
   $(SW)/tpch/qpipe/qpipe_q18.cpp \
   $(SW)/tpch/qpipe/qpipe_q19.cpp \
   $(SW)/tpch/qpipe/qpipe_q20.cpp \
   $(SW)/tpch/qpipe/qpipe_q21.cpp \
   $(SW)/tpch/qpipe/qpipe_q22.cpp

WL_SSB_SHORE = \
   $(SW)/ssb/ssb_random.cpp \
   $(SW)/ssb/ssb_input.cpp \
   $(SW)/ssb/ssb_util.cpp \
   $(SW)/ssb/shore_ssb_schema.cpp \
   $(SW)/ssb/shore_ssb_schema_man.cpp \
   $(SW)/ssb/shore_ssb_env.cpp \
   $(SW)/ssb/shore_ssb_xct.cpp \
   $(SW)/ssb/shore_ssb_client.cpp

WL_SSB_DBGEN_SHORE = \
   $(SW)/ssb/dbgen/ssb_build.cpp \
   $(SW)/ssb/dbgen/ssb_driver.cpp \
   $(SW)/ssb/dbgen/ssb_bm_utils.cpp \
   $(SW)/ssb/dbgen/ssb_rnd.cpp \
   $(SW)/ssb/dbgen/ssb_print.cpp \
   $(SW)/ssb/dbgen/ssb_load_stub.cpp \
   $(SW)/ssb/dbgen/ssb_bcd2.cpp \
   $(SW)/ssb/dbgen/ssb_speed_seed.cpp \
   $(SW)/ssb/dbgen/ssb_text.cpp \
   $(SW)/ssb/dbgen/ssb_permute.cpp 

WL_SSB_QPIPE = \
   $(SW)/ssb/qpipe_ssb_xct.cpp \
   $(SW)/ssb/qpipe/qpipe_q1_1.cpp \
   $(SW)/ssb/qpipe/qpipe_q1_2.cpp \
   $(SW)/ssb/qpipe/qpipe_q1_3.cpp \
   $(SW)/ssb/qpipe/qpipe_q2_1.cpp \
   $(SW)/ssb/qpipe/qpipe_q2_2.cpp \
   $(SW)/ssb/qpipe/qpipe_q2_3.cpp \
   $(SW)/ssb/qpipe/qpipe_q3_1.cpp \
   $(SW)/ssb/qpipe/qpipe_q3_2.cpp \
   $(SW)/ssb/qpipe/qpipe_q3_3.cpp \
   $(SW)/ssb/qpipe/qpipe_q3_4.cpp \
   $(SW)/ssb/qpipe/qpipe_q4_1.cpp \
   $(SW)/ssb/qpipe/qpipe_q4_2.cpp \
   $(SW)/ssb/qpipe/qpipe_q4_3.cpp \
   $(SW)/ssb/qpipe/qpipe_qdate.cpp \
   $(SW)/ssb/qpipe/qpipe_qpart.cpp \
   $(SW)/ssb/qpipe/qpipe_qsupplier.cpp \
   $(SW)/ssb/qpipe/qpipe_qcustomer.cpp \
   $(SW)/ssb/qpipe/qpipe_qlineorder.cpp \
   $(SW)/ssb/qpipe/qpipe_qtest.cpp 

WL_TPCE_SHORE = \
   $(SW)/tpce/tpce_input.cpp \
   $(SW)/tpce/shore_tpce_schema.cpp \
   $(SW)/tpce/shore_tpce_schema_man.cpp \
   $(SW)/tpce/shore_tpce_env.cpp \
   $(SW)/tpce/shore_tpce_client.cpp

WL_TPCE_SHORE_XCTS = \
   $(SW)/tpce/shore_tpce_xct_populate.cpp \
   $(SW)/tpce/shore_tpce_xct_trade_order.cpp \
   $(SW)/tpce/shore_tpce_xct_trade_result.cpp \
   $(SW)/tpce/shore_tpce_xct_trade_lookup.cpp \
   $(SW)/tpce/shore_tpce_xct_customer_position.cpp \
   $(SW)/tpce/shore_tpce_xct_market_feed.cpp \
   $(SW)/tpce/shore_tpce_xct_trade_cleanup.cpp \
   $(SW)/tpce/shore_tpce_xct_market_watch.cpp \
   $(SW)/tpce/shore_tpce_xct_data_maintenance.cpp \
   $(SW)/tpce/shore_tpce_xct_security_detail.cpp \
   $(SW)/tpce/shore_tpce_xct_trade_status.cpp \
   $(SW)/tpce/shore_tpce_xct_broker_volume.cpp \
   $(SW)/tpce/shore_tpce_xct_trade_update.cpp

WL_TPCE_SHORE_EGEN = \
   $(SW)/tpce/egen/DateTime.cpp \
   $(SW)/tpce/egen/error.cpp \
   $(SW)/tpce/egen/Random.cpp \
   $(SW)/tpce/egen/Money.cpp \
   $(SW)/tpce/egen/EGenVersion.cpp \
   $(SW)/tpce/egen/locking.cpp \
   $(SW)/tpce/egen/threading.cpp \
   $(SW)/tpce/egen/BaseLogger.cpp \
   $(SW)/tpce/egen/EGenLogFormatterTab.cpp \
   $(SW)/tpce/egen/EGenLoader.cpp \
   $(SW)/tpce/egen/MEE.cpp \
   $(SW)/tpce/egen/MEEPriceBoard.cpp \
   $(SW)/tpce/egen/MEESecurity.cpp \
   $(SW)/tpce/egen/MEETickerTape.cpp \
   $(SW)/tpce/egen/MEETradingFloor.cpp \
   $(SW)/tpce/egen/WheelTime.cpp \
   $(SW)/tpce/egen/AddressTable.cpp \
   $(SW)/tpce/egen/CustomerSelection.cpp \
   $(SW)/tpce/egen/CustomerTable.cpp \
   $(SW)/tpce/egen/InputFlatFilesStructure.cpp \
   $(SW)/tpce/egen/Person.cpp \
   $(SW)/tpce/egen/ReadRowFunctions.cpp \
   $(SW)/tpce/egen/TradeGen.cpp \
   $(SW)/tpce/egen/FlatFileLoader.cpp \
   $(SW)/tpce/egen/CE.cpp \
   $(SW)/tpce/egen/CETxnInputGenerator.cpp \
   $(SW)/tpce/egen/CETxnMixGenerator.cpp \
   $(SW)/tpce/egen/DM.cpp \
   $(SW)/tpce/egen/EGenGenerateAndLoad.cpp \
   $(SW)/tpce/egen/EGenValidate.cpp \
   $(SW)/tpce/egen/strutil.cpp \
   $(SW)/tpce/egen/progressmeter.cpp \
   $(SW)/tpce/egen/progressmeterinterface.cpp \
   $(SW)/tpce/egen/bucketsimulator.cpp


lib_libworkload_a_SOURCES = \
   $(WL_SG_TPCC_SHORE) \
   $(WL_TPCC_SHORE) \
   $(WL_TM1_SHORE) \
   $(WL_TPCB_SHORE) \
   $(WL_TPCH_DBGEN_SHORE) \
   $(WL_TPCH_SHORE) \
   $(WL_TPCE_SHORE_EGEN) \
   $(WL_TPCE_SHORE) \
   $(WL_TPCE_SHORE_XCTS)


if USE_QPIPE
lib_libworkload_a_SOURCES += $(WL_TPCH_QPIPE)
lib_libworkload_a_SOURCES += $(WL_SSB_QPIPE) $(WL_SSB_DBGEN_SHORE) $(WL_SSB_SHORE)
endif


lib_libworkload_a_INCLUDES = $(INCLUDES) -I$(top_srcdir)/include/workload $(SHORE_INCLUDES)




##### USE_QPIPE ######
if USE_QPIPE

################################################################################
#
# src/util/ *** Needed by QPipe ***
#
################################################################################

QPIPE_UTIL = \
	$(SU)/busy_delay.cpp \
	$(SU)/rio.cpp \
	$(SU)/resource_pool.cpp \
	$(SU)/static_list_node.cpp \
	$(SU)/static_list.cpp \
	$(SU)/fileops.cpp \
	$(SU)/alignedops.cpp \
	$(SU)/tmpfile.cpp \
	$(SU)/static_hash_map.cpp \
	$(SU)/fnv.cpp


lib_libqpipeutil_a_CXXFLAGS = $(AM_CXXFLAGS) 

lib_libqpipeutil_a_SOURCES = \
	$(QPIPE_UTIL)




################################################################################
#
# src/qpipe
#
################################################################################

SQP=$(top_srcdir)/src/qpipe

QPIPE_SCHEDULER = \
   $(SQP)/scheduler/cpu_set.cpp \
   $(SQP)/scheduler/cpu.cpp

QPIPE_CORE = \
   $(SQP)/core/tuple_fifo_directory.cpp \
   $(SQP)/core/stage_container.cpp \
   $(SQP)/core/dispatcher.cpp \
   $(SQP)/core/packet.cpp \
   $(SQP)/core/tuple.cpp \
   $(SQP)/core/tuple_fifo.cpp

QPIPE_STAGES = \
   $(SQP)/stages/merge.cpp \
   $(SQP)/stages/bnl_in.cpp \
   $(SQP)/stages/hash_join.cpp \
   $(SQP)/stages/partial_aggregate.cpp \
   $(SQP)/stages/tscan.cpp \
   $(SQP)/stages/fdump.cpp \
   $(SQP)/stages/sieve.cpp \
   $(SQP)/stages/echo.cpp \
   $(SQP)/stages/pipe_hash_join.cpp \
   $(SQP)/stages/aggregate.cpp \
   $(SQP)/stages/sorted_in.cpp \
   $(SQP)/stages/func_call.cpp \
   $(SQP)/stages/sort.cpp \
   $(SQP)/stages/hash_aggregate.cpp \
   $(SQP)/stages/delay_writer.cpp \
   $(SQP)/stages/bnl_join.cpp \
   $(SQP)/stages/sort_merge_join.cpp \
   $(SQP)/stages/fscan.cpp \
   $(SQP)/stages/register_stage_containers.cpp

QPIPE_COMMON = \
   $(SQP)/common/process_query.cpp \
   $(SQP)/common/predicates.cpp

lib_libqpipe_a_SOURCES = \
   $(QPIPE_SCHEDULER) \
   $(QPIPE_CORE) \
   $(QPIPE_STAGES) \
   $(QPIPE_COMMON)

lib_libqpipe_a_INCLUDES = $(INCLUDES) -I$(top_srcdir)/include/qpipe $(SHORE_INCLUDES)

endif
##### EOF USE_QPIPE ######



##### USE_DORA ######
if USE_DORA

################################################################################
#
# src/dora/
#
################################################################################

SD=$(top_srcdir)/src/dora
DO_BASE = \
   $(SD)/dkey_ranges_map.cpp \
   $(SD)/base_action.cpp \
   $(SD)/rvp.cpp \
   $(SD)/logical_lock.cpp \
   $(SD)/base_partition.cpp \
   $(SD)/partition.cpp \
   $(SD)/dflusher.cpp \
   $(SD)/worker.cpp \
   $(SD)/part_table.cpp \
   $(SD)/range_part_table.cpp \
   $(SD)/dora_env.cpp

lib_libdora_a_CXXFLAGS = $(AM_CXXFLAGS) $(SHORE_INCLUDES)

lib_libdora_a_SOURCES = \
   $(DO_BASE)



################################################################################
#
# src/dora/{workload}/
#
################################################################################

SDW=$(top_srcdir)/src/dora
DW_TPCC = \
   $(SDW)/tpcc/dora_payment.cpp \
   $(SDW)/tpcc/dora_new_order.cpp \
   $(SDW)/tpcc/dora_order_status.cpp \
   $(SDW)/tpcc/dora_stock_level.cpp \
   $(SDW)/tpcc/dora_delivery.cpp \
   $(SDW)/tpcc/dora_mbench.cpp \
   $(SDW)/tpcc/dora_tpcc.cpp \
   $(SDW)/tpcc/dora_tpcc_xct.cpp \
   $(SDW)/tpcc/dora_tpcc_client.cpp

DW_TM1 = \
   $(SDW)/tm1/dora_tm1_impl.cpp \
   $(SDW)/tm1/dora_tm1.cpp \
   $(SDW)/tm1/dora_tm1_xct.cpp \
   $(SDW)/tm1/dora_tm1_client.cpp

DW_TPCB = \
   $(SDW)/tpcb/dora_tpcb_impl.cpp \
   $(SDW)/tpcb/dora_tpcb.cpp \
   $(SDW)/tpcb/dora_tpcb_xct.cpp \
   $(SDW)/tpcb/dora_tpcb_client.cpp

lib_libdoraworkload_a_SOURCES = \
   $(DW_TPCC) \
   $(DW_TM1) \
   $(DW_TPCB)


lib_libdoraworkload_a_INCLUDES = $(INCLUDES) -I$(top_srcdir)/include/dora $(SHORE_INCLUDES)


endif
##### EOF USE_DORA ######





################################################################################
#
# shore_kits exec
#
################################################################################

ST=$(top_srcdir)/src/tests

shore_kits_SOURCES = $(ST)/shore_kits.cpp
shore_kits_CXXFLAGS = $(AM_CXXFLAGS) $(SHORE_INCLUDES)
shore_kits_LDADD = $(LDADD)

debug_%.so: debug_%.cpp
	$(CXXCOMPILE) -g -shared -fPIC -o $@ $<
