
########### install files ###############

install(FILES  asm_linkage.h stack.h trap.h DESTINATION include)



#original Makefile.am contents follow:

### Process this file with automake to produce Makefile.in
#
#include $(top_srcdir)/Makefile.generic
#
#include_HEADERS = asm_linkage.h stack.h trap.h
