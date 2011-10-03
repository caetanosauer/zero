
########### install files ###############

install(FILES  shore.def shore-config.h DESTINATION include)



#original Makefile.am contents follow:

### needed for "make dist" also
#include $(top_srcdir)/Makefile.generic
#EXTRA_DIST = shore.def
#include_HEADERS = shore.def shore-config.h
#MAINTAINERCLEANFILES = shore-config.h
#
#INDEPENDENT_MAKEFLAGS   = $(AM_CXXFLAGS)
#
#all-local:
#	echo "These are the CXX flags used with the last build: " > $(top_srcdir)/makeflags
#	echo $(INDEPENDENT_MAKEFLAGS) >> $(top_srcdir)/makeflags
#
#install-exec-local:
#	echo "These are the CXX flags used with the build for this installation: " > $(DESTDIR)$(includedir)/makeflags
#	echo $(INDEPENDENT_MAKEFLAGS) >> $(DESTDIR)$(includedir)/makeflags
#	cp shore.def $(DESTDIR)$(includedir)/
#	cp shore-config.h $(DESTDIR)$(includedir)/
#
#uninstall-local:
#	rm -f $(DESTDIR)$(includedir)/makeflags 
#
#distclean-local:
#	echo "Nothing to do for distclean in config/"
