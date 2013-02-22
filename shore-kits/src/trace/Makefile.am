# -*- makefile -*-

INCLUDES = -I$(top_srcdir)/include -I$(top_srcdir)/include/trace

# encapsulates all tracing functionality used by QPipe
lib_LIBRARIES = libtrace.a

libtrace_a_SOURCES = \
   trace.cpp \
   trace_force.cpp \
   trace_print_pthread.cpp \
   trace_stream.cpp
