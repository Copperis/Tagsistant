bin_PROGRAMS = tagsistant

tagsistant_SOURCES = tagsistant.c path_resolution.c\
	compat/fuse_opt.c debug.h debug_free_calls.h

#### tagsistant_CFLAGS = -D'PLUGINS_DIR="@libexecdir@/tagsistant"'
tagsistant_CFLAGS = -D'PLUGINS_DIR="@libdir@/"'
tagsistant_LDFLAGS = -lfuse -lsqlite3 -ldl

lib_LTLIBRARIES = libtagsistant_generic-0.1.la
libtagsistant_generic_0_1_la_SOURCES = tagsistant.h tp_generic.c
