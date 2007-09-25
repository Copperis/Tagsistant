bin_PROGRAMS = tagsistant

tagsistant_SOURCES = tagsistant.c path_resolution.c\
	compat/fuse_opt.c debug.h debug_free_calls.h

tagsistant_CFLAGS = -D'PLUGINS_DIR="@libexecdir@/tagsistant"'
tagsistant_LDFLAGS = -lfuse -lsqlite3 -ldl
