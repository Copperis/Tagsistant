SUBDIRS = plugins

bin_PROGRAMS = tagsistant

tagsistant_SOURCES = tagsistant.c\
	tagsistant.h\
	path_resolution.c\
	path_resolution.h\
	compat/fuse_opt.c\
	compat/fuse_opt.h\
	debug.h\
	debug_free_calls.h\
	sql.c\
	sql.h\
	utils.c\
	plugin.c\
	plugin.h\
	fuse_operations/operations.h\
	fuse_operations/access.c\
	fuse_operations/chmod.c\
	fuse_operations/chown.c\
	fuse_operations/getattr.c\
	fuse_operations/link.c\
	fuse_operations/mkdir.c\
	fuse_operations/mknod.c\
	fuse_operations/open.c\
	fuse_operations/read.c\
	fuse_operations/readdir.c\
	fuse_operations/readlink.c\
	fuse_operations/rename.c\
	fuse_operations/rmdir.c\
	fuse_operations/statfs.c\
	fuse_operations/statvfs.c\
	fuse_operations/symlink.c\
	fuse_operations/truncate.c\
	fuse_operations/unlink.c\
	fuse_operations/utime.c\
	fuse_operations/write.c	

tagsistant_CFLAGS = -D'PLUGINS_DIR="@libdir@/"' $(CFLAGS) `pkg-config --cflags glib-2.0`
tagsistant_LDFLAGS = -lfuse -ldl -rdynamic `pkg-config --libs glib-2.0`
