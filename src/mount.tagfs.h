#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

/* used to import mempcpy */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#ifdef linux
/* For pread()/pwrite() */
#ifdef _XOPEN_SOURCE
#undef _XOPEN_SOURCE
#endif
#define _XOPEN_SOURCE 500
#endif

#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 22
#endif

#define _REENTRANT
#define _POSIX_PTHREAD_SEMANTICS

#include <pthread.h>
#include <debug.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#if FUSE_USE_VERSION == 25
#	include <sys/statvfs.h>
#else
#	include <sys/statfs.h>
#endif
#include <stddef.h>
#include <netinet/in.h>
#include <utime.h>
#include <signal.h>

#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif

#ifndef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64
#endif

#include <fuse.h>
#include <compat/fuse_opt.h>

typedef struct path_tree {
	char *name;
	struct path_tree *OR;
	struct path_tree *AND;
} path_tree_t;

typedef struct file_handle {
	char *name;
	struct file_handle *next;
} file_handle_t;

/* defines command line options for tagfs mount tool */
struct tagfs {
	int   debug;
	char *progname;
	char *mountpoint;
	char *repository;
	char *archive;
	char *tagsdir;
#ifdef OBSOLETE_CODE
	char *tmparchive;
#endif /* OBSOLETE_CODE */
};

extern struct tagfs tagfs;

extern int debug;
extern int log_enabled;


extern char *get_tag_name(const char *path);
extern char *get_tag_path(const char *tag);
extern char *get_file_path(const char *tag);
extern char *get_tmp_file_path(const char *tag);

extern path_tree_t *build_pathtree(const char *path);
extern file_handle_t *build_filetree(path_tree_t *pt);

extern void destroy_path_tree(path_tree_t *pt);
extern void destroy_file_tree(file_handle_t *fh);

// vim:ts=4:nocindent:nowrap
