/*
   Tagsistant (tagfs) -- tagsistant.h
   Copyright (C) 2006-2009 Tx0 <tx0@strumentiresistenti.org>

   Tagsistant (tagfs) mount binary written using FUSE userspace library.
   Header file

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2, or (at your option)
   any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA. 
*/

#define TAGSISTANT_PLUGIN_PREFIX "libtagsistant_"

#define TAGSISTANT_QUERY_DELIMITER "@"
#define TAGSISTANT_QUERY_DELIMITER_CHAR '@'

#define TAGSISTANT_ANDSET_DELIMITER "+"
#define TAGSISTANT_ANDSET_DELIMITER_CHAR '+'

// deduplicator has been replaced by a call in fuse_operations/flush.c
#define TAGSISTANT_ENABLE_DEDUPLICATOR 0
#define TAGSISTANT_DEDUPLICATION_FREQUENCY 60 // seconds

#define TAGSISTANT_VERBOSE_LOGGING 0

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifndef VERSION
#define VERSION 0
#endif

#ifndef PLUGINS_DIR
#define PLUGINS_DIR "/usr/local/lib/tagsistant/"
#endif

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
#define FUSE_USE_VERSION 26
#endif

#define _REENTRANT
#define _POSIX_PTHREAD_SEMANTICS

#include <pthread.h>
#include "debug.h"
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
#	if HAVE_SYS_STATVFS_H
#		include <sys/statvfs.h>
# endif
#else
#	if HAVE_SYS_STATFS_H
#		include <sys/statfs.h>
#	endif
#endif
#include <stddef.h>
#include <netinet/in.h>
#include <utime.h>
#include <signal.h>
#include <dlfcn.h> /* for dlopen() and friends */
#include <glib.h>
#include <inttypes.h>
#include <stdint.h>

#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif

#undef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64

#include <fuse.h>
#include "compat/fuse_opt.h"

/**
 * each object is identified by a unique number of type tagsistant_id
 */
typedef uint32_t tagsistant_inode;

/**
 * some limits mainly taken from POSIX standard
 */
#define TAGSISTANT_MAX_TAG_LENGTH 255
#define TAGSISTANT_MAX_PATH_TOKENS 128

#define TAGSISTANT_INODE_DELIMITER "___"

/*
 * if tagsistant_symlink is called with two internal
 * paths, it should add the tags from the destination
 * path to the object pointed by source path (1), or
 * should create a new symlink (0)?
 *
 * NOTE: choosing 1 breaks support of Nautilus and
 * probably other file managers
 */
#define TAGSISTANT_RETAG_INTERNAL_SYMLINKS 0

#define dyn_strcat(original, newstring) original = _dyn_strcat(original, newstring)
extern gchar *_dyn_strcat(gchar *original, const gchar *newstring);

#include "sql.h"
#include "path_resolution.h"
#include "plugin.h"

/**
 * defines command line options for tagsistant mount tool
 */
struct tagsistant {
	int debug;			/**< enable debug */
	int	foreground;		/**< run in foreground */
	int	singlethread;	/**< single thread? */
	int	readonly;		/**< mount filesystem readonly */
	int	verbose;		/**< do verbose logging on syslog (stderr is always verbose) */
	int	quiet;			/**< don't log anything */
	int	show_config;	/**< show whole configuration */

	char *progname;		/**< tagsistant */
	char *mountpoint;	/**< no clue? */
	char *repository;	/**< it's where files and tags are archived, no? */
	char *archive;		/**< a directory holding all the files */
	char *tags;			/**< a SQLite database on file */
	char *dboptions;	/**< database options for DBI */

	/** the list of available plugins */
	tagsistant_plugin_t *plugins;

	/** debug file descriptor */
	FILE *debugfd;

	/** SQL backend provides intersect keyword */
	int sql_backend_have_intersect;

	/** SQL database driver */
	int sql_database_driver;
};

/** where global values are stored */
extern struct tagsistant tagsistant;

/**
 * g_free() a symbol only if it's not NULL
 *
 * @param symbol the symbol to free
 * @return
 */
#define freenull(symbol) {\
	if (symbol) {\
		g_free(symbol);\
		symbol = NULL;\
	}\
}

/**
 * Fuse operations logging macros.
 * Enabled or disabled by TAGSISTANT_VERBOSE_LOGGING.
 */
#if TAGSISTANT_VERBOSE_LOGGING

#	define TAGSISTANT_START(line, ...) { dbg(LOG_INFO, line, ##__VA_ARGS__);
#	define TAGSISTANT_STOP_OK(line, ...) dbg(LOG_INFO, line, ##__VA_ARGS__);
#	define TAGSISTANT_STOP_ERROR(line,...) dbg(LOG_ERR, line, ##__VA_ARGS__);

#else

#	define TAGSISTANT_START(line, ...) {}
#	define TAGSISTANT_STOP_OK(line, ...) {}
#	define TAGSISTANT_STOP_ERROR(line,...) {}

#endif // TAGSISTANT_VERBOSE_LOGGING

/*** *** *** *** *** *** *** continue cleaning this file from here  *** *** *** *** ***/

extern int 				tagsistant_process(tagsistant_querytree *qtree);

extern tagsistant_inode	tagsistant_get_inode(const gchar *path, gchar **purename);

extern void				tagsistant_utils_init();
extern void				init_syslog();
extern void				tagsistant_plugin_loader();
extern void				tagsistant_plugin_unloader();

extern void				tagsistant_plugin_apply_regex(const tagsistant_querytree *qtree, const char *buf, GMutex *m, GRegex *rx);

#define 				tagsistant_create_and_tag_object(qtree, errno)\
							tagsistant_inner_create_and_tag_object(qtree, errno, 0);

#define 				tagsistant_force_create_and_tag_object(qtree, errno)\
							tagsistant_inner_create_and_tag_object(qtree, errno, 1);

extern int				tagsistant_inner_create_and_tag_object(tagsistant_querytree *qtree, int *tagsistant_errno, int force_create);

extern void				tagsistant_invalidate_object_checksum(tagsistant_inode inode, dbi_conn conn);

#include "fuse_operations/operations.h"

// vim:ts=4:nocindent:nowrap
