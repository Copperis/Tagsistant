/*
   Tagsistant (tagfs) -- tagsistant.h
   Copyright (C) 2006-2007 Tx0 <tx0@strumentiresistenti.org>

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

/* for developing purposes only */
#define VERBOSE_DEBUG 0
/* #define DEBUG_STRDUP */

#define TAGSISTANT_PLUGIN_PREFIX "libtagsistant_"

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
#define FUSE_USE_VERSION 26
#endif

#define _REENTRANT
#define _POSIX_PTHREAD_SEMANTICS

#include <pthread.h>
#ifndef TAGMAN
#include <debug.h>
#else
#include "../../debug.h"
#endif
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
#include <glib/gstrfuncs.h>

#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif

#ifndef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64
#endif

#include <sqlite3.h>
#ifndef TAGMAN
#include <fuse.h>
#include <compat/fuse_opt.h>
#endif

#include "sql.h"

#define MAX_TAG_LENGTH 255

#define dyn_strcat(original, newstring) original = _dyn_strcat(original, newstring)
extern gchar *_dyn_strcat(gchar *original, const gchar *newstring);

/**
 * defines an AND token in a query path
 */
typedef struct ptree_and_node {
	/** the name of this token */
	char *tag;

	/** list of all related tags **/
	struct ptree_and_node *related;

	/** next AND token */
	struct ptree_and_node *next;
} ptree_and_node_t;

/**
 * define an OR section in a query path
 */
typedef struct ptree_or_node {
	/** the next OR section */	
	struct ptree_or_node *next;

	/** the list of AND tokens */
	struct ptree_and_node *and_set;
} ptree_or_node_t;

/**
 * used in linked list of returned results
 */
typedef struct file_handle {
	/** filename pointed by this structure */
	char *name;

	/** next element in results */
	struct file_handle *next;
} file_handle_t;

/* codes used in plugin chain processing */

#define TP_ERROR	0	/**< an error occurred while processing with this plugin */
#define TP_OK		1	/**< ok, but further tagging can be done by other plugins */
#define TP_STOP		2	/**< this plugin is authoritative for mimetype, stop chaining */
#define TP_NULL		3	/**< no tagging has been done, but that's not an error */

/**
 * holds a pointer to a processing funtion
 * exported by a plugin
 */
typedef struct tagsistant_plugin {
	/** MIME type managed by this plugins */
	char *mime_type;

	/** file name of this plugin */
	char *filename;

	/** handle to plugin returned by dlopen() */
	void *handle;

	/**
	 * hook to processing function
	 *
	 * \param filename the file to be processed
	 * \return 0 on failure (the plugin wasn't unable to process the file), 1 on
	 *   partial success (the plugin did processed the file, but later processing
	 *   by other plugins is allowed) or 2 on successful processing (no further
	 *   processing required).
	 */
	int (*processor)(const char *filename);

	/**
	 * hook to g_free allocated resources
	 */
	void (*free)();

	/** next plugin in linked list */
	struct tagsistant_plugin *next;
} tagsistant_plugin_t;

/**
 * defines command line options for tagsistant mount tool
 */
struct tagsistant {
	int      debug;			/**< enable debug */
	int		 foreground;	/**< run in foreground */
	int		 singlethread;	/**< single thread? */
	int		 readonly;		/**< mount filesystem readonly */
	int		 verbose;		/**< do verbose logging on syslog (stderr is always verbose) */
	int		 quiet;			/**< don't log anything */

	char    *progname;		/**< tagsistant */
	char    *mountpoint;	/**< no clue? */
	char    *repository;	/**< it's where files and tags are archived, no? */
	char    *archive;		/**< a directory holding all the files */
	char    *tags;			/**< a SQLite database on file */

	sqlite3 *dbh;			/**< database handle to operate on SQLite thingy, but no longer used? */
};

extern struct tagsistant tagsistant;

extern int debug;
extern int log_enabled;

extern char *get_tag_name(const char *path);
extern char *get_tag_path(const char *tag);
extern char *get_file_path(const char *tag);
extern char *get_tmp_file_path(const char *tag);

extern int is_tagged(char *filename, char *tagname);
extern int tag_file(const char *filename, char *tagname);
extern int untag_file(char *filename, char *tagname);

extern ptree_or_node_t *build_querytree(const char *path);
extern file_handle_t *build_filetree(ptree_or_node_t *query, const char *path);

extern void destroy_querytree(ptree_or_node_t *pt);
extern void destroy_filetree(file_handle_t *fh);

extern FILE *debugfd;

#ifdef DEBUG_STRDUP
#	ifndef DEBUG_TO_LOGFILE
#		define DEBUG_TO_LOGFILE
#	endif
#	ifdef strdup
#		undef strdup
#		define strdup(string) real_strdup(string, __FILE__, __LINE__)
#	endif
char *real_strdup(const char *orig, char *file, int line);
#endif

#define freenull(symbol) {\
	if (symbol != NULL) {\
		if (debugfd != NULL) {\
			fprintf(debugfd, "0x%.8x: g_free()\n", (unsigned int) symbol);\
		}\
		g_free(symbol);\
		symbol = NULL;\
	} else {\
		dbg(LOG_ERR, "FREE ERROR: symbol %s is NULL!", __STRING(symbol));\
	}\
}

// vim:ts=4:nocindent:nowrap
