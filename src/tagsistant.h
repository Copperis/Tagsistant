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
#define TAGSISTANT_ARCHIVE_PLACEHOLDER "<<<tagsistant>>>"

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
#include <glib/gstdio.h>

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

typedef uint32_t tagsistant_id;

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
 * define the querytree structure
 * that holds a tree of ptree_or_node_t
 * and ptree_and_node_t and a string
 * containing the file part of the path.
 */
typedef struct querytree {
	/** the query tree */
	ptree_or_node_t *tree;

	/** the path of the object, if provided */
	gchar *object_path;

	/** the ID of the object, if directly managed by tagsistant */
	tagsistant_id object_id;
} querytree_t;

/**
 * used in linked list of returned results
 */
typedef struct file_handle {
	/** filename pointed by this structure */
	char *name;

	/** next element in results */
	struct file_handle *next;
} file_handle_t;

/**
 * reasoning structure to trace reasoning process
 */
typedef struct reasoning {
	ptree_and_node_t *start_node;
	ptree_and_node_t *actual_node;
	int added_tags;
} reasoning_t;

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
 * A structure to manage object inside the database
 */
typedef struct tagsistant_object {
	/** the objet ID from the database */
	tagsistant_id ID;

	/** the basename from the database */
	gchar *basename;

	/** the path on disk, without the basename */
	gchar *path;

	/** the full path on disk */
	gchar *fullpath;

	/** linked list of tags applied to this object */
	GList *tags;
} tagsistant_object_t;

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

#define get_tag_name(path) g_path_get_basename(path)

extern gchar *_get_file_path(const gchar *filename, int file_id, int use_first_match);
#define get_file_path(filename, file_id) _get_file_path(filename, file_id, FALSE)
#define get_first_file_path(filename, file_id) _get_file_path(filename, file_id, TRUE)

extern void get_file_id_and_name(const gchar *original, int *id, char **name);

extern gboolean is_tagged(int file_id, char *tagname);
extern gboolean filename_is_tagged(const char *filename, const char *tagname);

extern querytree_t *build_querytree(const char *path, int do_reasoning);
extern file_handle_t *build_filetree(ptree_or_node_t *query, const char *path);
extern void traverse_querytree(querytree_t *qtree, void (*funcpointer)(ptree_and_node_t *, ...), ...);

extern void destroy_querytree(querytree_t *qtree);
extern void destroy_filetree(file_handle_t *fh);

/**
 * allows for applying a function to all the ptree_and_node_t nodes of
 * a querytree_t structure. the function applied must be declared as:
 *   void function(ptree_and_node_t *node, ...)
 * while can be of course provided with fixed length argument list
 *
 * @param qtree the querytree_t structure to traverse
 * @param funcpointer the pointer to the function (barely the function name)
 */
#define traverse_querytree(qtree, funcpointer, ...) {\
	if (NULL != qtree) {\
		ptree_or_node_t *ptx = qtree->tree;\
		while (NULL != ptx) {\
			ptree_and_node_t *andptx = ptx->and_set;\
			while (NULL != andptx) {\
				funcpointer(andptx, __VA_ARGS__);\
				andptx = andptx->next;\
			}\
			ptx = ptx->next;\
		}\
	}\
}

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
		dbg(LOG_ERR, "FREE ERROR: symbol %s is NULL @%s:%u!", __STRING(symbol), __FILE__, __LINE__);\
	}\
}

// vim:ts=4:nocindent:nowrap
