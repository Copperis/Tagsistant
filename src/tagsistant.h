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

#ifndef TAGMAN
#include <fuse.h>
#include <compat/fuse_opt.h>
#endif

/*
 * each object is identified by a unique number of type tagsistant_id
 */
typedef uint32_t tagsistant_id;

#include "sql.h"

/*
 * some limits mainly taken from POSIX standard
 */
#define TAGSISTANT_MAX_TAG_LENGTH 255
#define TAGSISTANT_MAX_PATH_TOKENS 128

#define TAGSISTANT_ID_DELIMITER "___"

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

/**
 * evaluates true if string "relation" matches at least
 * one of available relations
 */
#define IS_VALID_RELATION(relation) ((g_strcmp0(relation, "is_equivalent")) == 0 || (g_strcmp0(relation, "includes")))

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

/*
 * depeding on relative path, a query can be one in the following:
 */
typedef enum {
	QTYPE_MALFORMED,	// wrong path (not starting by /tags, /archive, /stats or /relations)
	QTYPE_ROOT,			// no path, that's a special case for root directory
	QTYPE_ARCHIVE,		// path pointing to objects on disk, begins with /archive/
	QTYPE_TAGS,			// path that's a query, begins with /tags/
	QTYPE_RELATIONS,	// path that's a relation between two or more tags, begins with /relations/
	QTYPE_STATS,		// path that's a special query for internal status, begins with /stats/
	QTYPE_TOTAL
} tagsistant_query_type_t;

/*
 * to ease coding, there are some macros to check
 * if a query if of a given type
 */
#define QTREE_IS_MALFORMED(qtree) (QTYPE_MALFORMED == qtree->type)
#define QTREE_IS_ROOT(qtree) (QTYPE_ROOT == qtree->type)
#define QTREE_IS_TAGS(qtree) (QTYPE_TAGS == qtree->type)
#define QTREE_IS_ARCHIVE(qtree) (QTYPE_ARCHIVE == qtree->type)
#define QTREE_IS_RELATIONS(qtree) (QTYPE_RELATIONS == qtree->type)
#define QTREE_IS_STATS(qtree) (QTYPE_STATS == qtree->type)

/*
 * if a query points to an object on disk this returns true;
 * that's:
 *
 *   archive/something
 *   tags/t1/t2/.../tN/=/something
 */
#define QTREE_POINTS_TO_OBJECT(qtree) (qtree->points_to_object == 1)

/*
 * some more info about a query:
 * is_taggable -> points_to_object but on first level (so not on tags/t1/t2/.../tN/=/something/more/...)
 * is_complete -> query is of type tags/ and has an =/
 * is_external -> the query points outside tagsistant mountpoint
 * is_internal -> the query points inside tagsistant mountpoint
 */
#define QTREE_IS_TAGGABLE(qtree) (qtree->is_taggable == 1)
#define QTREE_IS_COMPLETE(qtree) (qtree->complete)
#define QTREE_IS_EXTERNAL(qtree) (qtree->is_external)
#define QTREE_IS_INTERNAL(qtree) (!qtree->is_external)

/*
 * two queries are of the same type and are both complete
 * the second is true for tags/ if both are complete,
 * and always for other types of queries
 */
#define QTREES_ARE_SIMILAR(qtree1, qtree2) ((qtree1->type == qtree2->type) && (qtree1->complete == qtree2->complete))

/*
 * check if a path is external to tagsistant mountpoint
 * without requiring query resolution and querytree building
 */
#define TAGSISTANT_PATH_IS_EXTERNAL(path) (g_strstr_len(path, strlen(path), tagsistant.mountpoint) != path)

/**
 * define the querytree structure
 * that holds a tree of ptree_or_node_t
 * and ptree_and_node_t and a string
 * containing the file part of the path.
 */
typedef struct querytree {
	/** the complete path that generated the tree */
	/** i.e. <MPOINT>/t1/+/t2/=/object/path.txt */
	gchar *full_path;

	/** the query tree */
	ptree_or_node_t *tree;

	/** the path of the object, if provided */
	/** i.e. object/path.txt */
	gchar *object_path;

	/** the path of the object on disk */
	/** NNN___object/path.txt */
	gchar *archive_path;

	/** like the previous one, but with current archive path prefixed */
	/** ~/.tagsistant/archive/NNN___object/path.txt */
	gchar *full_archive_path;

	/** the query points to an object on disk? */
	int points_to_object;

	/** the object path pointed to is taggable (one element path) */
	int is_taggable;

	/** the object is external to tagsistant mountpoint */
	int is_external;

	/** the ID of the object, if directly managed by tagsistant */
	tagsistant_id object_id;

	/** last tag found while parsing a /tags query */
	gchar *last_tag;

	/** the query is valid */
	int valid;

	/** the query is complete */
	int complete;

	/** which kind of path is this? */
	/** can be QTYPE_MALFORMED, QTYPE_ROOT, QTYPE_TAGS, QTYPE_ARCHIVE, QTYPE_RELATIONS, QTYPE_STATS */
	int type;

	/** the first tag in a relation */
	gchar *first_tag;

	/** the second tag in a relation */
	gchar *second_tag;

	/** the relation */
	gchar *relation;

	/** the stats path (used for status query in /stat/ paths */
	gchar *stats_path;
} tagsistant_querytree_t;

// definition used to move from querytree_t to tagsistant_querytree_t
#define querytree_t tagsistant_querytree_t

/**
 * used in linked list of returned results
 *
 * Alessandro AkiRoss Re reported a conflict with the structure
 * file_handle in /usr/include/bits/fcntl.h on Fedora 15; making
 * the struct anonymous.
 */
typedef struct /* file_handle */ {
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
	int (*processor)(const tagsistant_querytree_t *qtree);

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
	int     debug;			/**< enable debug */
	int	foreground;		/**< run in foreground */
	int	singlethread;		/**< single thread? */
	int	readonly;		/**< mount filesystem readonly */
	int	verbose;		/**< do verbose logging on syslog (stderr is always verbose) */
	int	quiet;			/**< don't log anything */
	int	show_config;		/**< show whole configuration */

	char    *progname;		/**< tagsistant */
	char    *mountpoint;		/**< no clue? */
	char    *repository;		/**< it's where files and tags are archived, no? */
	char    *archive;		/**< a directory holding all the files */
	char    *tags;			/**< a SQLite database on file */
	char    *dboptions;		/**< database options for DBI */
};

extern struct tagsistant tagsistant;

extern int debug;
extern int log_enabled;

extern querytree_t *tagsistant_build_querytree(const char *path, int do_reasoning);
extern file_handle_t *tagsistant_build_filetree(ptree_or_node_t *query, const char *path);

extern void tagsistant_destroy_querytree(querytree_t *qtree);
extern void tagsistant_destroy_filetree(file_handle_t *fh);

extern int tagsistant_process(tagsistant_querytree_t *qtree);

extern tagsistant_id tagsistant_get_object_id(const gchar *path, gchar **purename);

extern void init_syslog();
extern void tagsistant_plugin_loader();
extern void tagsistant_plugin_unloader();

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
	dbg(LOG_INFO, "Traversing querytree...");\
	if (NULL != qtree) {\
		ptree_or_node_t *ptx = qtree->tree;\
		while (NULL != ptx) {\
			ptree_and_node_t *andptx = ptx->and_set;\
			while (NULL != andptx) {\
				funcpointer(andptx->tag, ##__VA_ARGS__);\
				dbg(LOG_INFO, "Applying %s(%s,...)", #funcpointer, andptx->tag);\
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
	} else if (VERBOSE_DEBUG) {\
		dbg(LOG_INFO, "free(%s) but symbol is NULL!", __STRING(symbol));\
	}\
}

#define tagsistant_qtree_set_object_path(qtree, path) {\
	qtree->object_path = g_strdup(path);\
	tagsistant_querytree_rebuild_paths(qtree);\
}

#define tagsistant_qtree_copy_object_path(from_qtree, to_qtree) tagsistant_qtree_set_object_path(to_qtree, from_qtree->object_path)

// change the object ID to a querytree_t structure
extern void tagsistant_qtree_renumber(querytree_t *qtree, tagsistant_id object_id);

extern void tagsistant_querytree_rebuild_paths(querytree_t *qtree);

extern void tagsistant_set_alias(const char *alias, const char *aliased);
extern gchar *tagsistant_get_alias(const char *alias);
extern void tagsistant_delete_alias(const char *alias);

// returns the type of query reppresented by a querytree_t struct
extern gchar *tagsistant_query_type(querytree_t *qtree);

#define TAGSISTANT_START(line,...) {\
	init_time_profile();\
	start_time_profile();\
	dbg(LOG_INFO, line, ##__VA_ARGS__);\
	tagsistant_start_transaction();\
}

#define TAGSISTANT_STOP_OK(line,...) {\
	tagsistant_commit_transaction();\
	dbg(LOG_INFO, line, ##__VA_ARGS__);\
}

#define TAGSISTANT_STOP_ERROR(line,...) {\
	tagsistant_rollback_transaction();\
	dbg(LOG_ERR, line, ##__VA_ARGS__);\
}

#define tagsistant_check_tagging_consistency(qtree) __tagsistant_check_tagging_consistency(qtree, 0)

extern gchar *tagsistant_ID_strip_from_path(const char *path);
extern gchar *tagsistant_ID_strip_from_querytree(querytree_t *qtree);

extern tagsistant_id tagsistant_ID_extract_from_path(const char *path);
extern tagsistant_id tagsistant_ID_extract_from_querytree(querytree_t *qtree);

extern void tagsistant_show_config();

// vim:ts=4:nocindent:nowrap
