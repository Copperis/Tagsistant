/*
   TAGFS -- mount.tagfs.h
   Copyright (C) 2006-2007 Tx0 <tx0@autistici.org>

   TAGFS mount binary written using FUSE userspace library.
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
#define VERBOSE_DEBUG 1

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

#include <sqlite3.h>
#include <fuse.h>
#include <compat/fuse_opt.h>

#define MAX_TAG_LENGTH 255

/***************\
 * SQL QUERIES *
\***************/
#define CREATE_TAGS_TABLE	"create table tags(id integer primary key autoincrement not null, tagname varchar unique not null);"
#define CREATE_TAGGED_TABLE	"create table tagged(id integer primary key autoincrement not null, tagname varchar not null, filename varchar not null);"
#define CREATE_CACHE_TABLE	"create table cache_queries(id integer primary key autoincrement not null, path text not null, age datetime not null);"
#define CREATE_RESULT_TABLE	"create table cache_results(id integer not null, age datetime not null, filename varchar not null);"

#define CREATE_TAG			"insert into tags(tagname) values('%s');"
#define DELETE_TAG			"delete from tags where tagname = '%s'; delete from tagged where tagname = '%s'; delete from cache_queries where path like '%%/%s/%%' or path like '%%/%s';"
#define TAG_FILE 			"insert into tagged(tagname, filename) values('%s', '%s');"
#define UNTAG_FILE			"delete from tagged where tagname = '%s' and filename = '%s';"
#define RENAME_TAG			"update tags set tagname = '%s' where tagname = '%s'; update tagged set tagname = '%s' where tagname = '%s';"
#define RENAME_FILE			"update tagged set filename = '%s' where filename = '%s';"
#define IS_TAGGED			"select filename from tagged where filename = '%s' and tagname = '%s';"
#define HAS_TAGS			"select tagname from tagged where filename = '%s';" 
#define ALL_FILES_TAGGED	"select filename from tagged where tagname = '%s'"
#define TAG_EXISTS			"select tagname from tags where tagname = '%s';"
#define GET_ALL_TAGS		"select tagname from tags;"
#define IS_CACHED			"select id from cache_queries where path = '%s';"
#define ALL_FILES_IN_CACHE	"select filename from cache_results join cache_queries on cache_queries.id = cache_results.id where path = '%s';"
#define CLEAN_CACHE			"delete from cache_queries where age < datetime('now'); delete from cache_results where age < datetime('now');"
#define ADD_CACHE_ENTRY		"insert into cache_queries(path, age) values('%s', datetime('now', '+15 minutes'));"
#define ADD_RESULT_ENTRY	"insert into cache_results(id, filename, age) values('%lld','%s',datetime('now', '+15 minutes'));"
#define GET_ID_OF_QUERY		"select id from cache_queries where path = '%s';"
#define GET_ID_OF_TAG		"select id from cache_queries where path like '%%/%s/%%' or path like '%%/%s';"
#define DROP_FILES			"delete from cache_results where id = %s;"
#define DROP_FILE			"delete from cache_results where filename = '%s' and id = %s;"
#define DROP_QUERY_BY_ID	"delete from cache_queries where id = %s;"
#define DROP_QUERY			"delete from cache_queries where path like '%%/%s/%%' or path like '%%/%s';"

struct ptree_and_node {
	char *tag;
	struct ptree_and_node *next;
};

typedef struct ptree_and_node ptree_and_node_t;

struct ptree_or_node {
	struct ptree_or_node *next;
	struct ptree_and_node *and_set;
};

typedef struct ptree_or_node ptree_or_node_t;


typedef struct file_handle {
	char *name;
	struct file_handle *next;
} file_handle_t;

/* defines command line options for tagfs mount tool */
struct tagfs {
	int      debug;			/* enable debug */
	int		 foreground;	/* run in foreground */
	int		 singlethread;	/* single thread? */
	int		 readonly;		/* mount filesystem readonly */

	char    *progname;		/* mount.tagfs */
	char    *mountpoint;	/* no clue? */
	char    *repository;	/* where's archived files and tags no? */
	char    *archive;		/* a directory holding all the files */
	char    *tags;			/* a SQLite database on file */

	sqlite3 *dbh;			/* database handle to operate on SQLite thingy */
};

extern struct tagfs tagfs;

extern int debug;
extern int log_enabled;

extern char *get_tag_name(const char *path);
extern char *get_tag_path(const char *tag);
extern char *get_file_path(const char *tag);
extern char *get_tmp_file_path(const char *tag);

extern ptree_or_node_t *build_querytree(const char *path);
extern file_handle_t *build_filetree(ptree_or_node_t *query, const char *path);

extern void destroy_querytree(ptree_or_node_t *pt);
extern void destroy_file_tree(file_handle_t *fh);

#define  do_sql(dbh, statement, callback, firstarg)\
	real_do_sql(dbh, statement, callback, firstarg, strdup(__FILE__), (unsigned int) __LINE__)

extern int real_do_sql(sqlite3 **dbh, char *statement, int (*callback)(void *, int, char **, char **), void *firstarg, char *file, unsigned int line);

// vim:ts=4:nocindent:nowrap
