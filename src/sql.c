/*
   Tagsistant (tagfs) -- sql.c
   Copyright (C) 2006-2008 Tx0 <tx0@strumentiresistenti.org>

   Tagsistant (tagfs) mount binary written using FUSE userspace library.

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

extern struct tagsistant tagsistant;
#include "tagsistant.h"

/**
 * Perform SQL queries. This function was added to avoid database opening
 * duplication and better handle SQLite interfacement. If dbh is passed
 * NULL, a new SQLite connection will be opened. Otherwise, existing
 * connection will be used.
 *
 * NEVER use real_do_sql() directly. Always use do_sql() macro which adds
 * __FILE__ and __LINE__ transparently for you. Code will be cleaner.
 *
 * \param dbh pointer to sqlite3 database handle
 * \param statement SQL query to be performed
 * \param callback pointer to function to be called on results of SQL query
 * \param firstarg pointer to buffer for callback retured data
 * \param file __FILE__ passed by calling function
 * \param line __LINE__ passed by calling function
 * \return 0 (always, due to SQLite policy)
 */
int real_do_sql(sqlite3 **dbh, char *statement, int (*callback)(void *, int, char **, char **),
	void *firstarg, char *file, unsigned int line)
{
	int result = SQLITE_OK;
	sqlite3 *intdbh = NULL;

	if (statement == NULL) {
		dbg(LOG_ERR, "Null SQL statement");
		return 0;
	}

	/*
	 * check if:
	 * 1. no database handle location has been passed (means: use local dbh)
	 * 2. database handle location is empty (means: create new dbh and return it)
	 */
	if ((dbh == NULL) || (*dbh == NULL)) {
		result = sqlite3_open(tagsistant.tags, &intdbh);
		if (result != SQLITE_OK) {
			dbg(LOG_ERR, "Error [%d] opening database %s", result, tagsistant.tags);
			dbg(LOG_ERR, "%s", sqlite3_errmsg(intdbh));
			return 0;
		}
	} else {
		intdbh = *dbh;
	}

	char *sqlerror = NULL;
	if (dbh == NULL) {
		dbg(LOG_INFO, "SQL [disposable]: \"%s\"", statement);
	} else if (*dbh == NULL) {
		dbg(LOG_INFO, "SQL [persistent]: \"%s\"", statement);
	} else {
		dbg(LOG_INFO, "SQL [0x%.8x]: \"%s\"", (unsigned int) *dbh, statement);
	}
	result = sqlite3_exec(intdbh, statement, callback, firstarg, &sqlerror);
	if (result != SQLITE_OK) {
		dbg(LOG_ERR, "SQL error: [%d] %s @%s:%u", result, sqlerror, file, line);
		sqlite3_free(sqlerror);
		return 0;
	}
	// freenull(file);
	sqlite3_free(sqlerror);

	if (dbh == NULL) {
		sqlite3_close(intdbh);
	} else /* if (*dbh == NULL) */ {
		*dbh = intdbh;
	}

	return result;
}

/**
 * Prepare SQL queries and perform them.
 *
 * \param format printf-like string of SQL query
 * \param callback pointer to function to be called on results of SQL query
 * \param firstarg pointer to buffer for callback retured data
 * \return 0 (always, due to SQLite policy)
 */
int tagsistant_query(const char *format, int (*callback)(void *, int, char **, char **), void *firstarg, ...)
{
	va_list ap;
	va_start(ap, firstarg);

	gchar *statement = g_strdup_vprintf(format, ap);
	int res = do_sql(NULL, statement, callback, firstarg);
	g_free(statement);

	return res;
}

/***************\
 * SQL QUERIES *
\***************/
#define CREATE_TAGS_TABLE		"create table tags(id integer primary key autoincrement not null, tagname varchar unique not null);"
#define CREATE_TAGGED_TABLE		"create table tagged(id integer primary key autoincrement not null, tagname varchar not null, filename varchar not null);"
#if TAGSISTANT_USE_CACHE_LAYER
#	define CREATE_CACHE_TABLE	"create table cache_queries(id integer primary key autoincrement not null, path text not null, age datetime not null);"
#	define CREATE_RESULT_TABLE	"create table cache_results(id integer not null, age datetime not null, filename varchar not null);"
#endif
#define CREATE_RELATIONS_TABLE	"create table relations(id integer primary key autoincrement not null, tag1 varchar not null, relation varchar not null, tag2 varchar not null);"

void sql_create_tag(const char* tagname) {
	tagsistant_query("insert into tags(tagname) values(\"%s\");", NULL, NULL, tagname);
}

void sql_delete_tag(const char *tagname) {
	tagsistant_query("delete from tags where tagname = \"%s\"", NULL, NULL, tagname);
	tagsistant_query("delete from tagged where tagname = \"%s\";", NULL, NULL, tagname);
	tagsistant_query("delete from relations where tag1 = \"%s\" or tag2 = \"%s\";", NULL, NULL, tagname, tagname);
#if TAGSISTANT_USE_CACHE_LAYER
	tagsistant_query("delete from cache_queries where path like \"%%/%s/%%\" or path like \"%%/%s\";", NULL, NULL, tagname, tagname);
#endif
}

void sql_tag_file(const char *tagname, const char *filename) {
	tagsistant_query("insert into tagged(tagname, filename) values(\"%s\", \"%s\");", NULL, NULL, tagname, filename);
}

void sql_untag_file(const char *tagname, const char *filename) {
	tagsistant_query("delete from tagged where tagname = \"%s\" and filename = \"%s\";", NULL, NULL, tagname, filename);
}

void sql_rename_tag(const char *tagname) {
	tagsistant_query("update tags set tagname = \"%s\" where tagname = \"%s\";", NULL, NULL, tagname, tagname);
	tagsistant_query("update tagged set tagname = \"%s\" where tagname = \"%s\";", NULL, NULL, tagname, tagname);
}

void sql_rename_file(const char *oldname, const char *newname) {
	tagsistant_query("update tagged set filename = \"%s\" where filename = \"%s\";", NULL, NULL, newname, oldname);
#if TAGSISTANT_USE_CACHE_LAYER
	tagsistant_query("update cache_results set filename = \"%s\" where filename = \"%s\"", NULL, NULL, newname, oldname);
#endif
}

#define ALL_FILES_TAGGED		"select filename from tagged where tagname = \"%s\""
#define TAG_EXISTS				"select tagname from tags where tagname = \"%s\";"
#define GET_ALL_TAGS			"select tagname from tags;"
	
#if TAGSISTANT_USE_CACHE_LAYER
#	define IS_CACHED			"select id from cache_queries where path = \"%s\";"
#	define ALL_FILES_IN_CACHE	"select filename from cache_results join cache_queries on cache_queries.id = cache_results.id where path = \"%s\";"
#	define CLEAN_CACHE			"delete from cache_queries where age < datetime(\"now\"); delete from cache_results where age < datetime(\"now\");"
#	define ADD_CACHE_ENTRY		"insert into cache_queries(path, age) values(\"%s\", datetime(\"now\", \"+15 minutes\"));"
#	define ADD_RESULT_ENTRY		"insert into cache_results(id, filename, age) values(\"%lld\",\"%s\",datetime(\"now\", \"+15 minutes\"));"
#	define GET_ID_OF_QUERY		"select id from cache_queries where path = \"%s\";"
#	define GET_ID_OF_TAG		"select id from cache_queries where path like \"%%/%s/%%\" or path like \"%%/%s\";"
#	define DROP_FILES			"delete from cache_results where id = %s;"
#	define DROP_FILE			"delete from cache_results where filename = \"%s\" and id = %s;"
#	define DROP_QUERY_BY_ID		"delete from cache_queries where id = %s;"
#	define DROP_QUERY			"delete from cache_queries where path like \"%%/%s/%%\" or path like \"%%/%s\";"
#endif
