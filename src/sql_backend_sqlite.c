/*
   Tagsistant (tagfs) -- sql_backend_sqlite.c
   Copyright (C) 2006-2010 Tx0 <tx0@strumentiresistenti.org>

   Tagsistant (tagfs) SQLite backend

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

#if TAGSISTANT_SQL_BACKEND == TAGSISTANT_SQLITE_BACKEND

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
		dbg(LOG_INFO, "SQL [disposable]: [%s] @%s:%d", statement, file, line);
	} else if (*dbh == NULL) {
		dbg(LOG_INFO, "SQL [persistent]: [%s] @%s:%d", statement, file, line);
	} else {
		dbg(LOG_INFO, "SQL [0x%.8x]: [%s] @%s:%d", (unsigned int) *dbh, statement, file, line);
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
int _tagsistant_query(const char *format, gchar *file, int line, int (*callback)(void *, int, char **, char **), void *firstarg, ...)
{
	va_list ap;
	va_start(ap, firstarg);

	// declare static SQLite handle
	static sqlite3 *dbh = NULL;

	// declare static mutex
	GStaticMutex mtx = G_STATIC_MUTEX_INIT;

	// format the SQL statement
	gchar *statement = g_strdup_vprintf(format, ap);

	// lock the mutex
	g_static_mutex_lock(&mtx);

	// do the query
	int res = real_do_sql(&dbh, statement, callback, firstarg, file, line);

	// unlock the mutex
	g_static_mutex_unlock(&mtx);

	// free the statement
	g_free(statement);

	return res;
}

/**
 * SQL callback. Return an integer from a query
 *
 * \param return_integer integer pointer cast to void* which holds the integer to be returned
 * \param argc counter of argv arguments
 * \param argv array of SQL given results
 * \param azColName array of SQL column names
 * \return 0 (always, due to SQLite policy)
 */
int return_integer(void *return_integer, int argc, char **argv, char **azColName)
{
	(void) argc;
	(void) azColName;
	uint32_t *buffer = (uint32_t *) return_integer;

	if (argv[0] != NULL) {
		sscanf(argv[0], "%u", buffer);
	}

	dbg(LOG_INFO, "Returning %s as %d", argv[0], *buffer);

	return 0;
}

/**
 * SQL callback. Return a string from a query
 * Should be called as in:
 *
 *   gchar *string;
 *   tagsistant_query("SQL statement;", return_string, &string); // note the &
 * 
 * \param return_integer string pointer cast to void* which holds the string to be returned
 * \param argc counter of argv arguments
 * \param argv array of SQL given results
 * \param azColName array of SQL column names
 * \return 0 (always, due to SQLite policy)
 */
int return_string(void *return_string, int argc, char **argv, char **azColName)
{
	(void) argc;
	(void) azColName;

	gchar **result = (gchar **) return_string;
	*result = (argv[0] != NULL) ? g_strdup(argv[0]) : NULL;
	return 0;
}

int get_exact_tag_id(const gchar *tagname)
{
	int id;
	tagsistant_query("select tag_id from tags where tagname = \"%s\";", return_integer, &id, tagname);
	return id;
}

/**
 * SQL callback. Report if an entity exists in database.
 *
 * \param exist_buffer integer pointer cast to void* which holds 1 if entity exists, 0 otherwise
 * \param argc counter of argv arguments
 * \param argv array of SQL given results
 * \param azColName array of SQL column names
 * \return 0 (always, due to SQLite policy)
 */
int report_if_exists(void *exists_buffer, int argc, char **argv, char **azColName)
{
	(void) argc;
	(void) azColName;
	int *exists = (int *) exists_buffer;
	if (argv[0] != NULL) {
		*exists = 1;
	} else {
		*exists = 0;
	}
	return 0;
}

gboolean sql_tag_exists(const gchar* tagname)
{
	gboolean exists;
	tagsistant_query("select count(tagname) from tags where tagname = \"%s\";", return_integer, &exists, tagname);
	return exists;
}

/**
 * add an object to the database.
 *
 * @param basename is the object basename; can't be null
 * @param @path the full path; if it's NULL, the object is relative and path
 * has to be computed using the object id
 *
 * @return the ID of the object
 */
tagsistant_id sql_create_object(const gchar *basename, const gchar *path)
{
	tagsistant_id ID = 0;

	/* check if object is already stored */
	if (path != NULL) {
		tagsistant_query(
			"select id from objects where path = \"%s\" and basename = \"%s\"",
			return_integer, &ID, path, basename);
	}

	/* create the object, if does not exists */
	if (!ID) {
		if (path) {
			tagsistant_query("insert into objects (basename, path) values (\"%s\", \"%s\")", NULL, NULL, basename, path);
		} else {
			gchar *guessed_path = g_strdup_printf("%s%s%u", TAGSISTANT_ARCHIVE_PLACEHOLDER, G_DIR_SEPARATOR_S, ID);
			tagsistant_query("insert into objects (basename, path) values (\"%s\", \"%s\")", NULL, NULL, basename, guessed_path);
			g_free(guessed_path);
		}
		tagsistant_query("select last_insert_rowid()", return_integer, &ID);
	}

	return ID;
}

void sql_delete_object(tagsistant_id object_id)
{
	tagsistant_query("delete from objects where object_id = %d", NULL, NULL, object_id);
	// ... to be completed???
}

/**
 * Return the full objectpath of a object_id
 *
 * @param object_id the ID of the object inside tagsistant sql database
 *
 * @return the path of the object, directly useable, no matter if internal
 * or external to tagsistant backend
 */
gchar *sql_objectpath(tagsistant_id object_id)
{
	gchar *objectpath = NULL;

	tagsistant_query(
		"select replace(path,\"%s\",\"%s\") || \"%s\" || objectname from objects where id = %d",
		return_string, &objectpath,
		TAGSISTANT_ARCHIVE_PLACEHOLDER, tagsistant.archive, G_DIR_SEPARATOR_S, object_id);
	
	return objectpath;
}

void sql_create_tag(const gchar *tagname)
{
	tagsistant_query("insert into tags(tagname) values(\"%s\");", NULL, NULL, tagname);
}

tagsistant_id sql_get_tag_id(const gchar *tagname)
{
	tagsistant_id tag_id;

	tagsistant_query("select tag_id from tags where tagname = \"%s\"", return_integer, &tag_id, tagname);

	return tag_id;
}

void sql_delete_tag(const gchar *tagname)
{
	tagsistant_id tag_id = sql_get_tag_id(tagname);

	tagsistant_query("delete from tags where tagname = \"%s\";", NULL, NULL, tagname);
	tagsistant_query("delete from tagging where tag_id = \"%d\";", NULL, NULL, tag_id);
	tagsistant_query("delete from relations where tag1_id = \"%d\" or tag2_id = \"%d\";", NULL, NULL, tag_id, tag_id);
}

void sql_tag_object(const gchar *tagname, tagsistant_id object_id)
{
	tagsistant_query("insert into tags(tagname) values(\"%s\");", NULL, NULL, tagname);

	tagsistant_id tag_id = sql_get_tag_id(tagname);

	dbg(LOG_INFO, "Tagging object %d as %s (%d)", object_id, tagname, tag_id);

	tagsistant_query("insert into tagging(tag_id, object_id) values(\"%d\", \"%d\");", NULL, NULL, tag_id, object_id);
}

void sql_untag_object(const gchar *tagname, tagsistant_id object_id)
{
	int tag_id = sql_get_tag_id(tagname);

	dbg(LOG_INFO, "Untagging object %d from tag %s (%d)", object_id, tagname, tag_id);

	tagsistant_query("delete from tagging where tag_id = \"%d\" and object_id = \"%d\";", NULL, NULL, tag_id, object_id);\
}

void sql_rename_tag(const gchar *tagname, const gchar *oldtagname)
{
	tagsistant_query("update tags set tagname = \"%s\" where tagname = \"%s\";", NULL, NULL, tagname, oldtagname);\
}

void sql_rename_object(tagsistant_id object_id, const gchar *newname)
{
	tagsistant_query("update objects set objectname = \"%s\" where object_id = %d", NULL, NULL, newname, object_id);
}

#endif /* TAGSISTANT_SQL_BACKEND == "SQLITE" */
