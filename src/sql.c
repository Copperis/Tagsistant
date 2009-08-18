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
int _tagsistant_query(const char *format, gchar *file, int line, int (*callback)(void *, int, char **, char **), void *firstarg, ...)
{
	va_list ap;
	va_start(ap, firstarg);

	gchar *statement = g_strdup_vprintf(format, ap);
	int res = real_do_sql(NULL, statement, callback, firstarg, file, line);
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

	return_string = argv[0] ? (void *) g_strdup(argv[0]) : NULL;
	return 0;
}

int get_exact_tag_id(const gchar *tagname)
{
	int id;
	tagsistant_query("select id from tags where tagname = \"%s\";", return_integer, &id, tagname);
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
