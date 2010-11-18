/*
   Tagsistant (tagfs) -- sql.c
   Copyright (C) 2006-2010 Tx0 <tx0@strumentiresistenti.org>

   Tagsistant (tagfs) DBI-based SQL backend

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

/* DBI connection handler used by subsequent calls to dbi_* functions */
dbi_conn conn;

#if TAGSISTANT_SQL_BACKEND == TAGSISTANT_MYSQL_BACKEND

/**
 * opens DBI connection to MySQL
 */
void tagsistant_db_connection()
{
	dbi_initialize(NULL);

	// list available drivers
	dbg(LOG_INFO, "Available drivers:");
	dbi_driver driver = NULL;
	int counter = 0;
	int driver_found = 0;
	while ((driver = dbi_driver_list(driver)) != NULL) {
		counter++;
		dbg(LOG_INFO, "  Driver #%d: %s - %s", counter, dbi_driver_get_name(driver), dbi_driver_get_filename(driver));
		if (strcmp(dbi_driver_get_name(driver), "mysql") == 0) {
			driver_found = 1;
		}
	}

	if (!counter) {
		dbg(LOG_ERR, "No SQL driver found! Exiting now.");
		exit(1);
	}

	if (!driver_found) {
		dbg(LOG_ERR, "No MySQL driver found!");
		exit(1);
	}

	// open a MySQL connection
	if (NULL == (conn = dbi_conn_new("mysql"))) {
		dbg(LOG_ERR, "Error connecting to MySQL");
		exit(1);
	}

	dbi_conn_set_option(conn, "host",		"localhost");
	dbi_conn_set_option(conn, "username",	"tagsistant");
	dbi_conn_set_option(conn, "password",	"tagsistant");
	dbi_conn_set_option(conn, "dbname",		"tagsistant");
	dbi_conn_set_option(conn, "encoding",	"UTF-8");

	// list available options
	const char *option = NULL;
	counter = 0;
	dbg(LOG_INFO, "Connection settings: ");
	while ((option = dbi_conn_get_option_list(conn, option)) != NULL) {
		counter++;
		dbg(LOG_INFO, "  Option #%d: %s = %s", counter, option, dbi_conn_get_option(conn, option));
	}
	
	if (dbi_conn_connect(conn) < 0) {
		const char *errmsg;
		(void) dbi_conn_error(conn, &errmsg);
		dbg(LOG_ERR, "Could not connect to MySQL: %s. Please check the option settings", errmsg);
		exit(1);
	}

	dbg(LOG_INFO, "MySQL: Connection established, initializing database");

	tagsistant_query("create table if not exists tags (tag_id integer primary key auto_increment not null, tagname varchar(65) unique not null);", NULL, NULL);
	tagsistant_query("create table if not exists objects (object_id integer not null primary key auto_increment, objectname varchar(255) not null, path varchar(1000) unique not null);", NULL, NULL);
	tagsistant_query("create table if not exists tagging (object_id integer not null, tag_id integer not null, constraint Tagging_key unique key (object_id, tag_id));", NULL, NULL);
	tagsistant_query("create table if not exists relations(relation_id integer primary key auto_increment not null, tag1_id integer not null, relation varchar(32) not null, tag2_id integer not null);", NULL, NULL);
	tagsistant_query("create index tags_index on tagging (object_id, tag_id);", NULL, NULL);
	tagsistant_query("create index relations_index on relations (tag1_id, tag2_id);", NULL, NULL);
	tagsistant_query("create index relations_type_index on relations (relation);", NULL, NULL);

#if 0
	// do some testing
	tagsistant_query("insert into tags (tagname) values (\"testtag1\")", NULL, NULL);
	tagsistant_query("insert into tags (tagname) values (\"testtag2\")", NULL, NULL);
	int i = 0;
	tagsistant_query("select tag_id from tags where tagname = \"testtag2\"", return_integer, &i);
	dbg(LOG_INFO, "Preliminary test: id should be 2 and is %d", i);
#endif
}

/**
 * Perform SQL queries. This function was added to avoid database opening
 * duplication and better handle SQLite interfacement. If dbh is passed
 * NULL, a new SQLite connection will be opened. Otherwise, existing
 * connection will be used.
 *
 * NEVER use real_do_sql() directly. Always use do_sql() macro which adds
 * __FILE__ and __LINE__ transparently for you. Code will be cleaner.
 *
 * \param statement SQL query to be performed
 * \param callback pointer to function to be called on results of SQL query
 * \param firstarg pointer to buffer for callback retured data
 * \param file __FILE__ passed by calling function
 * \param line __LINE__ passed by calling function
 * \return 0 (always, due to SQLite policy)
 */
int real_do_sql(char *statement, int (*callback)(void *, dbi_result),
	void *firstarg, char *file, unsigned int line)
{
	// check if statement is not null
	if (NULL == statement) {
		dbg(LOG_ERR, "Null SQL statement");
		return 0;
	}

	// check if connection has been created
	if (NULL == conn) {
		dbg(LOG_ERR, "ERROR! DBI connection was not initialized!");
		return 0;
	}

	int counter = 0;
	while (counter < 3) {
		if (dbi_conn_ping(conn)) break;
		dbi_conn_connect(conn);
		counter++;
	}

	if (!dbi_conn_ping(conn)) {
		dbg(LOG_ERR, "ERROR! DBI Connection has gone!");
		return 0;
	}

	dbg(LOG_INFO, "SQL: [%s] @%s:%d", statement, file, line);

	// declare static mutex
	GStaticMutex mtx = G_STATIC_MUTEX_INIT;

	// do the real query
	g_static_mutex_lock(&mtx);
	dbi_result result = dbi_conn_queryf(conn, statement);
	g_static_mutex_unlock(&mtx);

	int rows = 0;

	// call the callback function on results or report an error
	if (result) {
		while (dbi_result_next_row(result)) {
			callback(firstarg, result);
			rows++;
		}
		dbi_result_free(result);
		return rows;
	}

	const char *errmsg;
	(void) dbi_conn_error(conn, &errmsg);
	dbg(LOG_ERR, "SQL Error: %s.", errmsg);
	return -1;
}

/**
 * Prepare SQL queries and perform them.
 *
 * \param format printf-like string of SQL query
 * \param callback pointer to function to be called on results of SQL query
 * \param firstarg pointer to buffer for callback retured data
 * \return 0 (always, due to SQLite policy)
 */
int _tagsistant_query(const char *format, gchar *file, int line, int (*callback)(void *, dbi_result), void *firstarg, ...)
{
	va_list ap;
	va_start(ap, firstarg);

	gchar *statement = g_strdup_vprintf(format, ap);
	int res = real_do_sql(statement, callback, firstarg, file, line);
	g_free(statement);

	return res;
}

/**
 * SQL callback. Return an integer from a query
 *
 * \param return_integer integer pointer cast to void* which holds the integer to be returned
 * \param result dbi_result pointer
 * \return 0 (always, due to SQLite policy, may change in the future)
 */
int return_integer(void *return_integer, dbi_result result)
{
	uint32_t *buffer = (uint32_t *) return_integer;

	*buffer = dbi_result_get_uint_idx(result, 1);

	dbg(LOG_INFO, "Returning integer: %d", *buffer);

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
 * \param result dbi_result pointer
 * \return 0 (always, due to SQLite policy, may change in the future)
 */
int return_string(void *return_string, dbi_result result)
{
	gchar **result_string = (gchar **) return_string;

	*result_string = dbi_result_get_string_copy_idx(result, 1);

	dbg(LOG_INFO, "Returning string: %s", *result_string);

	return 0;
}

int get_exact_tag_id(const gchar *tagname)
{
	int id = 0;
	tagsistant_query("select tag_id from tags where tagname = \"%s\";", return_integer, &id, tagname);
	return id;
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

#endif /* TAGSISTANT_SQL_BACKEND == "MYSQL" */
