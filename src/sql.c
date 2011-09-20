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

/* set to 1 if sql dialect support intersect operator */
int tagsistant_sql_backend_have_intersect = 1;

/* DBI connection handler used by subsequent calls to dbi_* functions */
dbi_conn conn;

/* DBI driver family */
int tagsistant_database_driver = TAGSISTANT_NULL_BACKEND;

/**
 * check if requested driver is provided by local DBI installation
 * 
 * @param driver_name string with the name of the driver
 * @return 1 if exists, 0 otherwise
 */
int tagsistant_driver_is_available(const char *driver_name)
{
	int counter = 0;
	int driver_found = 0;
	dbi_driver driver = NULL;

	// list available drivers
	dbg(LOG_INFO, "Available drivers:");

	while ((driver = dbi_driver_list(driver)) != NULL) {
		counter++;
		dbg(LOG_INFO, "  Driver #%d: %s - %s", counter, dbi_driver_get_name(driver), dbi_driver_get_filename(driver));
		if (g_strcmp0(dbi_driver_get_name(driver), driver_name) == 0) {
			driver_found = 1;
		}
	}

	if (!counter) {
		dbg(LOG_ERR, "No SQL driver found! Exiting now.");
		return 0;
	}

	if (!driver_found) {
		dbg(LOG_ERR, "No %s driver found!", driver_name);
		return 0;
	}

	return 1;
}

/**
 * Print into log file SQL connection options
 */
void tagsistant_log_connection_options()
{
	const char *option = NULL;
	int counter = 0;
	dbg(LOG_INFO, "Connection settings: ");
	while ((option = dbi_conn_get_option_list(conn, option)) != NULL) {
		counter++;
		dbg(LOG_INFO, "  Option #%d: %s = %s", counter, option, dbi_conn_get_option(conn, option));
	}
}

/**
 * Parse command line options, create connection object,
 * start the connection and finally create database schema
 */
int tagsistant_db_connection()
{
	// init DBI library
	dbi_initialize(NULL);

	// if no database option has been passed, use default SQLite3
	if (strlen(tagsistant.dboptions) == 0) {
		tagsistant.dboptions = g_strdup("sqlite3");
		dbg(LOG_INFO, "Using default driver: sqlite3");
	}

	dbg(LOG_INFO, "Database options: %s", tagsistant.dboptions);

	// split database option value
	gchar **splitted = g_strsplit(tagsistant.dboptions, ":", 6); /* split up to 6 tokens */

	dbg(LOG_INFO, "Database driver: %s", splitted[0]);

	// initialize different drivers
	if (g_strcmp0(splitted[0], "mysql") == 0) {
		tagsistant_database_driver = TAGSISTANT_DBI_MYSQL_BACKEND;
		if (!tagsistant_driver_is_available("mysql")) exit(1);

		dbg(LOG_INFO, "Database driver used: mysql");

		// unlucky, MySQL does not provide INTERSECT operator
		tagsistant_sql_backend_have_intersect = 0;

		// create connection
		conn = dbi_conn_new("mysql");
		if (NULL == conn) {
			dbg(LOG_ERR, "Error creating MySQL connection");
			exit(1);
		}

		// set connection options
		if (strlen(splitted[1])) {
			dbi_conn_set_option(conn, "host", g_strdup(splitted[1]));
			if (strlen(splitted[2])) {
				dbi_conn_set_option(conn, "dbname", g_strdup(splitted[2]));
				if (strlen(splitted[3])) {
					dbi_conn_set_option(conn, "username", g_strdup(splitted[3]));
					if (strlen(splitted[4])) {
						dbi_conn_set_option(conn, "password", g_strdup(splitted[4]));
					} else {
						dbi_conn_set_option(conn, "password", "tagsistant");
					}
				} else {
					dbi_conn_set_option(conn, "username", "tagsistant");
					dbi_conn_set_option(conn, "password", "tagsistant");
				}
			} else {
				dbi_conn_set_option(conn, "dbname", "tagsistant");
				dbi_conn_set_option(conn, "username", "tagsistant");
				dbi_conn_set_option(conn, "password", "tagsistant");
			}
		} else {
			dbi_conn_set_option(conn, "host", "localhost");
			dbi_conn_set_option(conn, "dbname", "tagsistant");
			dbi_conn_set_option(conn, "username", "tagsistant");
			dbi_conn_set_option(conn, "password", "tagsistant");
		}

		dbi_conn_set_option(conn, "encoding",	"UTF-8");

	} else if ((g_strcmp0(splitted[0], "sqlite3") == 0) || (g_strcmp0(splitted[0], "sqlite"))) {
		tagsistant_database_driver = TAGSISTANT_DBI_SQLITE_BACKEND;
		if (!tagsistant_driver_is_available("sqlite3")) exit(1);

		dbg(LOG_INFO, "Database driver used: sqlite3");

		// create connection
		conn = dbi_conn_new("sqlite3");
		if (NULL == conn) {
			dbg(LOG_ERR, "Error connecting to SQLite3");
			exit(1);
		}

		// set connection options
		dbi_conn_set_option(conn, "dbname",			"tags.sql");
		dbi_conn_set_option(conn, "sqlite3_dbdir",	tagsistant.repository);

	} else {
		dbg(LOG_ERR, "No or wrong database family specified!");
	}

	// free DBI option splitted array
	g_strfreev(splitted);

	// list configured options
	tagsistant_log_connection_options();

	if (tagsistant_sql_backend_have_intersect) {
		dbg(LOG_INFO, "Database supports INTERSECT operator");
	} else {
		dbg(LOG_INFO, "Database does not support INTERSECT operator");
	}

	// try to connect
	if (dbi_conn_connect(conn) < 0) {
		// const char **errmsg = NULL;
		int error = dbi_conn_error(conn, NULL);
		dbg(LOG_ERR, "Could not connect to DB (error %d). Please check the --db settings", error);
		exit(1);
	}

	dbg(LOG_INFO, "SQL connection established, initializing database");

	// create database schema
	switch (tagsistant_database_driver) {
		case TAGSISTANT_DBI_SQLITE_BACKEND:
			tagsistant_query("create table if not exists tags (tag_id integer primary key autoincrement not null, tagname varchar(65) unique not null);", NULL, NULL);
			tagsistant_query("create table if not exists objects (object_id integer not null primary key autoincrement, objectname text(255) not null, path text(1024) unique not null default \"-\", last_autotag timestamp not null default 0);", NULL, NULL);
			tagsistant_query("create table if not exists tagging (object_id integer not null, tag_id integer not null, constraint Tagging_key unique (object_id, tag_id));", NULL, NULL);
			tagsistant_query("create table if not exists relations(relation_id integer primary key autoincrement not null, tag1_id integer not null, relation varchar not null, tag2_id integer not null);", NULL, NULL);
			tagsistant_query("create index if not exists tags_index on tagging (object_id, tag_id);", NULL, NULL);
			tagsistant_query("create index if not exists relations_index on relations (tag1_id, tag2_id);", NULL, NULL);
			tagsistant_query("create index if not exists relations_type_index on relations (relation);", NULL, NULL);
			tagsistant_query("create table if not exists aliases (id integer primary key autoincrement not null, alias varchar(1000) unique not null, aliased varchar(1000) not null)", NULL, NULL);
			break;

		case TAGSISTANT_DBI_MYSQL_BACKEND:
			tagsistant_query("create table if not exists tags (tag_id integer primary key auto_increment not null, tagname varchar(65) unique not null);", NULL, NULL);
			tagsistant_query("create table if not exists objects (object_id integer not null primary key auto_increment, objectname varchar(255) not null, path varchar(1000) unique not null default \"-\", last_autotag timestamp not null default 0);", NULL, NULL);
			tagsistant_query("create table if not exists tagging (object_id integer not null, tag_id integer not null, constraint Tagging_key unique key (object_id, tag_id));", NULL, NULL);
			tagsistant_query("create table if not exists relations(relation_id integer primary key auto_increment not null, tag1_id integer not null, relation varchar(32) not null, tag2_id integer not null);", NULL, NULL);
			tagsistant_query("create index tags_index on tagging (object_id, tag_id);", NULL, NULL);
			tagsistant_query("create index relations_index on relations (tag1_id, tag2_id);", NULL, NULL);
			tagsistant_query("create index relations_type_index on relations (relation);", NULL, NULL);
			tagsistant_query("create table if not exists aliases (id integer primary key auto_increment not null, alias varchar(1000) unique not null, aliased varchar(1000) not null)", NULL, NULL);
			break;

		default:
			break;
	}

	return tagsistant_database_driver;
}

// start a transaction
void tagsistant_start_transaction()
{
	switch (tagsistant_database_driver) {
		case TAGSISTANT_DBI_SQLITE_BACKEND:
			tagsistant_query("begin transaction", NULL, NULL);
			break;

		case TAGSISTANT_DBI_MYSQL_BACKEND:
			tagsistant_query("start transaction", NULL, NULL);
			break;
	}
}

// commit a transaction
void tagsistant_commit_transaction()
{
	tagsistant_query("commit", NULL, NULL);
}

// rollback a transaction
void tagsistant_rollback_transaction()
{
	tagsistant_query("rollback", NULL, NULL);
}

/**
 * Perform SQL queries. This function was added to avoid database opening
 * duplication and better handle SQLite interfacement. If dbh is passed
 * NULL, a new SQLite connection will be opened. Otherwise, existing
 * connection will be used.
 *
 * NEVER use tagistant_real_do_sql() directly. Always use do_sql() macro which adds
 * __FILE__ and __LINE__ transparently for you. Code will be cleaner.
 *
 * \param statement SQL query to be performed
 * \param callback pointer to function to be called on results of SQL query
 * \param firstarg pointer to buffer for callback retured data
 * \param file __FILE__ passed by calling function
 * \param line __LINE__ passed by calling function
 * \return 0 (always, due to SQLite policy)
 */
int tagistant_real_do_sql(char *statement, int (*callback)(void *, dbi_result),
	void *firstarg, char *file, unsigned int line)
{
	// check if statement is not null
	if (NULL == statement) {
		dbg(LOG_ERR, "Null SQL statement");
		return 0;
	}

	// check if connection has been created
	if (NULL == conn) {
		tagsistant_db_connection();
		if (NULL == conn) {
			dbg(LOG_ERR, "ERROR! DBI connection was not initialized!");
			return 0;
		}
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
		if (rows) dbg(LOG_INFO, "Retrieved %d rows", rows);
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
	int res = tagistant_real_do_sql(statement, callback, firstarg, file, line);
	g_free(statement);

	return res;
}

/**
 * return last insert row ID
 */
tagsistant_id tagsistant_last_insert_id()
{
	return dbi_conn_sequence_last(conn, NULL);

	// -------- alternative version -----------------------------------------------

	tagsistant_id ID = 0;

	switch (tagsistant_database_driver) {
		case TAGSISTANT_DBI_SQLITE_BACKEND:
			tagsistant_query("SELECT last_insert_rowid() ", tagsistant_return_integer, &ID);
			break;

		case TAGSISTANT_DBI_MYSQL_BACKEND:
			tagsistant_query("SELECT last_insert_id() ", tagsistant_return_integer, &ID);
			break;
	}

	return ID;
}

/**
 * SQL callback. Return an integer from a query
 *
 * \param return_integer integer pointer cast to void* which holds the integer to be returned
 * \param result dbi_result pointer
 * \return 0 (always, due to SQLite policy, may change in the future)
 */
int tagsistant_return_integer(void *return_integer, dbi_result result)
{
	uint32_t *buffer = (uint32_t *) return_integer;
	*buffer = 0;

	if (tagsistant_database_driver == TAGSISTANT_DBI_SQLITE_BACKEND)
		*buffer = dbi_result_get_ulonglong_idx(result, 1);
	else
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
 * \param return_string string pointer cast to void* which holds the string to be returned
 * \param result dbi_result pointer
 * \return 0 (always, due to SQLite policy, may change in the future)
 */
int tagsistant_return_string(void *return_string, dbi_result result)
{
	gchar **result_string = (gchar **) return_string;

	*result_string = dbi_result_get_string_copy_idx(result, 1);

	dbg(LOG_INFO, "Returning string: %s", *result_string);

	return 0;
}

void tagsistant_sql_create_tag(const gchar *tagname)
{
	tagsistant_query("insert into tags(tagname) values(\"%s\");", NULL, NULL, tagname);
}

int tagsistant_object_is_tagged(tagsistant_id object_id)
{
	tagsistant_id still_exists = 0;

	tagsistant_query(
		"select object_id from tagging where object_id = %d limit 1", 
		tagsistant_return_integer, &still_exists, object_id);
	
	return (still_exists) ? 1 : 0;
}

int tagsistant_object_is_tagged_as(tagsistant_id object_id, tagsistant_id tag_id)
{
	tagsistant_id is_tagged = 0;

	tagsistant_query(
		"select object_id from tagging where object_id = %d and tag_id = %d limit 1", 
		tagsistant_return_integer, &is_tagged, object_id, tag_id);
	
	return (is_tagged) ? 1 : 0;
}

void tagsistant_full_untag_object(tagsistant_id object_id)
{
	tagsistant_query("delete from tagging where object_id = %d", NULL, NULL, object_id);
}

tagsistant_id get_exact_tag_id(const gchar *tagname)
{
	tagsistant_id tag_id = 0;

	tagsistant_query(
		"select tag_id from tags where tagname = \"%s\" limit 1",
		tagsistant_return_integer, &tag_id, tagname);

	return tag_id;
}

void tagsistant_sql_delete_tag(const gchar *tagname)
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

void tagsistant_sql_rename_tag(const gchar *tagname, const gchar *oldtagname)
{
	tagsistant_query("update tags set tagname = \"%s\" where tagname = \"%s\";", NULL, NULL, tagname, oldtagname);\
}
