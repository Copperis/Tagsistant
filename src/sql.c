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

#include "tagsistant.h"

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

//	dbg(LOG_INFO, "Available drivers:");
	while ((driver = dbi_driver_list(driver)) != NULL) {
		counter++;
		// dbg(LOG_INFO, "  Driver #%d: %s - %s", counter, dbi_driver_get_name(driver), dbi_driver_get_filename(driver));
		if (g_strcmp0(dbi_driver_get_name(driver), driver_name) == 0) {
			driver_found = 1;
		}
	}

	if (!counter) {
		dbg(LOG_ERR, "No SQL driver found! Exiting now.");
		return(0);
	}

	if (!driver_found) {
		dbg(LOG_ERR, "No %s driver found!", driver_name);
		return(0);
	}

	return(1);
}

gchar **dboptions;

void tagsistant_db_init()
{
	// initialize DBI library
	dbi_initialize(NULL);

	// by default, DBI backend provides intersect
	tagsistant.sql_backend_have_intersect = 1;
	tagsistant.sql_database_driver = TAGSISTANT_NULL_BACKEND;

	// if no database option has been passed, use default SQLite3
	if (strlen(tagsistant.dboptions) == 0) {
		tagsistant.dboptions = g_strdup("sqlite3");
		dbg(LOG_INFO, "Using default driver: sqlite3");
	}

	dbg(LOG_INFO, "Database options: %s", tagsistant.dboptions);

	// split database option value
	dboptions = g_strsplit(tagsistant.dboptions, ":", 6); /* split up to 6 tokens */

	dbg(LOG_INFO, "Database driver: %s", dboptions[0]);
}

/**
 * Parse command line options, create connection object,
 * start the connection and finally create database schema
 */
dbi_conn tagsistant_db_connection()
{
	/* DBI connection handler used by subsequent calls to dbi_* functions */
	dbi_conn tagsistant_dbi_conn;

	// initialize different drivers
	if (g_strcmp0(dboptions[0], "mysql") == 0) {
		tagsistant.sql_database_driver = TAGSISTANT_DBI_MYSQL_BACKEND;
		if (!tagsistant_driver_is_available("mysql")) exit(1);

		// unlucky, MySQL does not provide INTERSECT operator
		tagsistant.sql_backend_have_intersect = 0;

		// create connection
		tagsistant_dbi_conn = dbi_conn_new("mysql");
		if (NULL == tagsistant_dbi_conn) {
			dbg(LOG_ERR, "Error creating MySQL connection");
			exit(1);
		}

		dbi_conn_set_option(tagsistant_dbi_conn, "host",     dboptions[1] ? g_strdup(dboptions[1]) : "localhost" );
		dbi_conn_set_option(tagsistant_dbi_conn, "dbname",   dboptions[2] ? g_strdup(dboptions[2]) : "tagsistant");
		dbi_conn_set_option(tagsistant_dbi_conn, "username", dboptions[3] ? g_strdup(dboptions[3]) : "tagsistant");
		dbi_conn_set_option(tagsistant_dbi_conn, "password", dboptions[4] ? g_strdup(dboptions[4]) : "tagsistant");
		dbi_conn_set_option(tagsistant_dbi_conn, "encoding", "UTF-8");

	} else if ((g_strcmp0(dboptions[0], "sqlite3") == 0) || (g_strcmp0(dboptions[0], "sqlite"))) {
		tagsistant.sql_database_driver = TAGSISTANT_DBI_SQLITE_BACKEND;
		if (!tagsistant_driver_is_available("sqlite3")) exit(1);

		// create connection
		tagsistant_dbi_conn = dbi_conn_new("sqlite3");
		if (NULL == tagsistant_dbi_conn) {
			dbg(LOG_ERR, "Error connecting to SQLite3");
			exit(1);
		}

		// set connection options
		dbi_conn_set_option(tagsistant_dbi_conn, "dbname", "tags.sql");
		dbi_conn_set_option(tagsistant_dbi_conn, "sqlite3_dbdir", tagsistant.repository);

	} else {
		dbg(LOG_ERR, "No or wrong database family specified!");
		exit(1);
	}

#if TAGSISTANT_VERBOSE_LOGGING
	// list configured options
	const char *option = NULL;
	int counter = 0;
	dbg(LOG_INFO, "Connection settings: ");
	while ((option = dbi_conn_get_option_list(tagsistant_dbi_conn, option))	!= NULL ) {
		counter++;
		dbg(LOG_INFO, "  Option #%d: %s = %s", counter, option, dbi_conn_get_option(tagsistant_dbi_conn, option));
	}

	// tell if backend have INTERSECT
	if (tagsistant.sql_backend_have_intersect) {
		dbg(LOG_INFO, "Database supports INTERSECT operator");
	} else {
		dbg(LOG_INFO, "Database does not support INTERSECT operator");
	}
#endif

	// try to connect
	if (dbi_conn_connect(tagsistant_dbi_conn) < 0) {
		int error = dbi_conn_error(tagsistant_dbi_conn, NULL);
		dbg(LOG_ERR, "Could not connect to DB (error %d). Please check the --db settings", error);
		exit(1);
	}

#if TAGSISTANT_VERBOSE_LOGGING
	dbg(LOG_INFO, "SQL connection established");
#endif

	/* start a transaction */
	switch (tagsistant.sql_database_driver) {
		case TAGSISTANT_DBI_SQLITE_BACKEND:
			tagsistant_query("begin transaction", tagsistant_dbi_conn, NULL, NULL);
			break;

		case TAGSISTANT_DBI_MYSQL_BACKEND:
			tagsistant_query("start transaction", tagsistant_dbi_conn, NULL, NULL);
			break;
	}

	return(tagsistant_dbi_conn);
}

/**
 * Create DB schema
 */
void tagsistant_create_schema()
{
	dbi_conn conn = tagsistant_db_connection();

	// create database schema
	switch (tagsistant.sql_database_driver) {
		case TAGSISTANT_DBI_SQLITE_BACKEND:
			tagsistant_query("create table if not exists tags (tag_id integer primary key autoincrement not null, tagname varchar(65) unique not null);", conn, NULL, NULL);
			tagsistant_query("create table if not exists objects (inode integer not null primary key autoincrement, objectname text(255) not null, last_autotag timestamp not null default 0, checksum text(40) not null default \"\");", conn, NULL, NULL);
			tagsistant_query("create table if not exists tagging (inode integer not null, tag_id integer not null, constraint Tagging_key unique (inode, tag_id));", conn, NULL, NULL);
			tagsistant_query("create table if not exists relations(relation_id integer primary key autoincrement not null, tag1_id integer not null, relation varchar not null, tag2_id integer not null);", conn, NULL, NULL);
			tagsistant_query("create index if not exists tags_index on tagging (inode, tag_id);", conn, NULL, NULL);
			tagsistant_query("create index if not exists relations_index on relations (tag1_id, tag2_id);", conn, NULL, NULL);
			tagsistant_query("create index if not exists relations_type_index on relations (relation);", conn, NULL, NULL);
			break;

		case TAGSISTANT_DBI_MYSQL_BACKEND:
			tagsistant_query("create table if not exists tags (tag_id integer primary key auto_increment not null, tagname varchar(65) unique not null);", conn, NULL, NULL);
			tagsistant_query("create table if not exists objects (inode integer not null primary key auto_increment, objectname varchar(255) not null, last_autotag timestamp not null default 0, checksum varchar(40) not null default \"\");", conn, NULL, NULL);
			tagsistant_query("create table if not exists tagging (inode integer not null, tag_id integer not null, constraint Tagging_key unique key (inode, tag_id));", conn, NULL, NULL);
			tagsistant_query("create table if not exists relations(relation_id integer primary key auto_increment not null, tag1_id integer not null, relation varchar(32) not null, tag2_id integer not null);", conn, NULL, NULL);
			tagsistant_query("create index tags_index on tagging (inode, tag_id);", conn, NULL, NULL);
			tagsistant_query("create index relations_index on relations (tag1_id, tag2_id);", conn, NULL, NULL);
			tagsistant_query("create index relations_type_index on relations (relation);", conn, NULL, NULL);
			break;

		default:
			break;
	}

	tagsistant_commit_transaction(conn);
	dbi_conn_close(conn);
}

/**
 * Prepare SQL queries and perform them.
 *
 * @param format printf-like string of SQL query
 * @param callback pointer to function to be called on results of SQL query
 * @param firstarg pointer to buffer for callback returned data
 * @return 0 (always, due to SQLite policy)
 */
int tagsistant_real_query(
	dbi_conn conn,
	const char *format,
	gchar *file,
	int line,
	int (*callback)(void *, dbi_result),
	void *firstarg,
	...)
{
	va_list ap;
	va_start(ap, firstarg);

	// check if connection has been created
	if (NULL == conn) {
		dbg(LOG_ERR, "ERROR! DBI connection was not initialized!");
		return(0);
	}

	// check if the connection is alive
	if (!dbi_conn_ping(conn) && (dbi_conn_connect(conn) < 0)) {
		dbg(LOG_ERR, "ERROR! DBI Connection has gone!");
		return(0);
	}

	gchar *statement = g_strdup_vprintf(format, ap);

	// check if statement is not null
	if (NULL == statement) {
		dbg(LOG_ERR, "Null SQL statement");
		return(0);
	}

#if TAGSISTANT_VERBOSE_LOGGING
	dbg(LOG_INFO, "SQL: [%s] @%s:%d", statement, file, line);
#endif

	// do the query
	dbi_result result = dbi_conn_query(conn, statement);

	// call the callback function on results or report an error
	int rows = 0;

	if (result) {
		if (callback) {
			while (dbi_result_next_row(result)) {
				callback(firstarg, result);
				rows++;
			}
		}
		dbi_result_free(result);
	} else {
		// get the error message
		const char *errmsg;
		int err = dbi_conn_error(conn, &errmsg);
		if ((-1 == err) && errmsg) dbg(LOG_ERR, "Error: %s.", errmsg);
	}

	g_free(statement);
	return(rows);
}

/**
 * return(last insert row inode)
 */
tagsistant_inode tagsistant_last_insert_id(dbi_conn conn)
{
	return(dbi_conn_sequence_last(conn, NULL));

#if 0
	// -------- alternative version -----------------------------------------------

	tagsistant_inode inode = 0;

	switch (tagsistant.sql_database_driver) {
		case TAGSISTANT_DBI_SQLITE_BACKEND:
			tagsistant_query("SELECT last_insert_rowid() ", conn, tagsistant_return_integer, &inode);
			break;

		case TAGSISTANT_DBI_MYSQL_BACKEND:
			tagsistant_query("SELECT last_insert_id() ", conn, tagsistant_return_integer, &inode);
			break;
	}

	return (inode);
#endif
}

/**
 * SQL callback. Return an integer from a query
 *
 * @param return_integer integer pointer cast to void* which holds the integer to be returned
 * @param result dbi_result pointer
 * @return 0 (always, due to SQLite policy, may change in the future)
 */
int tagsistant_return_integer(void *return_integer, dbi_result result)
{
	uint32_t *buffer = (uint32_t *) return_integer;
	*buffer = 0;

	if (tagsistant.sql_database_driver == TAGSISTANT_DBI_SQLITE_BACKEND)
		*buffer = dbi_result_get_ulonglong_idx(result, 1);
	else
		*buffer = dbi_result_get_uint_idx(result, 1);

#if TAGSISTANT_VERBOSE_LOGGING
	dbg(LOG_INFO, "Returning integer: %d", *buffer);
#endif

	return (0);
}

/**
 * SQL callback. Return a string from a query
 * Should be called as in:
 *
 *   gchar *string;
 *   tagsistant_query("SQL statement;", return_string, &string); // note the &
 * 
 * @param return_string string pointer cast to void* which holds the string to be returned
 * @param result dbi_result pointer
 * @return 0 (always, due to SQLite policy, may change in the future)
 */
int tagsistant_return_string(void *return_string, dbi_result result)
{
	gchar **result_string = (gchar **) return_string;

	*result_string = dbi_result_get_string_copy_idx(result, 1);

#if TAGSISTANT_VERBOSE_LOGGING
	dbg(LOG_INFO, "Returning string: %s", *result_string);
#endif

	return (0);
}

void tagsistant_sql_create_tag(dbi_conn conn, const gchar *tagname)
{
	tagsistant_query("insert into tags(tagname) values(\"%s\");", conn, NULL, NULL, tagname);
}

int tagsistant_object_is_tagged(dbi_conn conn, tagsistant_inode inode)
{
	tagsistant_inode still_exists = 0;

	tagsistant_query(
		"select inode from tagging where inode = %d limit 1",
		conn, tagsistant_return_integer, &still_exists, inode);
	
	return ((still_exists) ? 1 : 0);
}

int tagsistant_object_is_tagged_as(dbi_conn conn, tagsistant_inode inode, tagsistant_inode tag_id)
{
	tagsistant_inode is_tagged = 0;

	tagsistant_query(
		"select inode from tagging where inode = %d and tag_id = %d limit 1",
		conn, tagsistant_return_integer, &is_tagged, inode, tag_id);
	
	return ((is_tagged) ? 1 : 0);
}

void tagsistant_full_untag_object(dbi_conn conn, tagsistant_inode inode)
{
	tagsistant_query("delete from tagging where inode = %d", conn, NULL, NULL, inode);
}

tagsistant_inode tagsistant_sql_get_tag_id(dbi_conn conn, const gchar *tagname)
{
	tagsistant_inode tag_id = 0;

	tagsistant_query(
		"select tag_id from tags where tagname = \"%s\" limit 1",
		conn, tagsistant_return_integer, &tag_id, tagname);

	return (tag_id);
}

void tagsistant_sql_delete_tag(dbi_conn conn, const gchar *tagname)
{
	tagsistant_inode tag_id = tagsistant_sql_get_tag_id(conn, tagname);

	tagsistant_query("delete from tags where tagname = \"%s\";", conn, NULL, NULL, tagname);
	tagsistant_query("delete from tagging where tag_id = \"%d\";", conn, NULL, NULL, tag_id);
	tagsistant_query("delete from relations where tag1_id = \"%d\" or tag2_id = \"%d\";", conn, NULL, NULL, tag_id, tag_id);
}

void tagsistant_sql_tag_object(dbi_conn conn, const gchar *tagname, tagsistant_inode inode)
{
	tagsistant_query("insert into tags(tagname) values(\"%s\");", conn, NULL, NULL, tagname);

	tagsistant_inode tag_id = tagsistant_sql_get_tag_id(conn, tagname);

#if TAGSISTANT_VERBOSE_LOGGING
	dbg(LOG_INFO, "Tagging object %d as %s (%d)", inode, tagname, tag_id);
#endif

	tagsistant_query("insert into tagging(tag_id, inode) values(\"%d\", \"%d\");", conn, NULL, NULL, tag_id, inode);
}

void tagsistant_sql_untag_object(dbi_conn conn, const gchar *tagname, tagsistant_inode inode)
{
	tagsistant_inode tag_id = tagsistant_sql_get_tag_id(conn, tagname);

#if TAGSISTANT_VERBOSE_LOGGING
	dbg(LOG_INFO, "Untagging object %d from tag %s (%d)", inode, tagname, tag_id);
#endif

	tagsistant_query("delete from tagging where tag_id = \"%d\" and inode = \"%d\";", conn, NULL, NULL, tag_id, inode);
}

void tagsistant_sql_rename_tag(dbi_conn conn, const gchar *tagname, const gchar *oldtagname)
{
	tagsistant_query("update tags set tagname = \"%s\" where tagname = \"%s\";", conn, NULL, NULL, tagname, oldtagname);
}
