/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor Boston, MA 02110-1301,  USA
 */

/*****************\
 * SQL FUNCTIONS *
\*****************/

#include <dbi/dbi.h>

//
// Tagsistant offers an SQLite backend but plans to implement
// a MySQL backend too. If you want to switch to experimental
// MySQL backend, use:
//
// CFLAGS="-D TAGSISTANT_SQL_BACKEND=TAGSISTANT_MYSQL_BACKEND"
//
#define TAGSISTANT_SQLITE_BACKEND 0
#define TAGSISTANT_MYSQL_BACKEND 1

#ifndef TAGSISTANT_SQL_BACKEND
#	define TAGSISTANT_SQL_BACKEND TAGSISTANT_MYSQL_BACKEND
#endif

#if TAGSISTANT_SQL_BACKEND == TAGSISTANT_SQLITE_BACKEND // <-------------- sqlite backend ------------------------------------------------------------

#define tagsistant_db_connection {}

/* execute SQL query adding file:line coords */
#define  do_sql(dbh, statement, callback, firstarg)\
	real_do_sql(dbh, statement, callback, firstarg, __FILE__, (unsigned int) __LINE__)

/* real function to execute SQL statements */
extern int real_do_sql(sqlite3 **dbh, char *statement, int (*callback)(void *, int, char **, char **), void *firstarg, char *file, unsigned int line);

/* execute SQL statements autoformatting the SQL string and adding file:line coords */
#define tagsistant_query(format, callback, firstarg, ...) _tagsistant_query(format, __FILE__, __LINE__, callback, firstarg, ## __VA_ARGS__)

/* the real code behind the previous macro */
extern int _tagsistant_query(const char *format, gchar *file, int line, int (*callback)(void *, int, char **, char **), void *firstarg, ...);

extern int return_integer(void *return_integer, int argc, char **argv, char **azColName);
extern int return_string(void *return_string, int argc, char **argv, char **azColName);

#elif TAGSISTANT_SQL_BACKEND == TAGSISTANT_MYSQL_BACKEND // <-------------- mysql backend ------------------------------------------------------------

extern void tagsistant_db_connection();

/* execute SQL query adding file:line coords */
#define  do_sql(statement, callback, firstarg)\
	real_do_sql(statement, callback, firstarg, __FILE__, (unsigned int) __LINE__)

/* real function to execute SQL statements */
extern int real_do_sql(char *statement, int (*callback)(void *, dbi_result), void *firstarg, char *file, unsigned int line);

/* execute SQL statements autoformatting the SQL string and adding file:line coords */
#define tagsistant_query(format, callback, firstarg, ...) _tagsistant_query(format, __FILE__, __LINE__, callback, firstarg, ## __VA_ARGS__)

/* the real code behind the previous macro */
extern int _tagsistant_query(const char *format, gchar *file, int line, int (*callback)(void *, dbi_result), void *firstarg, ...);

extern int return_string(void *return_string, dbi_result result);
extern int return_integer(void *return_integer, dbi_result result);

#endif

extern int get_exact_tag_id(const gchar *tagname);

extern gboolean sql_tag_exists(const gchar* tagname);

/***************\
 * SQL QUERIES *
\***************/

#define tagsistant_init_database() {\
	tagsistant_query("create table tags (tag_id integer primary key autoincrement not null, tagname varchar(65) unique not null);", NULL, NULL);\
	tagsistant_query("create table objects (object_id integer not null primary key autoincrement, objectname text(255) not null, path text(1024) unique not null);", NULL, NULL);\
	tagsistant_query("create table tagging (object_id integer not null, tag_id not null, constraint Tagging_key unique (object_id, tag_id));", NULL, NULL);\
	tagsistant_query("create table relations(relation_id integer primary key autoincrement not null, tag1_id integer not null, relation varchar not null, tag2_id integer not null);", NULL, NULL);\
	tagsistant_query("create index tags_index on tagging (object_id, tag_id);", NULL, NULL);\
	tagsistant_query("create index relations_index on relations (tag1_id, tag2_id);", NULL, NULL);\
	tagsistant_query("create index relations_type_index on relations (relation);", NULL, NULL);\
}

extern void sql_create_tag(const gchar *tagname);
extern tagsistant_id sql_get_tag_id(const gchar *tagname);
extern void sql_delete_tag(const gchar *tagname);
extern void sql_tag_object(const gchar *tagname, tagsistant_id object_id);
extern void sql_untag_object(const gchar *tagname, tagsistant_id object_id);
extern void sql_rename_tag(const gchar *tagname, const gchar *oldtagname);

#define ALL_FILES_TAGGED		"select objectname from objects join tagging on tagging.object_id = objects.id where tagging.tagname = \"%s\""
