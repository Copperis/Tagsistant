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

#define TAGSISTANT_NULL_BACKEND			0
#define TAGSISTANT_DBI_MYSQL_BACKEND	1
#define TAGSISTANT_DBI_SQLITE_BACKEND	2

#define TAGSISTANT_COMMIT_TRANSACTION	1
#define TAGSISTANT_ROLLBACK_TRANSACTION	0

#ifndef TAGSISTANT_SQL_BACKEND
#	define TAGSISTANT_SQL_BACKEND TAGSISTANT_DBI_SQLITE_BACKEND
#endif

extern void tagsistant_db_init();
extern dbi_conn tagsistant_db_connection();
extern void tagsistant_create_schema();

/* execute SQL statements autoformatting the SQL string and adding file:line coords */
#define tagsistant_query(format, conn, callback, firstarg, ...) \
	tagsistant_real_query(conn, format, __FILE__, __LINE__, callback, firstarg, ## __VA_ARGS__)

/* the real code behind the previous macro */
extern int tagsistant_real_query(
		dbi_conn conn,
		const char *format,
		gchar *file,
		int line,
		int (*callback)(void *, dbi_result),
		void *firstarg,
		...);

/** callback to return a string */
extern int tagsistant_return_string(void *return_string, dbi_result result);

/** callback to return an integer */
extern int tagsistant_return_integer(void *return_integer, dbi_result result);

/**
 * transactions are started by default in tagsistant_db_connection()
 * and must be closed calling one of the following macros
 */
#define tagsistant_commit_transaction(dbi_conn) tagsistant_query("commit", dbi_conn, NULL, NULL)
#define tagsistant_rollback_transaction(dbi_conn) tagsistant_query("rollback", dbi_conn, NULL, NULL)

/***************\
 * SQL QUERIES *
\***************/

extern void				tagsistant_sql_create_tag(dbi_conn conn, const gchar *tagname);
extern tagsistant_inode	tagsistant_sql_get_tag_id(dbi_conn conn, const gchar *tagname);
extern void				tagsistant_sql_delete_tag(dbi_conn conn, const gchar *tagname);
extern void				tagsistant_sql_tag_object(dbi_conn conn, const gchar *tagname, tagsistant_inode inode);
extern void				tagsistant_sql_untag_object(dbi_conn conn, const gchar *tagname, tagsistant_inode inode);
extern void				tagsistant_sql_rename_tag(dbi_conn conn, const gchar *tagname, const gchar *oldtagname);
extern tagsistant_inode	tagsistant_last_insert_id(dbi_conn conn);
extern int				tagsistant_object_is_tagged(dbi_conn conn, tagsistant_inode inode);
extern int				tagsistant_object_is_tagged_as(dbi_conn conn, tagsistant_inode inode, tagsistant_inode tag_id);
extern void				tagsistant_full_untag_object(dbi_conn conn, tagsistant_inode inode);
