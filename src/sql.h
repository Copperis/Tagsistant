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
extern int get_exact_tag_id(const gchar *tagname);

extern int report_if_exists(void *exists_buffer, int argc, char **argv, char **azColName);
extern gboolean sql_tag_exists(const gchar* tagname);

/***************\
 * SQL QUERIES *
\***************/
#define tagsistant_init_database() {\
	tagsistant_query("create table tags(id integer primary key autoincrement not null, tagname varchar unique not null);", NULL, NULL);\
	tagsistant_query("create table tagged(id integer primary key autoincrement not null, tagname varchar not null, filename varchar not null);", NULL, NULL);\
	tagsistant_query("create table relations(id integer primary key autoincrement not null, tag1 varchar not null, relation varchar not null, tag2 varchar not null);", NULL, NULL);\
	tagsistant_query("create index tags_index on tags (tagname);", NULL, NULL);\
	tagsistant_query("create index tagged_index on tagged (tagname, filename);", NULL, NULL);\
	tagsistant_query("create index relations_index on relations (tag1, tag2);", NULL, NULL);\
	tagsistant_query("create index relations_type_index on relations (relation);", NULL, NULL);\
}

#define sql_create_tag(tagname) tagsistant_query("insert into tags(tagname) values(\"%s\");", NULL, NULL, tagname)

#define sql_delete_tag(tagname) {\
	tagsistant_query("delete from tags where tagname = \"%s\"", NULL, NULL, tagname);\
	tagsistant_query("delete from tagged where tagname = \"%s\";", NULL, NULL, tagname);\
	tagsistant_query("delete from relations where tag1 = \"%s\" or tag2 = \"%s\";", NULL, NULL, tagname, tagname);\
}

#define sql_tag_file(tagname, filename) tagsistant_query("insert into tagged(tagname, filename) values(\"%s\", \"%s\");", NULL, NULL, tagname, filename)
#define sql_untag_file(tagname, filename) tagsistant_query("delete from tagged where tagname = \"%s\" and filename = \"%s\";", NULL, NULL, tagname, filename)
#define sql_rename_tag(tagname, oldtagname) {\
	tagsistant_query("update tags set tagname = \"%s\" where tagname = \"%s\";", NULL, NULL, tagname, oldtagname);\
	tagsistant_query("update tagged set tagname = \"%s\" where tagname = \"%s\";", NULL, NULL, tagname, oldtagname);\
}

#define sql_rename_file(oldname, newname) tagsistant_query("update tagged set filename = \"%s\" where filename = \"%s\";", NULL, NULL, newname, oldname);

#define ALL_FILES_TAGGED		"select filename from tagged where tagname = \"%s\""

