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
extern int return_string(void *return_string, int argc, char **argv, char **azColName);
extern int get_exact_tag_id(const gchar *tagname);

extern int report_if_exists(void *exists_buffer, int argc, char **argv, char **azColName);
extern gboolean sql_tag_exists(const gchar* tagname);

extern tagsistant_id sql_create_file(const gchar *path, const gchar *basename);

/***************\
 * SQL QUERIES *
\***************/
#define tagsistant_init_database() {\
	tagsistant_query("create table tags (id integer primary key autoincrement not null, tagname varchar(64) unique not null);", NULL, NULL);\
	tagsistant_query("create table objects (id integer not null primary key autoincrement, filename text(255) not null, path text(1024) unique not null);", NULL, NULL);\
	tagsistant_query("create table tagging (file_id integer not null primary key autoincrement, tagname text(64) not null, constraint Tagging_key unique (file_id, tagname));", NULL, NULL);\
	tagsistant_query("create table relations(id integer primary key autoincrement not null, tag1 varchar(64) not null, relation varchar not null, tag2 varchar(64) not null);", NULL, NULL);\
	tagsistant_query("create index tags_index on tagging (tagname);", NULL, NULL);\
	tagsistant_query("create index tagging_index on tagging (tagname, file_id);", NULL, NULL);\
	tagsistant_query("create index relations_index on relations (tag1, tag2);", NULL, NULL);\
	tagsistant_query("create index relations_type_index on relations (relation);", NULL, NULL);\
}

#define sql_create_tag(tagname) tagsistant_query("insert into tags(tagname) values(\"%s\");", NULL, NULL, tagname)

#define sql_delete_tag(tagname) {\
	tagsistant_query("delete from tags where tagname = \"%s\";", NULL, NULL, tagname);\
	tagsistant_query("delete from tagging where tagname = \"%s\";", NULL, NULL, tagname);\
	tagsistant_query("delete from relations where tag1 = \"%s\" or tag2 = \"%s\";", NULL, NULL, tagname, tagname);\
}

#define sql_tag_file(tagname, file_id) {\
	tagsistant_query("insert into tagging(tagname, file_id) values(\"%s\", \"%d\");", NULL, NULL, tagname, file_id);\
	tagsistant_query("insert into tags(tagname) values(\"%s\");", NULL, NULL, tagname);\
}

#define sql_untag_file(tagname, file_id)\
	tagsistant_query("delete from tagging where tagname = \"%s\" and file_id = \"%d\";", NULL, NULL, tagname, file_id)

#define sql_rename_tag(tagname, oldtagname) {\
	tagsistant_query("update tagging set tagname = \"%s\" where tagname = \"%s\";", NULL, NULL, tagname, oldtagname);\
	tagsistant_query("update tags set tagname = \"%s\" where tagname = \"%s\";", NULL, NULL, tagname, oldtagname);\
}

#define sql_rename_file(oldname, newname) tagsistant_query("update objects set filename = \"%s\" where filename = \"%s\";", NULL, NULL, newname, oldname);

#define ALL_FILES_TAGGED		"select filename from objects join tagging on tagging.file_id = objects.id where tagging.tagname = \"%s\""

#define tag_file(file_id, tagname) sql_tag_file(tagname, file_id)
#define untag_file(file_id, tagname) sql_untag_file(tagname, file_id)
