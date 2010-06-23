/*
   Tagsistant (tagfs) -- objects.c
   Copyright (C) 2006-2007 Tx0 <tx0@strumentiresistenti.org>

   Manage objects inside SQL database

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

#define REGISTER_CLEANUP 0
#include "tagsistant.h"

#ifndef MACOSX
#include <mcheck.h>
#endif

/**
 * Return the ID of an object from its filename name
 * If second parameter purename is not null, the name of
 * the file without the leading number is copied
 */
tagsistant_id tagsistant_get_object_id(const gchar *filename, gchar **purename)
{
	tagsistant_id id = 0;

	if (purename != NULL)
		sscanf(filename, "%lu.%s", (long unsigned *) &id, *purename);
	else
		sscanf(filename, "%lu.", (long unsigned *) &id);

	return id;
}

/**
 * Append a tag in the object's tags GList
 *
 * \param @list is a GList** pointer masked as a void* pointer (the GList may change during append operations)
 */
int tagsistant_object_append_tag(void *list, int argc, char **argv, char **azColName)
{
	(void) argc;
	(void) azColName;

	if (argv[0]) {
		GList **store = (GList **) list;
		*store = g_list_append(*store, g_strdup(argv[0]));
	}

	return 0;
}

/**
 * Load an object from the database
 */
tagsistant_object_t *tagsistant_object_load(tagsistant_id ID) {
	tagsistant_object_t *obj = g_new0(tagsistant_object_t, 1);

	// save object ID
	obj->ID = ID;

	// load object basename and path
	// TODO do it in ONE single query (write a generic return_strings() function that stops on NULL arg)
	tagsistant_query("select basename from objects where id = %d", return_string, &(obj->basename), ID);
	tagsistant_query("select path from objects where id = %d", return_string, &(obj->path), ID);

	// add linked list of tags
	tagsistant_query("select tagname from tagging where object_id = %lu", tagsistant_object_append_tag, &(obj->tags), ID);

	return obj;
}

int tagsistant_object_create_on_disk(const gchar *path, mode_t mode) {
	// write a switch statement to create each kind of object	
	(void) path;
	(void) mode;
	return 0;
}

/**
 * Create a new object from the frontend.
 * \param @path holds the original path, if available
 * \param @basename holds the name the object will have inside the db
 * \param @mode stores object permissions and type
 */
tagsistant_object_t *tagsistant_object_create(const gchar *path, const gchar *basename, mode_t mode) {
	/* Figure out if file is local or remote */
	int file_is_local = 0;
	static gchar *pattern = NULL;

	if (path == NULL) {
		file_is_local = 1;

	} else {
		if (!pattern)
			pattern = g_strdup_printf("^%s", tagsistant.repository);

		if (g_regex_match_simple(pattern, path, 0, 0))
			file_is_local = 1;
	}

	/* Create the path in the database */
	tagsistant_id ID = sql_create_object(basename, path);

	/* Load the object */
	tagsistant_object_t *obj = tagsistant_object_load(ID);

	if (!obj) {
		// log something!
		return NULL;
	}

	/* file is local, should be created */
	if (file_is_local) {
		tagsistant_object_create_on_disk(obj->path, mode);
	}

	/* check if file is available into local archive */
	struct stat st;
	g_lstat(obj->path, &st);

	/* set file mode */
	g_chmod(obj->path, mode);

	return obj;
}

void tagsistant_object_free(tagsistant_object_t *obj) {
	// free object data
	g_free(obj->basename);
	g_free(obj->path);
	
	// free the linked list of tags
	g_list_foreach(obj->tags, (GFunc) g_free, NULL);
	g_list_free(obj->tags);
	
	// free the object structure
	g_free(obj);
}

// vim:ts=4:autoindent:nocindent:syntax=c
