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
 * Load an object from the database
 */
tagsistant_object_t *tagsistant_object_load(tagsistant_id ID) {
	tagsistant_object_t *obj = g_new0(tagsistant_object_t, 1);

	obj->ID = ID;
	tagsistant_query("select basename from objects where id = %d", return_string, &(obj->basename), ID);
	tagsistant_query("select path from objects where id = %d", return_string, &(obj->path), ID);

	// TODO add linked list of tags

	return obj;
}

/**
 * Create a new object from the frontend.
 * \param @path holds the original path, if available
 * \param @basename holds the name the object will have inside the db
 * \param @mode stores object permissions and type
 */
tagsistant_object_t *tagsistant_object_create(const gchar *path, const gchar *basename, mode_t mode) {
	/* Figure out if file is local or remote */
	/* If local, put path to NULL */
	/* may be using a regular expression??? */

	/* Create the path in the database */
	tagsistant_id ID = sql_create_file(path, basename);

	/* Load the object */
	tagsistant_object_t *obj = tagsistant_object_load(ID);

	if (!obj) {
		// log something!
		return NULL;
	}

	/* check if file is available into local archive */
	struct stat st;
	g_lstat(obj->path, &st);

	/* set file mode */
	g_chmod(obj->path, mode);

	return obj;
}

void tagsistant_object_free(tagsistant_object_t *obj) {
	g_free(obj->basename);
	g_free(obj->path);
	
	// TODO add linked list of tags
	
	g_free(obj);
}

// vim:ts=4:autoindent:nocindent:syntax=c
