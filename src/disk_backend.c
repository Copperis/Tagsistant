/*
   Tagsistant (tagfs) -- disk_backend.c
   Copyright (C) 2006-2007 Tx0 <tx0@strumentiresistenti.org>

   Disk storage backend: provide abstracted storage for files, directories
   and other UNIX disk objects abstracting all the aspects, included if
   objects are imported from existing location, via link() or symlink(),
   or created ex-novo.

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

#ifndef MACOSX
#include <mcheck.h>
#endif

/**
 * Return the path of a file inside Tagsistant repository
 *
 * @param path the file pathname
 * @return char * the file path name inside Tagsistant repository
 */
gchar *tagsistant_localpath(const gchar *path)
{
	gchar *basename = g_path_get_basename(path);
	gchar *filename = g_build_filename(tagsistant.archive, basename, NULL);

	g_free(basename);
	return filename;
}

/**
 * Create a new file in the local storage, add it inside
 * database table, recover the ID and return it.
 */
tagsistant_id create_file(const char *filename, mode_t mode) {
	tagsistant_id object_id = sql_create_object(filename, NULL);

	if (!object_id) {
		return 0;
	}

	gchar *filepath = sql_objectpath(object_id);

	int fd = creat(filepath, mode);
	if (fd == -1) {
		sql_delete_object(object_id);
		object_id = 0;
	} else {
		close(fd);
	}

	g_free(filepath);
	return object_id;
}

tagsistant_id create_dir(const char *dirname, mode_t mode)
{
	tagsistant_id object_id = sql_create_object(dirname, NULL);

	if (!object_id) {
		return 0;
	}

	gchar *dirpath = sql_objectpath(object_id);

	int result = mkdir(dirpath, mode);
	if (result == -1) {
		sql_delete_object(object_id);
		object_id = 0;
	}

	g_free(dirpath);
	return object_id;
}

// vim:ts=4:autoindent:nocindent:syntax=c
