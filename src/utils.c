/*
   Tagsistant (tagfs) -- utils.c
   Copyright (C) 2006-2009 Tx0 <tx0@strumentiresistenti.org>

   Tagsistant (tagfs) mount binary written using FUSE userspace library.

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

FILE *debugfd = NULL;

#ifdef DEBUG_TO_LOGFILE
void open_debug_file()
{
	char debug_file[1024];
	sprintf(debug_file, "/tmp/tagsistant.debug.%d", getpid());
	debugfd = fopen(debug_file, "w");
	if (debugfd != NULL) {
		dbg(LOG_INFO, "Logfile %s open!", debug_file);
	} else {
		dbg(LOG_ERR, "Can't open logfile %s: %s!", debug_file, strerror(errno));
	}
}
#endif

#ifdef DEBUG_STRDUP
char *real_strdup(const char *orig, char *file, int line)
{
	if (orig == NULL) return NULL;
	/* dbg(LOG_INFO, "strdup(%s) @%s:%d", orig, file, line); */
	char *res = g_malloc0(sizeof(char) * (strlen(orig) + 1));
	memcpy(res, orig, strlen(orig));
	if (debugfd != NULL) fprintf(debugfd, "0x%.8x: strdup(%s) @%s:%d\n", (unsigned int) res, orig, file, line);
	return res;
}
#endif

int debug = 0;
int log_enabled = 0;

/**
 * Return the real path on the underlying filesystem to access a file.
 * The file can be provided as its basename with the "filename" parameter,
 * or through its unique ID using the "file_id" parameter. If the file is
 * not located inside the DB, and a "filename" has been provided, a path
 * inside the archive/ directory is returned. The "filename" and "file_id"
 * parameters can be specified exclusively.
 *
 * EXAMPLE 1: _get_file_path(NULL, 12) will return the path of file with
 * ID 12, if existing, NULL otherwise.
 *
 * EXAMPLE 2: _get_file_path("passwd", 0) will return the path of the only
 * "passwd" file in the files table. If more than one file exist, NULL
 * is returned. If no file is found, the path "<repository>/archive/passwd"
 * is returned, where <repository> is the path of Tagsistant repository.
 *
 * \param @filename a string with the basename of the file
 * \param @file_id an ID referring to a file
 * \param @use_first_match if no file_id is provided and more than one filename
 *   matches "filename", use the first instead of returning NULL
 * \returns a string, if appropriate, NULL otherwise. Must be freed!
 */
gchar *_get_file_path(const gchar *filename, int file_id, int use_first_match)
{
	gchar *path = NULL;

	if (file_id) {
		tagsistant_query("select path from objects where id = %u", return_string, &path, file_id);
		if ((path == NULL) && (filename != NULL)) {
			path = g_strdup_printf("%s%s", tagsistant.archive, filename);
		}
	} else if (filename != NULL) {
		int count = 0;
		tagsistant_query("select count(filename) from objects where basename = \"%s\"", return_integer, &count, filename);

		if (count == 0) {
			path = g_strdup_printf("%s%s", tagsistant.archive, filename);
		} else if ((count == 1) || use_first_match) {
			tagsistant_query("select path from objects where basename = \"%s\"", return_string, &path, filename);
		} else {
			// if more than 1 entry match the file name, the corresponding path can't be guessed
		}
	}

	dbg(LOG_INFO, "get_file_path(\"%s\", %u) == \"%s\"", filename, file_id, path);
	return path;
}

/**
 * given a full path, returns the filename, the filepath (relative
 * to archive directory) and the tagname
 *
 * \param path original path to analize
 * \param filename pointer to path filename
 * \param filepath pointer to path other part which is not filename
 * \param tagname pointer to path last tag
 * \return 1 if successfull, 0 otherwise
 * \todo return value is not conditional, this function always returns 1!
 */
int get_filename_and_tagname(const char *path, char **filename, char **filepath, char **tagname)
{
	*filename = g_path_get_basename(path);
	*filepath = get_file_path(*filename, 0);
	char *path_dup = g_strdup(path);
	char *ri = rindex(path_dup, '/');
	*ri = '\0';
	ri = rindex(path_dup, '/');
	if (ri) {
		ri++;
		*tagname = g_strdup(ri);
	}
	freenull(path_dup);
	return 1;
}

#ifdef _DEBUG_SYSLOG
/**
 * initialize syslog stream
 */
void init_syslog()
{
	if (log_enabled)
		return;

	openlog("tagsistant", LOG_PID, LOG_DAEMON);
	log_enabled = 1;
}
#endif

/**
 * Check if a file with id "file_id" is associated with a given tag.
 *
 * \param file_id the file_id to be checked
 * \param tagname the name of the tag to be searched on filename
 * \return 1 if file filename is tagged with tag tagname, 0 otherwise.
 */
gboolean is_tagged(int file_id, char *tagname)
{
	int exists = 0;
	tagsistant_query(
		"select count(filename) from tagging where file_id = \"%d\" and tagname = \"%s\";",
		return_integer, &exists, file_id, tagname);
	return exists ? TRUE : FALSE;
}

/**
 * Check if a file named "filename" is associated with a given tag.
 *
 * \param filename the filename to be checked (no path)
 * \param tagname the name of the tag to be searched on filename
 * \return 1 if file filename is tagged with tag tagname, 0 otherwise.
 */
gboolean filename_is_tagged(const char *filename, const char *tagname)
{
	int is_tagged = 0;
	tagsistant_query(
		"select count(tagname) from tagging join objects on objects.id = tagging.file_id where objects.filename = \"%s\" and tagging.tagname = \"%s\";",
		return_integer, &is_tagged, filename, tagname);
	return is_tagged ? TRUE : FALSE;
}

#ifdef MACOSX
ssize_t getline(char **lineptr, size_t *n, FILE *stream)
{
	if (*lineptr == NULL)
		*lineptr = g_malloc0(sizeof(char) * (*n + 1));

	if (*lineptr == NULL)
		return 0;

	if (fgets(*lineptr, *n, stream) == NULL)
		*n = 0;
	else
		*n = strlen(*lineptr);

	return *n;
}
#endif

