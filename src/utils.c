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
#include <time.h>

GRegex *tagsistant_ID_strip_from_querytree_regex = NULL;
GRegex *tagsistant_ID_extract_from_querytree_regex = NULL;

/**
 * initialize all the utilities
 */
void tagsistant_utils_init()
{
	gchar *regex_pattern;
	
	regex_pattern = g_strdup_printf("^[0-9]+%s", TAGSISTANT_ID_DELIMITER);
	tagsistant_ID_strip_from_querytree_regex = g_regex_new(regex_pattern, 0, 0, NULL);
	g_free(regex_pattern);

	regex_pattern = g_strdup_printf("%s.*", TAGSISTANT_ID_DELIMITER);
	tagsistant_ID_extract_from_querytree_regex = g_regex_new(regex_pattern, 0, 0, NULL);
	g_free(regex_pattern);
}

#ifdef DEBUG_TO_LOGFILE
void open_debug_file()
{
	char debug_file[1024];
	sprintf(debug_file, "/tmp/tagsistant.debug.%d", getpid());
	tagsistant.debugfd = fopen(debug_file, "w");
	if (tagsistant.debugfd != NULL) {
		dbg(LOG_INFO, "Logfile %s open!", debug_file);
	} else {
		dbg(LOG_ERR, "Can't open logfile %s: %s!", debug_file, strerror(errno));
	}
}
#endif

#ifdef _DEBUG_SYSLOG
/**
 * initialize syslog stream
 */
void init_syslog()
{
	static enabled = 0;
	if (!enabled) {
		openlog("tagsistant", LOG_PID, LOG_DAEMON);
		enabled = 1;
	}
}
#endif

#ifdef MACOSX
ssize_t getline(char **lineptr, size_t *n, FILE *stream)
{
	if (*lineptr == NULL)
		*lineptr = g_malloc0(sizeof(char) * (*n + 1));

	if (*lineptr == NULL)
		return(0);

	if (fgets(*lineptr, *n, stream) == NULL)
		*n = 0;
	else
		*n = strlen(*lineptr);

	return(*n);
}
#endif


/**
 * since paths are issued without the tagsistant_id trailing
 * the filename (as in path/path/filename), while after being
 * created inside tagsistant, filenames get the additional ID
 * (as in path/path/3273___filename), an hash table to point
 * original paths to actual paths is required.
 *
 * the aliases hash table gets instantiated inside main() and
 * must be used with tagsistant_get_alias(), tagsistant_set_alias() and tagsistant_delete_alias().
 *
 * @param alias the alias to be set
 * @param aliased the path aliased by alias
 */
void tagsistant_set_alias(const char *alias, const char *aliased) {
	dbg(LOG_INFO, "Setting alias %s for %s", aliased, alias);
	tagsistant_query("insert into aliases (alias, aliased) values (\"%s\", \"%s\")", NULL, NULL, alias, aliased);
}

/**
 * look for a matching alias and return(aliased object)
 *
 * @param alias the alias to fetch and translate into aliased object
 */
gchar *tagsistant_get_alias(const char *alias) {
	gchar *aliased = NULL;
	tagsistant_query("select aliased from aliases where alias = \"%s\"", tagsistant_return_string, &aliased, alias);
	dbg(LOG_INFO, "Looking for an alias for %s, found %s", alias, aliased);
	return(aliased);
}

/**
 * remove tagsistant id from a path
 *
 * @param path the path to be purged of the ID
 * @return the purged path
 */
gchar *tagsistant_ID_strip_from_path(const char *path)
{
	gchar *stripped = NULL;

	// split incoming path
	gchar **elements = g_strsplit_set(path, "/", 256);

	// get the last element
	int last_index = g_strv_length(elements) - 1;

	// split the last element
	gchar **last = g_strsplit(elements[last_index], TAGSISTANT_ID_DELIMITER, 2);

	elements[last_index] = NULL; // TODO: memory leaking here?
	gchar *directories = g_strjoinv("/", elements);
	g_strfreev(elements);

	// if the last element returned an ID and a filename...
	if ((last[0] != NULL) && (last[1] != NULL)) {
		if (strlen(directories))
			stripped = g_strjoin("/", directories, last[1], NULL);
		else
			stripped = g_strdup(last[1]);
	}
	// else return(the original path)
	else {
		stripped = g_strdup(path);
	}

	g_free(directories);
	dbg(LOG_INFO, "%s stripped to %s", path, stripped);
	return(stripped);
}

/**
 * return(the tagsistant ID contained into a path)
 *
 * @param path the path supposed to contain an ID
 * @return the ID, if found
 */
tagsistant_id tagsistant_ID_extract_from_path(const char *path)
{
	tagsistant_id ID = 0;

	// split incoming path
	gchar **elements = g_strsplit_set(path, "/", 256);

	// get the last element
	int last_index = g_strv_length(elements) - 1;

	// split the last element
	gchar **last = g_strsplit(elements[last_index], TAGSISTANT_ID_DELIMITER, 2);
	g_strfreev(elements);

	// if the last element returned an ID and a filename...
	if (last[0] != NULL)
		ID = strtol(last[0], NULL, 10);

	dbg(LOG_INFO, "%s has ID %lu", path, (long unsigned int) ID);

	return(ID);
}

/**
 * strip the id part of an object name, starting from qtree->object_path field.
 * if you provide a qtree with object_path == "321___document.txt", this function
 * will return("document.txt".)
 *
 * @param qtree the tagsistant_querytree_t
 * @return the purged qtree->object_path
 */
gchar *tagsistant_ID_strip_from_querytree(tagsistant_querytree_t *qtree)
{
	GStaticMutex mtx = G_STATIC_MUTEX_INIT;
	g_static_mutex_lock(&mtx);
	gchar *stripped = g_regex_replace_literal(tagsistant_ID_strip_from_querytree_regex, qtree->object_path, -1, 0, "", 0, NULL);
	g_static_mutex_unlock(&mtx);

	dbg(LOG_INFO, "%s stripped to %s", stripped, qtree->object_path);

	return(stripped);
}

/**
 * extract the ID from a querytree object
 *
 * @param qtree the tagsistant_querytree_t holding the ID
 * @return the ID, if found
 */
tagsistant_id tagsistant_ID_extract_from_querytree(tagsistant_querytree_t *qtree)
{
	GStaticMutex mtx = G_STATIC_MUTEX_INIT;
	g_static_mutex_lock(&mtx);
	gchar *stripped = g_regex_replace_literal(tagsistant_ID_extract_from_querytree_regex, qtree->object_path, -1, 0, "", 0, NULL);
	g_static_mutex_unlock(&mtx);

	dbg(LOG_INFO, "%s extracted from %s", stripped, qtree->object_path);

	tagsistant_id ID = strtol(stripped, NULL, 10);

	g_free(stripped);
	return(ID);
}

/**
 * Print configuration lines on STDERR
 */
void tagsistant_show_config()
{
	int c;

	// repo internal data
	fprintf(stderr, "\n[Repository]\n");
	c = 1;
	fprintf(stderr, "repository: %s\n", tagsistant.repository);
	fprintf(stderr, "archive: %s\n", tagsistant.archive);
	fprintf(stderr, "mount_point: %s\n", tagsistant.mountpoint);

	// SQL backend
	fprintf(stderr, "\n[SQL]\n");
	fprintf(stderr, "db_options: %s\n", tagsistant.dboptions);
	dbi_driver driver = NULL;
	c = 1;
	while ((driver = dbi_driver_list(driver))) {
		fprintf(stderr, "driver_%02d: %s, %s\n", c++, dbi_driver_get_name(driver), dbi_driver_get_filename(driver));
	}

	// plugin infrastructure
	fprintf(stderr, "\n[Plugins]\n");
	tagsistant_plugin_t *pp = tagsistant.plugins;
	c = 1;
	while (pp != NULL) {
		fprintf(stderr, "%s: %s\n", pp->mime_type, pp->filename);
		pp = pp->next;
	}

	exit(0);
}
