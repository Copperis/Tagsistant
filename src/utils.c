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

FILE *tagsistant_debugfd = NULL;

#ifdef DEBUG_TO_LOGFILE
void open_debug_file()
{
	char debug_file[1024];
	sprintf(debug_file, "/tmp/tagsistant.debug.%d", getpid());
	tagsistant_debugfd = fopen(debug_file, "w");
	if (tagsistant_debugfd != NULL) {
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
	if (tagsistant_debugfd != NULL) fprintf(tagsistant_debugfd, "0x%.8x: strdup(%s) @%s:%d\n", (unsigned int) res, orig, file, line);
	return res;
}
#endif

int tagsistant_debug = 0;
int tagsistant_log_enabled = 0;

#ifdef _DEBUG_SYSLOG
/**
 * initialize syslog stream
 */
void init_syslog()
{
	if (tagsistant_log_enabled)
		return;

	openlog("tagsistant", LOG_PID, LOG_DAEMON);
	tagsistant_log_enabled = 1;
}
#endif

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

gchar *tagsistant_querytree_types[QTYPE_TOTAL];
int tagsistant_querytree_types_initialized = 0;

/**
 * return querytree type as a printable string.
 * the string MUST NOT be freed
 */
gchar *tagsistant_query_type(tagsistant_querytree_t *qtree)
{
	if (!tagsistant_querytree_types_initialized) {
		tagsistant_querytree_types[QTYPE_MALFORMED] = g_strdup("QTYPE_MALFORMED");
		tagsistant_querytree_types[QTYPE_ROOT] = g_strdup("QTYPE_ROOT");
		tagsistant_querytree_types[QTYPE_ARCHIVE] = g_strdup("QTYPE_ARCHIVE");
		tagsistant_querytree_types[QTYPE_TAGS] = g_strdup("QTYPE_TAGS");
		tagsistant_querytree_types[QTYPE_RELATIONS] = g_strdup("QTYPE_RELATIONS");
		tagsistant_querytree_types[QTYPE_STATS] = g_strdup("QTYPE_STATS");
		tagsistant_querytree_types_initialized++;
	}

	return(tagsistant_querytree_types[qtree->type]);
}

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
 * look for a matching alias and return aliased object
 *
 * @param alias the alias to fetch and translate into aliased object
 */
gchar *tagsistant_get_alias(const char *alias) {
	gchar *aliased = NULL;
	tagsistant_query("select aliased from aliases where alias = \"%s\"", tagsistant_return_string, &aliased, alias);
	dbg(LOG_INFO, "Looking for an alias for %s, found %s", alias, aliased);
	return(aliased);
}

#if 0
/**
 * delete an existing alias
 *
 * @param alias the alias to be deleted
 */
void tagsistant_delete_alias(const char *alias) {
	dbg(LOG_INFO, "Deleting alias for %s", alias);
	tagsistant_query("delete from aliases where alias = \"%s\"", NULL, NULL, alias);
}
#endif

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
	// else return the original path
	else {
		stripped = g_strdup(path);
	}

	g_free(directories);
	dbg(LOG_INFO, "%s stripped to %s", path, stripped);
	return(stripped);
}

/**
 * return the tagsistant ID contained into a path
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
 * will return "document.txt".
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

#if 0
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
	return ID;
}
#endif

/**
 * Service routine that rebuilds tagsistant_querytree_t paths after
 * some internal field has changed
 *
 * @param qtree the tagsistant_querytree_t to be rebuilt
 */
void tagsistant_querytree_rebuild_paths(tagsistant_querytree_t *qtree)
{
	if (!qtree->object_id) qtree->object_id = tagsistant_ID_extract_from_path(qtree->full_path);

	// free the paths
	if (qtree->archive_path) g_free(qtree->archive_path);
	if (qtree->full_archive_path) g_free(qtree->full_archive_path);

	// prepare new paths
	qtree->archive_path = g_strdup_printf("%d%s%s", qtree->object_id, TAGSISTANT_ID_DELIMITER, qtree->object_path);
	qtree->full_archive_path = g_strdup_printf("%s%s", tagsistant.archive, qtree->archive_path);
}

/**
 * renumber an object, by changing its object_id and rebuilding its
 * object_path, archive_path and full_archive_path.
 */
void tagsistant_querytree_renumber(tagsistant_querytree_t *qtree, tagsistant_id object_id)
{
	if (qtree && object_id) {
		// save the object id
		qtree->object_id = object_id;

		// strip the object id
		gchar *stripped = tagsistant_ID_strip_from_querytree(qtree);
		g_free(qtree->object_path);
		qtree->object_path = stripped;

		// build the new object name
		tagsistant_querytree_rebuild_paths(qtree);
	}
}

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
	tagsistant_plugin_t *pp = plugins;
	c = 1;
	while (pp != NULL) {
		fprintf(stderr, "%s: %s\n", pp->mime_type, pp->filename);
		pp = pp->next;
	}

	exit(0);
}

void tagsistant_plugin_apply_regex(const tagsistant_querytree_t *qtree, const char *buf, GMutex *m, GRegex *rx)
{
	GMatchInfo *match_info;

	/* apply the regex, locking the mutex if provided */
	if (NULL != m) g_mutex_lock(m);
	g_regex_match(rx, buf, 0, &match_info);
	if (NULL != m) g_mutex_unlock(m);

	/* process the matched entries */
	while (g_match_info_matches(match_info)) {
		gchar *raw = g_match_info_fetch(match_info, 1);
		dbg(LOG_INFO, "Found raw data: %s", raw);

		gchar **tokens = g_strsplit_set(raw, " \t,.!?", 255);
		g_free(raw);

		int x = 0;
		while (tokens[x]) {
			if (strlen(tokens[x]) >= 3) tagsistant_sql_tag_object(tokens[x], qtree->object_id);
			x++;
		}

		g_strfreev(tokens);

		g_match_info_next(match_info, NULL);
	}
}
