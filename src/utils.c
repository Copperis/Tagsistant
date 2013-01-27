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

GRegex *tagsistant_inode_strip_from_querytree_regex = NULL;
GRegex *tagsistant_inode_extract_from_querytree_regex = NULL;

/**
 * initialize all the utilities
 */
void tagsistant_utils_init()
{
	gchar *regex_pattern;
	
	regex_pattern = g_strdup_printf("^[0-9]+%s", TAGSISTANT_INODE_DELIMITER);
	tagsistant_inode_strip_from_querytree_regex = g_regex_new(regex_pattern, 0, 0, NULL);
	g_free(regex_pattern);

	regex_pattern = g_strdup_printf("%s.*", TAGSISTANT_INODE_DELIMITER);
	tagsistant_inode_extract_from_querytree_regex = g_regex_new(regex_pattern, 0, 0, NULL);
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
 * remove tagsistant id from a path
 *
 * @param path the path to be purged of the inode
 * @return the purged path
 */
gchar *tagsistant_inode_strip_from_path(const char *path)
{
	gchar *stripped = NULL;

	// split incoming path
	gchar **elements = g_strsplit_set(path, "/", 256);

	// get the last element
	int last_index = g_strv_length(elements) - 1;

	// split the last element
	gchar **last = g_strsplit(elements[last_index], TAGSISTANT_INODE_DELIMITER, 2);

	elements[last_index] = NULL; // TODO: memory leaking here?
	gchar *directories = g_strjoinv("/", elements);
	g_strfreev(elements);

	// if the last element returned an inode and a filename...
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
 * return(the tagsistant inode contained into a path)
 *
 * @param path the path supposed to contain an inode
 * @return the inode, if found
 */
tagsistant_inode tagsistant_inode_extract_from_path(const char *path)
{
	tagsistant_inode inode = 0;

	// split incoming path
	gchar **elements = g_strsplit_set(path, "/", 256);

	// get the last element
	int last_index = g_strv_length(elements) - 1;

	// split the last element
	gchar **last = g_strsplit(elements[last_index], TAGSISTANT_INODE_DELIMITER, 2);
	g_strfreev(elements);

	// if the last element returned an inode and a filename...
	if (last[0] != NULL)
		inode = strtol(last[0], NULL, 10);

	dbg(LOG_INFO, "%s has inode %lu", path, (long unsigned int) inode);

	return(inode);
}

/**
 * strip the id part of an object name, starting from qtree->object_path field.
 * if you provide a qtree with object_path == "321___document.txt", this function
 * will return("document.txt".)
 *
 * @param qtree the tagsistant_querytree_t
 * @return the purged qtree->object_path
 */
gchar *tagsistant_inode_strip_from_querytree(tagsistant_querytree_t *qtree)
{
//	GStaticMutex mtx = G_STATIC_MUTEX_INIT;
//	g_static_mutex_lock(&mtx);
	gchar *stripped = g_regex_replace_literal(tagsistant_inode_strip_from_querytree_regex, qtree->object_path, -1, 0, "", 0, NULL);
//	g_static_mutex_unlock(&mtx);

	dbg(LOG_INFO, "%s stripped to %s", stripped, qtree->object_path);

	return(stripped);
}

/**
 * extract the inode from a querytree object
 *
 * @param qtree the tagsistant_querytree_t holding the inode
 * @return the inode, if found
 */
tagsistant_inode tagsistant_inode_extract_from_querytree(tagsistant_querytree_t *qtree)
{
//	GStaticMutex mtx = G_STATIC_MUTEX_INIT;
//	g_static_mutex_lock(&mtx);
	gchar *stripped = g_regex_replace_literal(tagsistant_inode_extract_from_querytree_regex, qtree->object_path, -1, 0, "", 0, NULL);
//	g_static_mutex_unlock(&mtx);

	dbg(LOG_INFO, "%s extracted from %s", stripped, qtree->object_path);

	tagsistant_inode inode = strtol(stripped, NULL, 10);

	g_free(stripped);
	return(inode);
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

/**
 * Create an object and tag it
 *
 * @param qtree the querytree asking object creation
 * @param tagsistant_errno error_reporting variable
 * @param force_create boolean: if true, creation is forced
 */
int tagsistant_inner_create_and_tag_object(tagsistant_querytree_t *qtree, int *tagsistant_errno, int force_create)
{
	tagsistant_inode inode = 0;

	// 1. create the object on db or get its inode if exists
	//    if force_create is true, create a new object and fetch its inode
	//    if force_create is false, try to find an object with name and path matching
	//    and use its inode, otherwise create a new one
	if (!force_create) {
		tagsistant_query(
			"select object_id from objects where objectname = \"%s\" and path = \"%s\" limit 1",
			tagsistant_return_integer, &inode,
			qtree->object_path, qtree->archive_path);
	}

	if (force_create || (0 == inode)) {
		tagsistant_query(
			"insert into objects (objectname, path) values (\"%s\", \"-\")",
			NULL, NULL,
			qtree->object_path);

		/*
		tagsistant_query(
			"select max(object_id) from objects where objectname = \"%s\" and path = \"-\"",
			tagsistant_return_integer, &inode,
			qtree->object_path);
		*/

		// don't know why it does not work on MySQL
		inode = tagsistant_last_insert_id();
	}

	if (0 == inode) {
		dbg(LOG_ERR, "Object %s recorded as inode 0!", qtree->object_path);
		*tagsistant_errno = EIO;
		return(-1);
	}

	// 2. adjust archive_path and full_archive_path with leading object_id
	g_free(qtree->archive_path);
	g_free(qtree->full_archive_path);

	qtree->inode = inode;
	qtree->archive_path = g_strdup_printf("%d%s%s", inode, TAGSISTANT_INODE_DELIMITER, qtree->object_path);
	qtree->full_archive_path = g_strdup_printf("%s%d%s%s", tagsistant.archive, inode, TAGSISTANT_INODE_DELIMITER, qtree->object_path);

	// 2.bis adjust object_path inside DB
	tagsistant_query(
		"update objects set path = \"%s\" where object_id = %d",
		NULL, NULL,
		qtree->archive_path, inode);

	// 4. tag the object
	tagsistant_traverse_querytree(qtree, tagsistant_sql_tag_object, inode);

	// 5. use autotagging plugin stack
	tagsistant_process(qtree);

	return(inode);
}
