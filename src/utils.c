/*
   Tagsistant (tagfs) -- utils.c
   Copyright (C) 2006-2013 Tx0 <tx0@strumentiresistenti.org>

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
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

GRegex *tagsistant_inode_extract_from_path_regex = NULL;

#ifdef DEBUG_TO_LOGFILE
void open_debug_file()
{
	char debug_file[1024];
	sprintf(debug_file, "/tmp/tagsistant.debug.%d", getpid());
	tagsistant.debugfd = fopen(debug_file, "w");
	if (tagsistant.debugfd == NULL)
		dbg('l', LOG_ERR, "Can't open logfile %s: %s!", debug_file, strerror(errno));
}
#endif

#ifdef _DEBUG_SYSLOG
/**
 * initialize syslog stream
 */
void tagsistant_init_syslog()
{
	static int enabled = 0;
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
 * return the tagsistant inode contained into a path
 *
 * @param qtree the querytree object supposed to contain an inode
 * @return the inode, if found
 */
tagsistant_inode tagsistant_inode_extract_from_path(tagsistant_querytree *qtree)
{
	if (!qtree || !qtree->object_path || strlen(qtree->object_path) == 0) return (0);

	tagsistant_inode inode = 0;

	GMatchInfo *match_info;
	if (g_regex_match(tagsistant_inode_extract_from_path_regex, qtree->object_path, 0, &match_info)) {
		/*
		 * extract the inode
		 */
		gchar *inode_text = g_match_info_fetch(match_info, 1);
		inode = strtoul(inode_text, NULL, 10);
		g_free_null(inode_text);

		/*
		 * replace the inode and the separator with a blank string,
		 * actually stripping it from the object_path
		 */
		qtree->object_path = g_regex_replace(
			tagsistant_inode_extract_from_path_regex,
			qtree->object_path,
			strlen(qtree->object_path),
			0, "", 0, NULL);
	}
	g_match_info_free(match_info);

	if (inode) {
		dbg('l', LOG_INFO, "%s has inode %lu", qtree->object_path, (long unsigned int) inode);
	} else {
		dbg('l', LOG_INFO, "%s does not contain and inode", qtree->object_path);
	}

	return (inode);
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
}

/**
 * Create an object and tag it
 *
 * @param qtree the querytree asking object creation
 * @param tagsistant_errno error_reporting variable
 * @param force_create boolean: if true, creation is forced
 */
int tagsistant_inner_create_and_tag_object(tagsistant_querytree *qtree, int *tagsistant_errno, int force_create)
{
	tagsistant_inode inode = 0;

	// 1. create the object on db or get its inode if exists
	//    if force_create is true, create a new object and fetch its inode
	//    if force_create is false, try to find an object with name and path matching
	//    and use its inode, otherwise create a new one
	if (!force_create) {
		tagsistant_query(
			"select inode from objects where objectname = \"%s\" limit 1",
			qtree->dbi,
			tagsistant_return_integer,
			&inode,
			qtree->object_path);
	}

	if (force_create || (!inode)) {
		tagsistant_query(
			"insert into objects (objectname) values (\"%s\")",
			qtree->dbi, NULL, NULL, qtree->object_path);

		inode = tagsistant_last_insert_id(qtree->dbi);
	}

	if (!inode) {
		dbg('u', LOG_ERR, "Object %s recorded as inode 0!", qtree->object_path);
		*tagsistant_errno = EIO;
		return(-1);
	}

	// 2. adjust archive_path and full_archive_path with leading inode
	tagsistant_querytree_set_inode(qtree, inode);

	// 3. tag the object
	tagsistant_querytree_traverse(qtree, tagsistant_sql_tag_object, inode);

	if (force_create) {
		dbg('l', LOG_INFO, "Forced creation of object %s", qtree->full_path);
	} else {
		dbg('l', LOG_INFO, "Tried creation of object %s", qtree->full_path);
	}

	return(inode);
}

#if TAGSISTANT_ENABLE_DEDUPLICATION || TAGSISTANT_ENABLE_AUTOTAGGING
extern GThread *tagsistant_dedup_autotag_thread;
extern GAsyncQueue *tagsistant_dedup_autotag_queue;
extern void tagsistant_dedup_and_autotag_thread(gpointer data);
#endif

/**
 * Initialize all the utilities
 */
void tagsistant_utils_init()
{
	/* compile regular expressions */
	tagsistant_inode_extract_from_path_regex = g_regex_new("^([0-9]+)" TAGSISTANT_INODE_DELIMITER, 0, 0, NULL);

#if TAGSISTANT_ENABLE_DEDUPLICATION || TAGSISTANT_ENABLE_AUTOTAGGING
	/* init the asynchronous queue */
	tagsistant_dedup_autotag_queue = g_async_queue_new();

	/* start deduplication thread */
	tagsistant_dedup_autotag_thread = g_thread_new(
		"deduplication",
		(GThreadFunc) tagsistant_dedup_and_autotag_thread,
		NULL);

	/* increase reference count for both objects */
	g_async_queue_ref(tagsistant_dedup_autotag_queue);
	g_thread_ref(tagsistant_dedup_autotag_thread);
#endif
}

/****************************************************************************/
/***                                                                      ***/
/***   Repository .ini file parsing and writing                           ***/
/***                                                                      ***/
/****************************************************************************/

GKeyFile *tagsistant_ini = NULL;

#define tagsistant_get_repository_ini_path() g_strdup_printf("%s/repository.ini", tagsistant.repository)

/**
 * Read the repository.ini file contained into a tagsistant repository
 *
 * @return a GKeyFile object
 */
GKeyFile *tagsistant_parse_repository_ini()
{
	GError *error = NULL;
	gchar *ini_path = tagsistant_get_repository_ini_path();
	GKeyFile *kf = g_key_file_new();

	// load the key file
	g_key_file_load_from_file(kf, ini_path, G_KEY_FILE_NONE, &error);
	g_free_null(ini_path);

	// if no error occurred, return the GKeyFile object
	if (!error) return (kf);

	// otherwise, free the GKeyFile object and return NULL
	g_key_file_free(kf);
	return (NULL);
}

/**
 * Save a repository.ini file
 *
 * @param kf the GKeyFile object to save
 */
void tagsistant_save_repository_ini(GKeyFile *kf)
{
	gchar *ini_path = tagsistant_get_repository_ini_path();
	gsize size = 0;
	gchar *content = g_key_file_to_data(kf, &size, NULL);

	// open the file and write the content
	int fd = open(ini_path, O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR);
	if (-1 != fd) {
		int written = write(fd, content, size);
		if (-1 == written) {
			dbg('l', LOG_ERR, "Error writing %s: %s", ini_path, strerror(errno));
		}
		close(fd);
	} else {
		dbg('l', LOG_ERR, "Unable to write %s: %s", ini_path, strerror(errno));
	}

	g_free_null(content);
	g_free_null(ini_path);
}

#define tagsistant_set_init_default(kf, section, key, value) \
	if (!g_key_file_has_key(kf, section, key, NULL))\
		g_key_file_set_value(kf, section, key, value);

/**
 * Read the repository.ini file, compare its content with
 * provided command line arguments and than saves back the
 * merge of both in repository.ini
 */
void tagsistant_manage_repository_ini() {
	// read the repository.ini file from disk
	tagsistant_ini = tagsistant_parse_repository_ini();

	if (tagsistant_ini) {
		if (g_key_file_has_group(tagsistant_ini, "Tagsistant")) {
			if (g_key_file_has_key(tagsistant_ini, "Tagsistant", "db", NULL)) {
				if (tagsistant.dboptions) {
					dbg('b', LOG_INFO, "Ignoring command line --db parameter in favor of repository.ini");
				}
				tagsistant.dboptions = g_key_file_get_value(tagsistant_ini, "Tagsistant", "db", NULL);
			}
		}
	}

	// if repository.ini has not been laded, create an empty GKeyFile object
	if (!tagsistant_ini) tagsistant_ini = g_key_file_new();

	// fill the GKeyFile object with command line values
	if (!tagsistant.dboptions) tagsistant.dboptions = g_strdup("sqlite3::::");
	if (!strlen(tagsistant.dboptions)) {
		g_free_null(tagsistant.dboptions);
		tagsistant.dboptions = g_strdup("sqlite3::::");
	}

	g_key_file_set_value(tagsistant_ini, "Tagsistant", "db", tagsistant.dboptions);
	g_key_file_set_value(tagsistant_ini, "Tagsistant", "mountpoint", tagsistant.mountpoint);
	g_key_file_set_value(tagsistant_ini, "Tagsistant", "repository", tagsistant.repository);

	// set default plugin filters
	tagsistant_set_init_default(tagsistant_ini, "mime:application/xml",	"filter", "^(author|date|language)$");
	tagsistant_set_init_default(tagsistant_ini, "mime:image/gif",		"filter", "^(size|orientation)$");
	tagsistant_set_init_default(tagsistant_ini, "mime:text/html",		"filter", "^(author|date|language)$");
	tagsistant_set_init_default(tagsistant_ini, "mime:image/jpeg",		"filter", "^(size|orientation)$");
	tagsistant_set_init_default(tagsistant_ini, "mime:image/png",		"filter", "^(size|orientation)$");
	tagsistant_set_init_default(tagsistant_ini, "mime:application/ogg",	"filter", "^(year|album|artist)$");
	tagsistant_set_init_default(tagsistant_ini, "mime:audio/mpeg",		"filter", "^(year|album|artist)$");

	// save and free the GKeyFile object
	tagsistant_save_repository_ini(tagsistant_ini);
}
