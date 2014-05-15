/*
   Tagsistant (tagfs) -- deduplication.c
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

/****************************************************************************/
/***                                                                      ***/
/***   Checksumming and deduplication support                             ***/
/***                                                                      ***/
/****************************************************************************/

#if TAGSISTANT_AUTOTAG_IN_MULTIPLE_THREADS
	GMutex tagsistant_deduplication_mutex;
	GHashTable *tagsistant_deduplication_table;
#else
	GAsyncQueue *tagsistant_deduplication_queue;
#endif

#define TAGSISTANT_DO_AUTOTAGGING 1
#define TAGSISTANT_DONT_DO_AUTOTAGGING 0

/**
 * deduplication function called by tagsistant_calculate_object_checksum
 *
 * @param inode the object inode
 * @param hex the checksum string
 * @param dbi DBI connection handle
 * @return true if autotagging is requested, false otherwise
 */
int tagsistant_querytree_find_duplicates(tagsistant_querytree *qtree, gchar *hex)
{
	tagsistant_inode main_inode = 0;

	/*
	 * get the first inode matching the checksum
	 */
	tagsistant_query(
		"select inode from objects where checksum = '%s' order by inode limit 1",
		qtree->dbi,	tagsistant_return_integer, &main_inode,	hex);

	/*
	 * if main_inode is zero, something gone wrong, we must
	 * return here, but auto-tagging can be performed
	 */
	if (!main_inode) {
		dbg('2', LOG_ERR, "Inode 0 returned for checksum %s", hex);
		return (TAGSISTANT_DO_AUTOTAGGING);
	}

	/*
	 * if this is the only copy of the file, we can
	 * return and auto-tagging can be performed
	 */
	if (qtree->inode == main_inode) return (TAGSISTANT_DO_AUTOTAGGING);

	dbg('2', LOG_INFO, "Deduplicating %s: %d -> %d", qtree->full_archive_path, qtree->inode, main_inode);

	/* first move all the tags of qtree->inode to main_inode */
	tagsistant_query(
		"update tagging set inode = %d where inode = %d",
		qtree->dbi,	NULL, NULL,	main_inode,	qtree->inode);

	/* then delete records left because of duplicates in key(inode, tag_id) in the tagging table */
	tagsistant_query(
		"delete from tagging where inode = %d",
		qtree->dbi,	NULL, NULL,	qtree->inode);

	/* unlink the removable inode */
	tagsistant_query(
		"delete from objects where inode = %d",
		qtree->dbi, NULL, NULL,	qtree->inode);

	/* and finally delete it from the archive directory */
	qtree->schedule_for_unlink = 1;

#if TAGSISTANT_ENABLE_AND_SET_CACHE
	/*
	 * invalidate the and_set cache
	 */
	tagsistant_invalidate_and_set_cache_entries(qtree);
#endif

	// don't do autotagging, the file has gone
	return (TAGSISTANT_DONT_DO_AUTOTAGGING);
}

/**
 * kernel of the deduplication thread
 *
 * @param data the path to be deduplicated (must be casted back to gchar*)
 */
gpointer tagsistant_deduplication_kernel(gpointer data)
{
	gchar *path = (gchar *) data;

	/* create a qtree object just to extract the full_archive_path */
	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 0, 0, 1);

	if (qtree) {
		int fd = open(qtree->full_archive_path, O_RDONLY|O_NOATIME);
		tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);

		if (-1 != fd) {
			dbg('2', LOG_INFO, "Running deduplication on %s", path);

			GChecksum *checksum = g_checksum_new(G_CHECKSUM_SHA1);
			guchar *buffer = g_new0(guchar, 65535);

			if (checksum && buffer) {
				/* feed the checksum object */
				int length = 0;
				do {
					length = read(fd, buffer, 65535);
					g_checksum_update(checksum, buffer, length);
				} while (length > 0);

				/* get the hexadecimal checksum string */
				gchar *hex = g_strdup(g_checksum_get_string(checksum));

				/* destroy the checksum object */
				g_checksum_free(checksum);
				g_free_null(buffer);

				/* re-create the qtree object */
				qtree = tagsistant_querytree_new(path, 0, 1, 1);

				if (qtree) {
					/* save the string into the objects table */
					tagsistant_query(
						"update objects set checksum = '%s' where inode = %d",
						qtree->dbi, NULL, NULL, hex, qtree->inode);
	
					/* look for duplicated objects */
					if (tagsistant_querytree_find_duplicates(qtree, hex)) {
#if TAGSISTANT_ENABLE_AUTOTAGGING
						/*
						 * do in-place autotagging
						 */
						dbg('p', LOG_INFO, "Doing autotagging on %s", path);
						tagsistant_process(qtree);
#endif
					}

					tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);
				}

				/* free the hex checksum string */
				g_free_null(hex);
			}
		}

		close(fd);
	}

#if TAGSISTANT_AUTOTAG_IN_MULTIPLE_THREADS
	/* remove the path from the deduplication table */
	g_mutex_lock(&tagsistant_deduplication_mutex);
	g_hash_table_remove(tagsistant_deduplication_table, path);
	g_mutex_unlock(&tagsistant_deduplication_mutex);

	/* exit deduplication thread */
	g_thread_exit(NULL);
#endif

	return (NULL);
}

/**
 * This is the loop that calls the deduplication thread
 * when TAGSISTANT_AUTOTAG_IN_MULTIPLE_THREADS is 0
 */
#if !TAGSISTANT_AUTOTAG_IN_MULTIPLE_THREADS
gpointer tagsistant_deduplication_loop(gpointer data) {
	(void) data;

	while (1) {
		/* get a path from the queue */
		gchar *path = (gchar *) g_async_queue_pop(tagsistant_deduplication_queue);

		/* process the path only if it's not null */
		if (path && strlen(path)) tagsistant_deduplication_kernel(path);

		/* throw away the path */
		g_free_null(path);
	}

	return (NULL);
}
#endif

/**
 * Callback for tagsistant_fix_checksums()
 */
int tagsistant_fix_checksums_callback(void *null_pointer, dbi_result result)
{
	(void) null_pointer;

	/* fetch the inode and the objectname from the query */
	const gchar *inode = dbi_result_get_string_idx(result, 1);
	const gchar *objectname = dbi_result_get_string_idx(result, 2);

	/* build the path using the ALL/ tag */
	gchar *path = g_strdup_printf("/store/ALL/@@/%s%s%s", inode, TAGSISTANT_INODE_DELIMITER, objectname);

	/* deduplicate the object */
	tagsistant_deduplicate(path);

	/* free the path and return */
	g_free(path);

	return (0);
}

/**
 * called once() after starting to fix object entries lacking the checksum
 */
void tagsistant_fix_checksums()
{
	/* get a dedicated connection */
	dbi_conn *dbi = tagsistant_db_connection(0);

	/*
	 * find all the objects without a checksum. the inode is cast to varchar(12)
	 * to simplify the callback function
	 */
	tagsistant_query(
		"select cast(inode as varchar(12)), objectname from objects where checksum = ''",
		dbi, tagsistant_fix_checksums_callback, NULL);

	tagsistant_db_connection_release(dbi);
}

/**
 * Setup deduplication thread and facilities
 */
void tagsistant_deduplication_init()
{
#if TAGSISTANT_AUTOTAG_IN_MULTIPLE_THREADS
	/* setup the hash table to avoid deduplicating the same file twice */
	tagsistant_deduplication_table = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
#else
	/* setup the deduplication queue */
	tagsistant_deduplication_queue = g_async_queue_new_full(g_free);
	g_async_queue_ref(tagsistant_deduplication_queue);

	/* start the deduplication thread */
	g_thread_new("Deduplication thread", tagsistant_deduplication_loop, NULL);
#endif

	/* fix missing checksums */
	// tagsistant_fix_checksums();
}

/**
 * deduplicate an object
 *
 * @path the path to be deduplicated
 */
void tagsistant_deduplicate(gchar *path)
{
#if TAGSISTANT_AUTOTAG_IN_MULTIPLE_THREADS
	g_mutex_lock(&tagsistant_deduplication_mutex);
	if (!g_hash_table_contains(tagsistant_deduplication_table, path)) {
		g_thread_new(path, tagsistant_deduplication_kernel, path);
		g_hash_table_insert(tagsistant_deduplication_table, path, GINT_TO_POINTER(1));
	}
	g_mutex_unlock(&tagsistant_deduplication_mutex);
#else
	if (TAGSISTANT_DBI_SQLITE_BACKEND == tagsistant.sql_database_driver) {
		tagsistant_deduplication_kernel(path);
	} else {
		g_async_queue_push(tagsistant_deduplication_queue, g_strdup(path));
	}
#endif
}

#if 0
/**
 * Deduplicates the object pointed by the querytree
 *
 * @param qtree the querytree object
 * @return true if autotagging is required, false otherwise
 */
int tagsistant_querytree_deduplicate(tagsistant_querytree *qtree)
{
	dbg('2', LOG_INFO, "Running deduplication on %s", qtree->object_path);

	/* guess if the object is a file or a symlink */
	struct stat buf;
	if ((-1 == lstat(qtree->full_archive_path, &buf)) || (!S_ISREG(buf.st_mode) && !S_ISLNK(buf.st_mode)))
		return (TAGSISTANT_DONT_DO_AUTOTAGGING);

	dbg('2', LOG_INFO, "Checksumming %s", qtree->full_archive_path);

	/*
	 * we'll return a 'do autotagging' condition even if
	 * a problem arises in computing file checksum
	 */
	int do_autotagging = TAGSISTANT_DO_AUTOTAGGING;

	/*
	 * open the file and read its content to compute is checksum
	 */
	int fd = open(qtree->full_archive_path, O_RDONLY|O_NOATIME);
	if (-1 != fd) {
		GChecksum *checksum = g_checksum_new(G_CHECKSUM_SHA1);
		guchar *buffer = g_new0(guchar, 65535);

		if (checksum && buffer) {
			/* feed the checksum object */
			int length = 0;
			do {
				length = read(fd, buffer, 65535);
				g_checksum_update(checksum, buffer, length);
			} while (length);

			/* get the hexadecimal checksum string */
			gchar *hex = g_strdup(g_checksum_get_string(checksum));

			/* destroy the checksum object */
			g_checksum_free(checksum);
			g_free_null(buffer);

			/* save the string into the objects table */
			tagsistant_query(
				"update objects set checksum = '%s' where inode = %d",
				qtree->dbi, NULL, NULL, hex, qtree->inode);

			/* look for duplicated objects */
			do_autotagging = tagsistant_querytree_find_duplicates(qtree, hex);

			/* free the hex checksum string */
			g_free_null(hex);
		}
		close(fd);
	}

	return (do_autotagging);
}
#endif
