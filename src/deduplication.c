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

#if TAGSISTANT_ENABLE_DEDUPLICATION || TAGSISTANT_ENABLE_AUTOTAGGING
GThread *tagsistant_dedup_autotag_thread = NULL;
GAsyncQueue *tagsistant_dedup_autotag_queue = NULL;
#endif

/**
 * Cache inode resolution from DB
 */
#if TAGSISTANT_ENABLE_AND_SET_CACHE
extern GRWLock tagsistant_and_set_cache_lock;
extern GHashTable *tagsistant_and_set_cache;
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

	/* get the first inode matching the checksum */
	tagsistant_query(
		"select inode from objects where checksum = \"%s\" order by inode limit 1",
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
 * Deduplicate the object pointed by the querytree
 *
 * @param qtree the querytree object
 * @return true if autotagging is required, false otherwise
 */
int tagsistant_querytree_deduplicate(tagsistant_querytree *qtree)
{
	dbg('2', LOG_INFO, "Running deduplication on %s", qtree->object_path);

	int do_autotagging = TAGSISTANT_DONT_DO_AUTOTAGGING;

	/* guess if the object is a file or a symlink */
	struct stat buf;
	if ((-1 == lstat(qtree->full_archive_path, &buf)) || (!S_ISREG(buf.st_mode) && !S_ISLNK(buf.st_mode)))
		return (do_autotagging);

	dbg('2', LOG_INFO, "Checksumming %s", qtree->full_archive_path);

	/* we'll return a 'do autotagging' condition even if a problem arise in computing file checksum */
	do_autotagging = TAGSISTANT_DO_AUTOTAGGING;

	/* open the file and read its content to compute is checksum */
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
				"update objects set checksum = '%s' where inode = %d;",
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

/**
 * This is the kernel of the autotagging and deduplication thread.
 */
void tagsistant_dedup_and_autotag_thread(gpointer data) {
	(void) data;

#if TAGSISTANT_ENABLE_DEDUPLICATION || TAGSISTANT_ENABLE_AUTOTAGGING
	while (1) {
		// get a path from the queue
		gchar *path = (gchar *) g_async_queue_pop(tagsistant_dedup_autotag_queue);

		// process the path only if it's not null
		if (path && strlen(path)) {

			// build the querytree from the path
			tagsistant_querytree *qtree = tagsistant_querytree_new(path, 0, 1);
#endif

#if TAGSISTANT_ENABLE_DEDUPLICATION
			// deduplicate the object
			if (tagsistant_querytree_deduplicate(qtree)) {
#endif

#if TAGSISTANT_ENABLE_AUTOTAGGING
				// run the autotagging plugin stack
				tagsistant_process(qtree);
#endif

#if TAGSISTANT_ENABLE_DEDUPLICATION
			}
#endif

#if TAGSISTANT_ENABLE_DEDUPLICATION || TAGSISTANT_ENABLE_AUTOTAGGING
			// destroy the querytree
			tagsistant_querytree_destroy(qtree, 1);
		}

		g_free_null(path);
	}
#endif
}
