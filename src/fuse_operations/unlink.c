/*
   Tagsistant (tagfs) -- fuse_operations/unlink.c
   Copyright (C) 2006-2009 Tx0 <tx0@strumentiresistenti.org>

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

#include "../tagsistant.h"

/**
 * unlink equivalent
 *
 * @param path the path to be unlinked (deleted)
 * @return(0 on success, -errno otherwise)
 */
int tagsistant_unlink(const char *path)
{
    int res = 0, tagsistant_errno = 0;
	gchar *unlink_path = NULL;

	TAGSISTANT_START("/ UNLINK on %s", path);

	// build querytree
	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
	} else

	// -- objects on disk --
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		if (QTREE_IS_TAGGABLE(qtree) && QTREE_IS_TAGS(qtree)) {

			// if object is pointed by a tags/ query, then untag it
			// from the tags included in the query path...
			tagsistant_querytree_traverse(qtree, tagsistant_sql_untag_object, qtree->inode);

			// ...then check if it's tagged elsewhere...
			// ...if still tagged, then avoid real unlink(): the object must survive!
			if (tagsistant_object_is_tagged(qtree->inode))
				goto UNLINK_EXIT;

			// otherwise just delete if from the objects table and go on.
		} else if (QTREE_IS_ARCHIVE(qtree)) {
			// if the query path points to archive/, it's clear that the
			// object is required to disappear from the filesystem, so
			// must be erased from the tagging table.
			tagsistant_full_untag_object(qtree->inode);
		}

		// wipe the object from objects table...
		tagsistant_query("delete from objects where object_id = %d", NULL, NULL, qtree->inode);

		// ... and do the real unlink()
		unlink_path = qtree->full_archive_path;
		res = unlink(unlink_path);
		tagsistant_errno = errno;

	} else

	// -- tags --
	// -- stats --
	// -- relations --
	{
		res = -1;
		tagsistant_errno = EROFS;
	}

UNLINK_EXIT:
	stop_labeled_time_profile("unlink");

	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("\\ UNLINK on %s (%s) (%s): %d %d: %s", path, unlink_path, tagsistant_querytree_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ UNLINK on %s (%s): OK", path, tagsistant_querytree_type(qtree));
	}

	tagsistant_querytree_destroy(qtree);
	return((res == -1) ? -tagsistant_errno : 0);
}
