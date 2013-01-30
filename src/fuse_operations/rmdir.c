/*
   Tagsistant (tagfs) -- fuse_operations/rmdir.c
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
 * rmdir equivalent
 *
 * @param path the tag (directory) to be removed
 * @return(0 on success, -errno otherwise)
 */
int tagsistant_rmdir(const char *path)
{
    int res = 0, tagsistant_errno = 0;
	gchar *rmdir_path = NULL;

	TAGSISTANT_START("/ RMDIR on %s", path);

	tagsistant_querytree *qtree = tagsistant_querytree_new(path, FALSE);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
	} else

	// -- tags --
	// -- archive
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		if (QTREE_IS_TAGGABLE(qtree)) {
			// remove all the tags associated to the object
			tagsistant_traverse_querytree(qtree, tagsistant_sql_untag_object, qtree->inode);
		} else {

			// do a real mkdir
			rmdir_path = qtree->full_archive_path;
			res = rmdir(rmdir_path);
			tagsistant_errno = errno;
		}
	} else

	// -- tags but incomplete (means: delete a tag) --
	if (QTREE_IS_TAGS(qtree)) {
		tagsistant_sql_delete_tag(qtree->last_tag);
	} else

	// -- relations --
	if (QTREE_IS_RELATIONS(qtree)) {
		// rmdir can be used only on third level
		// since first level is all available tags
		// and second level is all available relations
		if (qtree->second_tag) {
			// create a new relation between two tags
			tagsistant_sql_create_tag(qtree->second_tag);
			int tag1_id = tagsistant_sql_get_tag_id(qtree->first_tag);
			int tag2_id = tagsistant_sql_get_tag_id(qtree->second_tag);
			if (tag1_id && tag2_id && IS_VALID_RELATION(qtree->relation)) {
				tagsistant_query(
					"delete from relations where tag1_id = \"%d\" and tag2_id = \"%d\" and relation = \"%s\"",
					NULL, NULL, tag1_id, tag2_id, qtree->relation);
			} else {
				res = -1;
				tagsistant_errno = EFAULT;
			}
		} else {
			res = -1;
			tagsistant_errno = EROFS;
		}
	} else

	// -- stats
	if (QTREE_IS_STATS(qtree)) {
		res = -1;
		tagsistant_errno = EROFS;
	}

	stop_labeled_time_profile("rmdir");

	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("\\ RMDIR on %s (%s): %d %d: %s", path, tagsistant_querytree_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ RMDIR on %s (%s): OK", path, tagsistant_querytree_type(qtree));
	}

	tagsistant_querytree_destroy(qtree);
	return((res == -1) ? -tagsistant_errno : 0);
}
