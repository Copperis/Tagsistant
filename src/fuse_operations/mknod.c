/*
   Tagsistant (tagfs) -- fuse_operations/mknod.c
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
 * mknod equivalent (used to create even regular files)
 *
 * @param path the path of the file (block, char, fifo) to be created
 * @param mode file type and permissions
 * @param rdev major and minor numbers, if applies
 * @return(0 on success, -errno otherwise)
 */
int tagsistant_mknod(const char *path, mode_t mode, dev_t rdev)
{
	int res = 0, tagsistant_errno = 0;

	TAGSISTANT_START("/ MKNOD on %s [mode: %u rdev: %u]", path, mode, (unsigned int) rdev);

	gchar *stripped_path = tagsistant_ID_strip_from_path(path);

	// build querytree
	tagsistant_querytree_t *qtree = tagsistant_build_querytree(stripped_path, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		res = -1;
		tagsistant_errno = EFAULT;
		goto MKNOD_EXIT;
	} else

	// -- tags --
	// -- archive --
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		if (QTREE_IS_TAGGABLE(qtree)) {
			res = tagsistant_force_create_and_tag_object(qtree, &tagsistant_errno);
			if (-1 == res) goto MKNOD_EXIT;
		}

		dbg(LOG_INFO, "NEW object on disk: mknod(%s)", qtree->full_archive_path);
		res = mknod(qtree->full_archive_path, mode, rdev);
		tagsistant_errno = errno;
	} else

	// -- stats --
	// -- relations --
	{
		res = -1;
		tagsistant_errno = EROFS;
	}

MKNOD_EXIT:
	stop_labeled_time_profile("mknod");

	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("\\ MKNOD on %s (%s) (%s): %d %d: %s", path, qtree->full_archive_path, tagsistant_query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		tagsistant_set_alias(path, qtree->full_archive_path);
		TAGSISTANT_STOP_OK("\\ MKNOD on %s (%s): OK", path, tagsistant_query_type(qtree));
	}

	g_free(stripped_path);
	tagsistant_destroy_querytree(qtree);
	return((res == -1) ? -tagsistant_errno : 0);
}