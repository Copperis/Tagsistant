/*
   Tagsistant (tagfs) -- fuse_operations/getattr.c
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
 * lstat equivalent
 *
 * @param path the path to be lstat()ed
 * @param stbuf pointer to struct stat buffer holding data about file
 * @return(0 on success, -errno otherwise)
 */
int tagsistant_getattr(const char *path, struct stat *stbuf)
{
    int res = 0, tagsistant_errno = 0;
	gchar *lstat_path = NULL;

	TAGSISTANT_START("/ GETATTR on %s", path);

	// build querytree
	tagsistant_querytree_t *qtree = tagsistant_build_querytree(path, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
		goto GETATTR_EXIT;
	} else
	
	// -- object on disk --
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		if (!tagsistant_check_tagging_consistency(qtree)) {
			gchar *alias = tagsistant_get_alias(path);
			if (!alias) {
				res = -1;
				tagsistant_errno = ENOENT;
				goto GETATTR_EXIT;
			}

		}

		lstat_path = qtree->full_archive_path;
		dbg(LOG_INFO, "lstat_path = %s", lstat_path);
	} else

	// -- tags --
	// -- archive --
	// -- root --
	// -- stats --
	// -- relations --
	{
		lstat_path = tagsistant.archive;
	}

	// do the real lstat()
	res = lstat(lstat_path, stbuf);
	tagsistant_errno = errno;

	//
	// a getattr may fail if issued on a path that
	// was just created by mknod or mkdir. so getattr
	// can query the aliases hash table to guess
	// if a path changed since its creation
	//
	// i.e.: 'cp filename tags/t1/=' will create a
	// file called N.filename in the archive/ directory
	// where N is the tagsistant_id assigned to the
	// file. its path will be:
	//
	//   archive/N___filename
	//   tags/t1/=/N___filename
	//
	if (-1 == res) {
		lstat_path = tagsistant_get_alias(path);
		if (NULL != lstat_path) {
			res = lstat(lstat_path, stbuf);
			tagsistant_errno = errno;
		}
	}

	// postprocess output
	if (QTREE_IS_TAGS(qtree)) {
		// dbg(LOG_INFO, "getattr: last tag is %s", qtree->last_tag);
		if (NULL == qtree->last_tag) {
			// ok
		} else if (g_strcmp0(qtree->last_tag, "+") == 0) {
			// path ends by '+'
			stbuf->st_ino += 1;
			stbuf->st_mode = S_IFDIR|S_IRUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH;
			stbuf->st_nlink = 1;
		} else if (g_strcmp0(qtree->last_tag, "=") == 0) {
			// path ends by '='
			stbuf->st_ino += 2;
			stbuf->st_mode = S_IFDIR|S_IRUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH;
			stbuf->st_nlink = 1;
		} else {
			tagsistant_id tag_id = tagsistant_get_exact_tag_id(qtree->last_tag);
			if (tag_id) {
				// each directory holds 3 inodes: itself/, itself/+, itself/=
				stbuf->st_ino = tag_id * 3;
			} else {
				tagsistant_errno = ENOENT;
				res = -1;
			}
		}
	} else if (QTREE_IS_STATS(qtree) || QTREE_IS_RELATIONS(qtree)) {
		// mangle inode for relations and stats
	}

GETATTR_EXIT:
	stop_labeled_time_profile("getattr");

	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("\\ GETATTR on %s (%s) {%s}: %d %d: %s", path, lstat_path, tagsistant_query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ GETATTR on %s (%s): OK", path, tagsistant_query_type(qtree));
	}

	tagsistant_destroy_querytree(qtree);
	return((res == -1) ? -tagsistant_errno : 0);
}
