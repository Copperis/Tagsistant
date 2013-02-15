/*
   Tagsistant (tagfs) -- fuse_operations/open.c
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
 * performs real open() on a file. Used by tagsistant_open(),
 * tagsistant_read() and tagsistant_write().
 *
 * @param filepath the path to be open()ed
 * @param flags how to open file (see open(2) for more informations)
 * @param _errno returns open() errno
 * @return(open() return value)
 * @todo Should it perform permissions checking???
 */
int tagsistant_internal_open(tagsistant_querytree *qtree, int flags, int *_errno)
{
	// first check on plain path
	int res = open(qtree->full_archive_path, flags);
	*_errno = errno;

#if VERBOSE_DEBUG
	dbg(LOG_INFO, "tagsistant_internal_open(%s): %d", filepath, res);
	if (flags&O_CREAT) dbg(LOG_INFO, "...O_CREAT");
	if (flags&O_WRONLY) dbg(LOG_INFO, "...O_WRONLY");
	if (flags&O_TRUNC) dbg(LOG_INFO, "...O_TRUNC");
	if (flags&O_LARGEFILE) dbg(LOG_INFO, "...O_LARGEFILE");
#endif

	return(res);
}

/**
 * open() equivalent
 *
 * @param path the path to be open()ed
 * @param fi struct fuse_file_info holding open() flags
 * @return(0 on success, -errno otherwise)
 */
int tagsistant_open(const char *path, struct fuse_file_info *fi)
{
    int res = -1, tagsistant_errno = ENOENT;

	TAGSISTANT_START("/ OPEN on %s", path);

	gchar *open_path = NULL;

	// build querytree
	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 1, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		TAGSISTANT_ABORT_OPERATION(ENOENT);
	}

	// -- object --
	else if (QTREE_POINTS_TO_OBJECT(qtree)) {
		open_path = qtree->full_archive_path;
		res = tagsistant_internal_open(qtree, fi->flags|O_RDONLY, &tagsistant_errno);
		if (-1 != res) close(res);
	}

	// -- stats --
	else if (QTREE_IS_STATS(qtree)) {
		open_path = qtree->stats_path;
		// do proper action
	}

	// -- tags --
	// -- relations --
	else TAGSISTANT_ABORT_OPERATION(EROFS);

TAGSISTANT_EXIT_OPERATION:
	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("\\ OPEN on %s (%s) (%s): %d %d: %s", path, open_path, tagsistant_querytree_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
	} else {
		TAGSISTANT_STOP_OK("\\ OPEN on %s (%s): OK", path, tagsistant_querytree_type(qtree));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);
	}

	return((res == -1) ? -tagsistant_errno : 0);
}
