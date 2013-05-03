/*
   Tagsistant (tagfs) -- fuse_operations/read.c
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
 * read() equivalent
 *
 * @param path the path of the file to be read
 * @param buf buffer holding read() result
 * @param size how many bytes should/can be read
 * @param offset starting of the read
 * @param fi struct fuse_file_info used for open() flags
 * @return(0 on success, -errno otherwise)
 */
int tagsistant_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    int res = 0, tagsistant_errno = 0;
    gchar stats_buffer[1024];

	TAGSISTANT_START("READ on %s [size: %lu offset: %lu]", path, (long unsigned int) size, (long unsigned int) offset);

	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 1, 0, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree))
		TAGSISTANT_ABORT_OPERATION(ENOENT);

	// -- object on disk --
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		if (!qtree->full_archive_path) {
			dbg('F', LOG_ERR, "Null qtree->full_archive_path");
			TAGSISTANT_ABORT_OPERATION(EFAULT);
		}

		int fd = open(qtree->full_archive_path, fi->flags|O_RDONLY);

		if (-1 == fd) {
			TAGSISTANT_ABORT_OPERATION(errno);
		}

		res = pread(fd, buf, size, offset);
		tagsistant_errno = errno;
		close(fd);
	}

	// -- stats --
	else if (QTREE_IS_STATS(qtree)) {
		memset(stats_buffer, 0, 1024);

		// -- connections --
		if (g_regex_match_simple("/connections$", path, 0, 0)) {
			if (offset == 0) {
				sprintf(stats_buffer, "# of MySQL open connections: %d\n", connections);
				size_t stats_size = strlen(stats_buffer);

				memcpy(buf, stats_buffer, stats_size);
				res = stats_size;
			} else {
				res = 0;
			}
		}

#if TAGSISTANT_ENABLE_QUERYTREE_CACHE
		// -- cached_queries --
		else if (g_regex_match_simple("/cached_queries$", path, 0, 0)) {
			if (offset == 0) {
				int entries = tagsistant_querytree_cache_total();
				sprintf(stats_buffer, "# of cached queries: %d\n", entries);
				size_t stats_size = strlen(stats_buffer);

				memcpy(buf, stats_buffer, stats_size);
				res = stats_size;
			} else {
				res = 0;
			}
		}
#endif /* TAGSISTANT_ENABLE_QUERYTREE_CACHE */

		// -- configuration --
		else if (g_regex_match_simple("/configuration$", path, 0, 0)) {
			if (offset == 0) {
				sprintf(stats_buffer,
					"\n"
					" --> Command line options:\n\n"
					"         mountpoint: %s\n"
					"    repository path: %s\n"
					"   database options: %s\n"
					"  run in foreground: %d\n"
					"    single threaded: %d\n"
					"    mount read-only: %d\n"
					"              debug: %s\n"
					"                     [%c] boot\n"
					"                     [%c] cache\n"
					"                     [%c] file tree (readdir)\n"
					"                     [%c] FUSE operations (open, read, write, symlink, ...)\n"
					"                     [%c] low level\n"
					"                     [%c] plugin\n"
					"                     [%c] query parsing\n"
					"                     [%c] reasoning\n"
					"                     [%c] SQL queries\n"
					"                     [%c] deduplication\n"
					"\n"
					" --> Compile flags:\n\n"
					"  TAGSISTANT_ENABLE_QUERYTREE_CACHE: %d\n"
					"     TAGSISTANT_ENABLE_TAG_ID_CACHE: %d\n"
					"    TAGSISTANT_ENABLE_AND_SET_CACHE: %d\n"
					" TAGSISTANT_RETAG_INTERNAL_SYMLINKS: %d\n"
					"         TAGSISTANT_VERBOSE_LOGGING: %d\n"
					"         TAGSISTANT_QUERY_DELIMITER: %c\n"
					"        TAGSISTANT_ANDSET_DELIMITER: %c\n"
					"         TAGSISTANT_INODE_DELIMITER: '%s'\n\n",
					tagsistant.mountpoint,
					tagsistant.repository,
					tagsistant.dboptions,
					tagsistant.foreground,
					tagsistant.singlethread,
					tagsistant.readonly,
					tagsistant.debug ? tagsistant.debug : "-",
					tagsistant.dbg['b'] ? 'x' : ' ',
					tagsistant.dbg['c'] ? 'x' : ' ',
					tagsistant.dbg['f'] ? 'x' : ' ',
					tagsistant.dbg['F'] ? 'x' : ' ',
					tagsistant.dbg['l'] ? 'x' : ' ',
					tagsistant.dbg['p'] ? 'x' : ' ',
					tagsistant.dbg['q'] ? 'x' : ' ',
					tagsistant.dbg['r'] ? 'x' : ' ',
					tagsistant.dbg['s'] ? 'x' : ' ',
					tagsistant.dbg['2'] ? 'x' : ' ',
					TAGSISTANT_ENABLE_QUERYTREE_CACHE,
					TAGSISTANT_ENABLE_TAG_ID_CACHE,
					TAGSISTANT_ENABLE_AND_SET_CACHE,
					TAGSISTANT_RETAG_INTERNAL_SYMLINKS,
					TAGSISTANT_VERBOSE_LOGGING,
					TAGSISTANT_QUERY_DELIMITER_CHAR,
					TAGSISTANT_ANDSET_DELIMITER_CHAR,
					TAGSISTANT_INODE_DELIMITER);
				size_t stats_size = strlen(stats_buffer);

				memcpy(buf, stats_buffer, stats_size);
				res = stats_size;
			} else {
				res = 0;
			}
		}

		// -- objects --
		else if (g_regex_match_simple("/objects$", path, 0, 0)) {
			if (offset == 0) {
				int entries = 0;
				tagsistant_query("select count(1) as entries from objects", qtree->dbi, tagsistant_return_integer, &entries);
				sprintf(stats_buffer, "# of objects: %d\n", entries);
				size_t stats_size = strlen(stats_buffer);

				memcpy(buf, stats_buffer, stats_size);
				res = stats_size;
			} else {
				res = 0;
			}
		}

		// -- tags --
		else if (g_regex_match_simple("/tags$", path, 0, 0)) {
			if (offset == 0) {
				int entries = 2;
				tagsistant_query("select count(1) as entries from tags", qtree->dbi, tagsistant_return_integer, &entries);
				sprintf(stats_buffer, "# of tags: %d\n", entries);
				size_t stats_size = strlen(stats_buffer);

				memcpy(buf, stats_buffer, stats_size);
				res = stats_size;
			} else {
				res = 0;
			}
		}

		// -- relations --
		else if (g_regex_match_simple("/relations$", path, 0, 0)) {
			if (offset == 0) {
				int entries = 0;
				tagsistant_query("select count(1) as entries from relations", qtree->dbi, tagsistant_return_integer, &entries);
				sprintf(stats_buffer, "# of relations: %d\n", entries);
				size_t stats_size = strlen(stats_buffer);

				memcpy(buf, stats_buffer, stats_size);
				res = stats_size;
			} else {
				res = 0;
			}
		}

	}

	// -- tags --
	// -- relations --
	else TAGSISTANT_ABORT_OPERATION(EINVAL);

TAGSISTANT_EXIT_OPERATION:
	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("READ %s (%s) (%s): %d %d: %s", path, qtree->full_archive_path, tagsistant_querytree_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
	} else {
		TAGSISTANT_STOP_OK("READ %s (%s): OK", path, tagsistant_querytree_type(qtree));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);
	}

	return ((res == -1) ? -tagsistant_errno : res);
}

