/*
   Tagsistant (tagfs) -- fuse_operations/getattr.c
   Copyright (C) 2006-2013 Tx0 <tx0@strumentiresistenti.org>

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

	TAGSISTANT_START("GETATTR on %s", path);

	// build querytree
	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 0, 0, 1);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree))
		TAGSISTANT_ABORT_OPERATION(ENOENT);
	
	// -- object on disk --
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		tagsistant_querytree_check_tagging_consistency(qtree);

		if (qtree->full_archive_path && (qtree->exists || QTREE_IS_ARCHIVE(qtree))) {
			lstat_path = qtree->full_archive_path;
		} else {
			TAGSISTANT_ABORT_OPERATION(ENOENT);
		}
	}

	// -- alias --
	else if (QTREE_IS_ALIAS(qtree)) {
		if (qtree->alias) {
			int exists = tagsistant_sql_alias_exists(qtree->dbi, qtree->alias);
			if (!exists) {
				TAGSISTANT_ABORT_OPERATION(ENOENT);
			}
			lstat_path = tagsistant.tags;
		} else {
			lstat_path = tagsistant.archive;
		}
	}

	// -- relations --
	else if (QTREE_IS_RELATIONS(qtree)) {
		/* if first tag does not exist, return ENOENT */
		if (qtree->first_tag) {
			tagsistant_inode tag_id = tagsistant_sql_get_tag_id(qtree->dbi, qtree->first_tag, "", "");
			if (!tag_id) TAGSISTANT_ABORT_OPERATION(ENOENT);
		}

		/* process a full relation */
		if (qtree->second_tag) {
			// check if the relation is valid
			gchar *check_name = NULL;

			tagsistant_query(
				"select t2.tagname from tags as t2 "
					"join relations on tag2_id = t2.tag_id "
					"join tags as t1 on t1.tag_id = relations.tag1_id "
					"where t1.tagname = '%s' and relation = '%s' and t2.tagname = '%s'",
				qtree->dbi,
				tagsistant_return_string,
				&check_name,
				qtree->first_tag,
				qtree->relation,
				qtree->second_tag);

			if ((NULL == check_name) || (strcmp(qtree->second_tag, check_name) != 0)) {
				TAGSISTANT_ABORT_OPERATION(ENOENT);
			} else {
				lstat_path = tagsistant.archive;
			}
		} else {
			lstat_path = tagsistant.archive;
		}

	}

	// -- stats --
	else if (QTREE_IS_STATS(qtree)) {
		if (g_regex_match_simple("^/stats/(connections|cached_queries|configuration|objects|relations|tags)$", path, 0, 0))
			lstat_path = tagsistant.tags;
		else if (g_regex_match_simple("^/stats$", path, 0, 0))
			lstat_path = tagsistant.archive;
		else
			TAGSISTANT_ABORT_OPERATION(ENOENT);
	}

	// -- store (incomplete) --
	// -- tags --
	// -- archive (the directory itself) --
	// -- root --
	else lstat_path = tagsistant.archive;

	// do the real lstat()
	res = lstat(lstat_path, stbuf);
	tagsistant_errno = errno;

	// post-processing output
	if (QTREE_IS_STORE(qtree)) {
		// dbg(LOG_INFO, "getattr: last tag is %s", qtree->last_tag);
		if (NULL == qtree->last_tag) {
			// OK
		} else if (g_strcmp0(qtree->last_tag, TAGSISTANT_ANDSET_DELIMITER) == 0) {

			// path ends by '+'
			stbuf->st_ino += 1;
			stbuf->st_mode = S_IFDIR|S_IRUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH;
			stbuf->st_nlink = 1;

		} else if ((g_strcmp0(qtree->last_tag, TAGSISTANT_QUERY_DELIMITER) == 0) ||
			(g_strcmp0(qtree->last_tag, TAGSISTANT_QUERY_DELIMITER_NO_REASONING) == 0)) {

			// path ends by TAGSISTANT_QUERY_DELIMITER (with or without reasoning)
			stbuf->st_ino += 2;
			stbuf->st_mode = S_IFDIR|S_IRUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH;
			stbuf->st_nlink = 1;

		} else if (g_regex_match_simple("^" TAGSISTANT_ALIAS_IDENTIFIER, qtree->last_tag, 0, 0)) {
			gchar *alias_name = qtree->last_tag + 1;
			int exists = tagsistant_sql_alias_exists(qtree->dbi, alias_name);
			if (!exists) {
				TAGSISTANT_ABORT_OPERATION(ENOENT);
			}

		} else {

			tagsistant_inode tag_id;
			if (qtree->namespace) {
				tag_id = tagsistant_sql_get_tag_id(qtree->dbi, qtree->namespace, qtree->key, qtree->value);
			} else {
				tag_id = tagsistant_sql_get_tag_id(qtree->dbi, qtree->last_tag, NULL, NULL);
			}
			if (tag_id) {
				// each directory holds 3 inodes: itself/, itself/+, itself/@
				stbuf->st_ino = tag_id * 3;
			} else {
				TAGSISTANT_ABORT_OPERATION(ENOENT);
			}

		}
	} else if (QTREE_IS_ALIAS(qtree) && qtree->alias) {

		stbuf->st_size = tagsistant_sql_alias_get_length(qtree->dbi, qtree->alias);

	} else if (QTREE_IS_STATS(qtree)) {

		stbuf->st_size = TAGSISTANT_STATS_BUFFER;

	} else if (QTREE_IS_TAGS(qtree)) {
		gchar *tagname = qtree->first_tag ? qtree->first_tag : qtree->namespace;
		if (tagname) {
			if (qtree->second_tag) TAGSISTANT_ABORT_OPERATION(ENOENT);

			tagsistant_inode tag_id = tagsistant_sql_get_tag_id(qtree->dbi, tagname, qtree->key, qtree->value);
			if (tag_id) {
				stbuf->st_ino = tag_id * 3;
			} else {
				TAGSISTANT_ABORT_OPERATION(ENOENT);
			}
		}

	} else if (QTREE_IS_RELATIONS(qtree)) {

		// mangle inode for relations and stats

	}

TAGSISTANT_EXIT_OPERATION:
	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("GETATTR on %s (%s) {%s}: %d %d: %s", path, lstat_path, tagsistant_querytree_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
		return (-tagsistant_errno);
	} else {
		TAGSISTANT_STOP_OK("GETATTR on %s (%s): OK", path, tagsistant_querytree_type(qtree));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);
		return (0);
	}
}
