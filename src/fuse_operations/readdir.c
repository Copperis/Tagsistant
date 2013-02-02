/*
   Tagsistant (tagfs) -- fuse_operations/readdir.c
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
 * used by add_entry_to_dir() SQL callback to perform readdir() operations
 */
struct tagsistant_use_filler_struct {
	fuse_fill_dir_t filler;			/**< libfuse filler hook to return dir entries */
	void *buf;						/**< libfuse buffer to hold readdir results */
	const char *path;				/**< the path that generates the query */
	tagsistant_querytree *qtree;	/**< the querytree that originated the readdir() */
};

/**
 * SQL callback. Add dir entries to libfuse buffer.
 *
 * @param filler_ptr struct tagsistant_use_filler_struct pointer (cast to void*)
 * @param result dbi_result pointer
 * @return(0 (always, see SQLite policy, may change in the future))
 */
static int tagsistant_add_entry_to_dir(void *filler_ptr, dbi_result result)
{
	struct tagsistant_use_filler_struct *ufs = (struct tagsistant_use_filler_struct *) filler_ptr;
	const char *dir = dbi_result_get_string_idx(result, 1);

	if (dir == NULL || strlen(dir) == 0)
		return(0);

	/* check if this tag has been already listed inside the path */
	ptree_or_node *ptx = ufs->qtree->tree;
	if (ptx) {
		while (NULL != ptx->next) ptx = ptx->next; // last OR section

		ptree_and_node *and_t = ptx->and_set;
		while (NULL != and_t) {
			if (g_strcmp0(and_t->tag, dir) == 0) {
				return(0);
			}
			and_t = and_t->next;
		}
	}

	return(ufs->filler(ufs->buf, dir, NULL, 0));
}

int tagsistant_readdir_on_object(
		tagsistant_querytree *qtree,
		const char *path,
		void *buf,
		fuse_fill_dir_t filler,
		int *tagsistant_errno)
{
	(void) path;

	DIR *dp = opendir(qtree->full_archive_path);
	if (NULL == dp) {
		*tagsistant_errno = errno;
		dbg(LOG_ERR, "Unable to readdir(%s)", qtree->full_archive_path);
		return -1;
	}

	struct dirent *de;
	while ((de = readdir(dp)) != NULL) {
		// dbg(LOG_INFO, "Adding entry %s", de->d_name);
		struct stat st;
		memset(&st, 0, sizeof(st));
		st.st_ino = de->d_ino;
		st.st_mode = de->d_type << 12;
		if (filler(buf, de->d_name, &st, 0))
			break;
	}

	closedir(dp);
	return 0;
}

int tagsistant_readdir_on_tags(
		tagsistant_querytree *qtree,
		const char *path,
		void *buf,
		fuse_fill_dir_t filler,
		off_t offset,
		int *tagsistant_errno)
{
	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

	if (qtree->complete) {
		// build the filetree
		tagsistant_file_handle *fh = tagsistant_filetree_new(qtree->tree);

		// check filetree is not null
		if (NULL == fh) {
			*tagsistant_errno = EBADF;
			return -1;
		}

		// save filetree reference to later destroy it
		tagsistant_file_handle *fh_save = fh;

		// add each filetree node to directory
		do {
			if ( (fh->name != NULL) && strlen(fh->name)) {
				dbg(LOG_INFO, "Adding %s to directory", fh->name);
				if (filler(buf, fh->name, NULL, offset))
					break;
			}
			fh = fh->next;
		} while ( fh != NULL && fh->name != NULL );

		// destroy the file tree
		tagsistant_filetree_destroy(fh_save);
	} else {
		// add operators if path is not "/tags", to avoid
		// "/tags/+" and "/tags/="
		if (g_strcmp0(path, "/tags") != 0) {
			filler(buf, "+", NULL, 0);
			filler(buf, "=", NULL, 0);
		}

		/*
	 	* if path does not terminate by =,
	 	* directory should be filled with tagsdir registered tags
	 	*/
		struct tagsistant_use_filler_struct *ufs = g_new0(struct tagsistant_use_filler_struct, 1);
		if (ufs == NULL) {
			dbg(LOG_ERR, "Error allocating memory");
			*tagsistant_errno = EBADF;
			return -1;
		}

		ufs->filler = filler;
		ufs->buf = buf;
		ufs->path = path;
		ufs->qtree = qtree;

		/* parse tagsdir list */
		tagsistant_query("select tagname from tags;", tagsistant_add_entry_to_dir, ufs);
		freenull(ufs);
	}

	return 0;
}

int tagsistant_readdir_on_relations(
		tagsistant_querytree *qtree,
		const char *path,
		void *buf,
		fuse_fill_dir_t filler,
		int *tagsistant_errno)
{
	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

	struct tagsistant_use_filler_struct *ufs = g_new0(struct tagsistant_use_filler_struct, 1);
	if (ufs == NULL) {
		dbg(LOG_ERR, "Error allocating memory");
		*tagsistant_errno = EBADF;
		return -1;
	}

	ufs->filler = filler;
	ufs->buf = buf;
	ufs->path = path;
	ufs->qtree = qtree;

	if (qtree->second_tag) {
		// nothing
		dbg(LOG_INFO, "readdir on /relations/something/relations/something_else");
	} else if (qtree->relation) {
		// list all tags related to first_tag with this relation
		dbg(LOG_INFO, "readdir on /relations/something/relation/");
		tagsistant_query("select tags.tagname from tags join relations on relations.tag2_id = tags.tag_id join tags as firsttags on firsttags.tag_id = relations.tag1_id where firsttags.tagname = '%s' and relation = '%s';",
			tagsistant_add_entry_to_dir, ufs, qtree->first_tag, qtree->relation);

	} else if (qtree->first_tag) {
		// list all relations
		dbg(LOG_INFO, "readdir on /relations/something/");
		filler(buf, "includes", NULL, 0);
		filler(buf, "is_equivalent", NULL, 0);

		/*
		tagsistant_query("select relation from relations join tags on tags.tag_id = relations.tag1_id where tagname = '%s';",
			tagsistant_add_entry_to_dir, ufs, qtree->first_tag);
		*/
	} else {
		// list all tags
		dbg(LOG_INFO, "readdir on /relations");
		tagsistant_query("select tagname from tags;", tagsistant_add_entry_to_dir, ufs);
	}

	freenull(ufs);
	return 0;
}

int tagsistant_readdir_on_stats(
		tagsistant_querytree *qtree,
		const char *path,
		void *buf,
		fuse_fill_dir_t filler,
		int *tagsistant_errno)
{
	(void) path;
	(void) qtree;
	(void) tagsistant_errno;

	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);
	// fill with available statistics

	return 0;
}

/**
 * readdir equivalent (in FUSE paradigm)
 *
 * @param path the path of the directory to be read
 * @param buf buffer holding directory entries
 * @param filler libfuse fuse_fill_dir_t function to save entries in *buf
 * @param offset offset of next read
 * @param fi struct fuse_file_info passed by libfuse; unused.
 * @return(0 on success, -errno otherwise)
 */
int tagsistant_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
	int res = 0, tagsistant_errno = 0;

	(void) fi;

	TAGSISTANT_START("/ READDIR on %s", path);

	// build querytree
	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 0, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		dbg(LOG_INFO, "readdir on malformed path %s", path);
		TAGSISTANT_ABORT_OPERATION(ENOENT);

	} else if ((QTREE_POINTS_TO_OBJECT(qtree) && qtree->full_archive_path) || QTREE_IS_ARCHIVE(qtree)) {
		dbg(LOG_INFO, "readdir on object %s", path);
		res = tagsistant_readdir_on_object(qtree, path, buf, filler, &tagsistant_errno);

	} else if (QTREE_IS_ROOT(qtree)) {
		dbg(LOG_INFO, "readdir on root %s", path);

		/* insert pseudo directories: tags/ archive/ relations/ and stats/ */
		filler(buf, ".", NULL, 0);
		filler(buf, "..", NULL, 0);
		filler(buf, "archive", NULL, 0);
		filler(buf, "relations", NULL, 0);
		filler(buf, "retag", NULL, 0);
		filler(buf, "stats", NULL, 0);
		filler(buf, "tags", NULL, 0);

	} else if (QTREE_IS_TAGS(qtree)) {
		dbg(LOG_INFO, "readdir on tags");
		res = tagsistant_readdir_on_tags(qtree, path, buf, filler, offset, &tagsistant_errno);

	} else if (QTREE_IS_RELATIONS(qtree)) {
		dbg(LOG_INFO, "readdir on relations");
		res = tagsistant_readdir_on_relations(qtree, path, buf, filler, &tagsistant_errno);

	} else if (QTREE_IS_STATS(qtree) || QTREE_IS_RETAG(qtree)) {
		dbg(LOG_INFO, "readdir on relations");
		res = tagsistant_readdir_on_stats(qtree, path, buf, filler, &tagsistant_errno);

	}

TAGSISTANT_EXIT_OPERATION:
	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("\\ READDIR on %s (%s): %d %d: %s", path, tagsistant_querytree_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ READDIR on %s (%s): OK", path, tagsistant_querytree_type(qtree));
	}

	tagsistant_querytree_destroy(qtree);
	return((res == -1) ? -tagsistant_errno : 0);
}
