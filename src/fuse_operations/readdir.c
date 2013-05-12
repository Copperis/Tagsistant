/*
   Tagsistant (tagfs) -- fuse_operations/readdir.c
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

static int tagsistant_readdir_on_tags_filler(gchar *name, GList *fh_list, struct tagsistant_use_filler_struct *ufs)
{
	(void) name;

	if (!fh_list) return (0);

	if (NULL == fh_list) return (0);

	if (!(fh_list->next)) {
		// just add the filename
		tagsistant_file_handle *fh = fh_list->data;

		if (!fh) return (0);

		return (ufs->filler(ufs->buf, fh->name, NULL, 0));
	}

	// add inodes to filenames
	while (fh_list) {
		tagsistant_file_handle *fh = fh_list->data;
		if (fh) {
			gchar *filename = g_strdup_printf("%d%s%s", fh->inode, TAGSISTANT_INODE_DELIMITER, fh->name);
			ufs->filler(ufs->buf, filename, NULL, 0);
			g_free_null(filename);
		}
		fh_list = fh_list->next;
	}

	return (0);
}

int tagsistant_readdir_on_tags(
		tagsistant_querytree *qtree,
		const char *path,
		void *buf,
		fuse_fill_dir_t filler,
		off_t offset,
		int *tagsistant_errno)
{
	(void) offset;

	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

	static gchar *tagsistant_check_tags_path_regex =
		"/(" TAGSISTANT_ANDSET_DELIMITER "|" TAGSISTANT_QUERY_DELIMITER "|" TAGSISTANT_QUERY_DELIMITER_NO_REASONING ")$";

	/*
 	* if path does not terminate by @,
 	* directory should be filled with tagsdir registered tags
 	*/
	struct tagsistant_use_filler_struct *ufs = g_new0(struct tagsistant_use_filler_struct, 1);
	if (!ufs) {
		dbg('F', LOG_ERR, "Error allocating memory");
		*tagsistant_errno = ENOMEM;
		return (-1);
	}

	ufs->filler = filler;
	ufs->buf = buf;
	ufs->path = path;
	ufs->qtree = qtree;

	if (qtree->complete) {

		// build the filetree
		GHashTable *hash_table = tagsistant_filetree_new(qtree->tree, qtree->dbi);
		g_hash_table_foreach(hash_table, (GHFunc) tagsistant_readdir_on_tags_filler, ufs);
		g_hash_table_foreach(hash_table, (GHFunc) tagsistant_filetree_destroy_value_list, NULL);
		g_hash_table_destroy(hash_table);

	} else {

		// add operators if path is not "/tags", to avoid "/tags/+" and "/tags/@"
		if ((!g_regex_match_simple(tagsistant_check_tags_path_regex, path, G_REGEX_EXTENDED, 0)) && g_strcmp0(path, "/tags")) {
			filler(buf, TAGSISTANT_ANDSET_DELIMITER, NULL, 0);
			filler(buf, TAGSISTANT_QUERY_DELIMITER, NULL, 0);
			filler(buf, TAGSISTANT_QUERY_DELIMITER_NO_REASONING, NULL, 0);
		}

		/* parse tagsdir list */
		tagsistant_query("select tagname from tags;", qtree->dbi, tagsistant_add_entry_to_dir, ufs);
	}

	g_free_null(ufs);
	return (0);
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
		dbg('F', LOG_ERR, "Unable to readdir(%s)", qtree->full_archive_path);
		return (-1);
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
	return (0);
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
		dbg('F', LOG_ERR, "Error allocating memory");
		*tagsistant_errno = EBADF;
		return (-1);
	}

	ufs->filler = filler;
	ufs->buf = buf;
	ufs->path = path;
	ufs->qtree = qtree;

	if (qtree->second_tag) {
		// nothing
	} else if (qtree->relation) {
		// list all tags related to first_tag with this relation
		tagsistant_query(
			"select tags.tagname from tags "
				"join relations on relations.tag2_id = tags.tag_id "
				"join tags as firsttags on firsttags.tag_id = relations.tag1_id "
				"where firsttags.tagname = '%s' and relation = '%s';",
			qtree->dbi,
			tagsistant_add_entry_to_dir,
			ufs,
			qtree->first_tag,
			qtree->relation);

	} else if (qtree->first_tag) {
		// list all relations
		filler(buf, "includes", NULL, 0);
		filler(buf, "is_equivalent", NULL, 0);

		/*
		tagsistant_query(
			"select relation from relations "
				"join tags on tags.tag_id = relations.tag1_id "
				"where tagname = '%s';",
			qtree->conn,
			tagsistant_add_entry_to_dir,
			ufs,
			qtree->first_tag);
		*/
	} else {
		// list all tags
		tagsistant_query("select tagname from tags;", qtree->dbi, tagsistant_add_entry_to_dir, ufs);
	}

	g_free_null(ufs);
	return (0);
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
#if TAGSISTANT_ENABLE_QUERYTREE_CACHE
	filler(buf, "cached_queries", NULL, 0);
#endif /* TAGSISTANT_ENABLE_QUERYTREE_CACHE */
	filler(buf, "configuration", NULL, 0);
	filler(buf, "connections", NULL, 0);
	filler(buf, "objects", NULL, 0);
	filler(buf, "relations", NULL, 0);
	filler(buf, "tags", NULL, 0);

	// fill with available statistics

	return (0);
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

	TAGSISTANT_START("READDIR on %s", path);

	// build querytree
	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 1, 0, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		TAGSISTANT_ABORT_OPERATION(ENOENT);

	} else if ((QTREE_POINTS_TO_OBJECT(qtree) && qtree->full_archive_path) || QTREE_IS_ARCHIVE(qtree)) {
		res = tagsistant_readdir_on_object(qtree, path, buf, filler, &tagsistant_errno);

	} else if (QTREE_IS_ROOT(qtree)) {

		/* insert pseudo directories: tags/ archive/ relations/ and stats/ */
		filler(buf, ".", NULL, 0);
		filler(buf, "..", NULL, 0);
		filler(buf, "archive", NULL, 0);
		filler(buf, "relations", NULL, 0);
//		filler(buf, "retag", NULL, 0);
		filler(buf, "stats", NULL, 0);
		filler(buf, "tags", NULL, 0);

	} else if (QTREE_IS_TAGS(qtree)) {
		res = tagsistant_readdir_on_tags(qtree, path, buf, filler, offset, &tagsistant_errno);

	} else if (QTREE_IS_RELATIONS(qtree)) {
		res = tagsistant_readdir_on_relations(qtree, path, buf, filler, &tagsistant_errno);

	} else if (QTREE_IS_STATS(qtree) || QTREE_IS_RETAG(qtree)) {
		res = tagsistant_readdir_on_stats(qtree, path, buf, filler, &tagsistant_errno);

	}

TAGSISTANT_EXIT_OPERATION:
	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("READDIR on %s (%s): %d %d: %s", path, tagsistant_querytree_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
	} else {
		TAGSISTANT_STOP_OK("READDIR on %s (%s): OK", path, tagsistant_querytree_type(qtree));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);
	}

	return((res == -1) ? -tagsistant_errno : 0);
}
