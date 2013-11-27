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

// filetree functions
extern GHashTable *tagsistant_filetree_new(ptree_or_node *query, dbi_conn conn);
extern void tagsistant_filetree_destroy_value_list(gchar *key, GList *list_p, gpointer data);
#define tagsistant_filetree_destroy_value g_free_null

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

	/* this must be the last value, just exit */
	if (dir == NULL) return(0);

	/*
	 * zero-length values can be returned while listing triple tags
	 * we must suppress them, but returning 1, to prevent the callback
	 * from exiting its cycle
	 */
	if (strlen(dir) == 0) return (1);

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

static int tagsistant_readdir_on_store_filler(gchar *name, GList *fh_list, struct tagsistant_use_filler_struct *ufs)
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

/**
 * Return true if the operators +/, @/ and @@/ should be added to
 * while listing the content of a store/ query
 */
int tagsistant_do_add_operators(tagsistant_querytree *qtree)
{
	gchar tagsistant_check_tags_path_regex[] =
		"/(\\" TAGSISTANT_ANDSET_DELIMITER "|" TAGSISTANT_QUERY_DELIMITER "|" TAGSISTANT_QUERY_DELIMITER_NO_REASONING ")$";

	if (!g_regex_match_simple(tagsistant_check_tags_path_regex, qtree->full_path, G_REGEX_EXTENDED, 0)) {
		if (g_strcmp0(qtree->full_path, "/tags")) {
			if (!qtree->namespace || (qtree->namespace && qtree->value)) {
				return (1);
			}
		}
	}

	return (0);
}

int tagsistant_readdir_on_store(
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
		g_hash_table_foreach(hash_table, (GHFunc) tagsistant_readdir_on_store_filler, ufs);
		g_hash_table_foreach(hash_table, (GHFunc) tagsistant_filetree_destroy_value_list, NULL);
		g_hash_table_destroy(hash_table);

	} else {

		// add operators if path is not "/tags", to avoid "/tags/+" and "/tags/@"
		if (tagsistant_do_add_operators(qtree)) {
			filler(buf, TAGSISTANT_ANDSET_DELIMITER, NULL, 0);
			filler(buf, TAGSISTANT_QUERY_DELIMITER, NULL, 0);
			filler(buf, TAGSISTANT_QUERY_DELIMITER_NO_REASONING, NULL, 0);
		}

		if (qtree->value) {
			tagsistant_query("select distinct tagname from tags;", qtree->dbi, tagsistant_add_entry_to_dir, ufs);
		} else if (qtree->operator) {
			tagsistant_query("select distinct value from tags where tagname = \"%s\" and key = \"%s\";", qtree->dbi, tagsistant_add_entry_to_dir, ufs, qtree->namespace, qtree->key);
		} else if (qtree->key) {
			filler(buf, TAGSISTANT_EQUALS_TO_OPERATOR, NULL, 0);
			filler(buf, TAGSISTANT_CONTAINS_OPERATOR, NULL, 0);
			filler(buf, TAGSISTANT_GREATER_THAN_OPERATOR, NULL, 0);
			filler(buf, TAGSISTANT_SMALLER_THAN_OPERATOR, NULL, 0);
		} else if (qtree->namespace) {
			tagsistant_query("select distinct key from tags where tagname = \"%s\";", qtree->dbi, tagsistant_add_entry_to_dir, ufs, qtree->namespace);
		} else {
			tagsistant_query("select distinct tagname from tags;", qtree->dbi, tagsistant_add_entry_to_dir, ufs);
		}
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

int tagsistant_readdir_on_tags(
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

	if (qtree->first_tag) {
		// nothing
	} else if (qtree->value) {
		// nothing
	} else if (qtree->key) {
		tagsistant_query("select distinct value from tags where tagname = \"%s\" and key = \"%s\";", qtree->dbi, tagsistant_add_entry_to_dir, ufs, qtree->namespace, qtree->key);
	} else if (qtree->namespace) {
		tagsistant_query("select distinct key from tags where tagname = \"%s\";", qtree->dbi, tagsistant_add_entry_to_dir, ufs, qtree->namespace);
	} else {
		// list all tags
		tagsistant_query("select distinct tagname from tags;", qtree->dbi, tagsistant_add_entry_to_dir, ufs);
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
	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 0, 0, 1);

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
		filler(buf, "store", NULL, 0);
		filler(buf, "tags", NULL, 0);

	} else if (QTREE_IS_STORE(qtree)) {
		res = tagsistant_readdir_on_store(qtree, path, buf, filler, offset, &tagsistant_errno);

	} else if (QTREE_IS_TAGS(qtree)) {
		res = tagsistant_readdir_on_tags(qtree, path, buf, filler, &tagsistant_errno);

	} else if (QTREE_IS_RELATIONS(qtree)) {
		res = tagsistant_readdir_on_relations(qtree, path, buf, filler, &tagsistant_errno);

	} else if (QTREE_IS_STATS(qtree) || QTREE_IS_RETAG(qtree)) {
		res = tagsistant_readdir_on_stats(qtree, path, buf, filler, &tagsistant_errno);

	}

TAGSISTANT_EXIT_OPERATION:
	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("READDIR on %s (%s): %d %d: %s", path, tagsistant_querytree_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
		return (-tagsistant_errno);
	} else {
		TAGSISTANT_STOP_OK("READDIR on %s (%s): OK", path, tagsistant_querytree_type(qtree));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);
		return (0);
	}
}

/************************************************************************************/
/***                                                                              ***/
/*** FileTree translation                                                         ***/
/***                                                                              ***/
/************************************************************************************/

/**
 * add a file to the file tree (callback function)
 */
static int tagsistant_add_to_filetree(void *hash_table_pointer, dbi_result result)
{
	/* Cast the hash table */
	GHashTable *hash_table = (GHashTable *) hash_table_pointer;

	/* fetch query results */
	gchar *name = dbi_result_get_string_copy_idx(result, 1);
	if (!name) return (0);

	tagsistant_inode inode = dbi_result_get_uint_idx(result, 2);

	/* lookup the GList object */
	GList *list = g_hash_table_lookup(hash_table, name);

	/* look for duplicates due to reasoning results */
	GList *list_tmp = list;
	while (list_tmp) {
		tagsistant_file_handle *fh_tmp = (tagsistant_file_handle *) list_tmp->data;

		if (fh_tmp && (fh_tmp->inode == inode)) {
			g_free_null(name);
			return (0);
		}

		list_tmp = list_tmp->next;
	}

	/* fetch query results into tagsistant_file_handle struct */
	tagsistant_file_handle *fh = g_new0(tagsistant_file_handle, 1);
	if (!fh) {
		g_free_null(name);
		return (0);
	}

	g_strlcpy(fh->name, name, 1024);
	fh->inode = inode;
	g_free_null(name);

	/* add the new element */
	// TODO valgrind says: check for leaks
	g_hash_table_insert(hash_table, g_strdup(fh->name), g_list_prepend(list, fh));

//	dbg('f', LOG_INFO, "adding (%d,%s) to filetree", fh->inode, fh->name);

	return (0);
}

/**
 * build a linked list of filenames that apply to querytree
 * query expressed in query. querytree translate as follows
 * while using SQLite:
 *
 *   1. each ptree_and_node_t list converted in a INTERSECT
 *   multi-SELECT query, and is saved as a VIEW. The name of
 *   the view is the string tv%.8X where %.8X is the memory
 *   location of the ptree_or_node_t to which the list is
 *   linked.
 *
 *   2. a global select is built using all the views previously
 *   created joined by UNION operators. a sqlite3_exec call
 *   applies it using add_to_filetree() as callback function.
 *
 *   3. all the views are removed with a DROP VIEW query.
 *
 * MySQL does not feature the INTERSECT operator. So each
 * ptree_and_node_t list is converted to a set of nested
 * queries. Steps 2 and 3 apply unchanged.
 *
 * @param query the ptree_or_node_t* query structure to
 * be resolved.
 */
GHashTable *tagsistant_filetree_new(ptree_or_node *query, dbi_conn conn)
{
	if (query == NULL) {
		dbg('f', LOG_ERR, "NULL path_tree_t object provided to %s", __func__);
		return(NULL);
	}

	/* save a working pointer to the query */
	ptree_or_node *query_dup = query;

	//
	// MySQL does not support intersect!
	// So we must find an alternative way.
	// Some hints:
	//
	// 1. select distinct objectname, objects.inode as inode from objects
	//    join tagging on tagging.inode = objects.inode
	//    join tags on tagging.tag_id = tags.tag_id
	//    where tags.tagname in ("t1", "t2", "t3");
	//
	// Main problem with 1. is that it finds all the objects tagged
	// at least with one of the tags listed, not what the AND-set
	// means
	//
	// 2. select distinct objects.inode from objects
	//    inner join tagging on tagging.inode = objects.inode
	//    inner join tags on tagging.tag_id = tags.tag_id
	//    where tagname = 't1' and objects.inode in (
	//      select distinct objects.inode from objects
	//      inner join tagging on tagging.inode = objects.inode
	//      inner join tags on tagging.tag_id = tags.tag_id
	//      where tagname = 't2' and objects.inode in (
	//        select distinct objects.inode from objects
	//        inner join tagging on tagging.inode = objects.inode
	//        inner join tags on tagging.tag_id = tags.tag_id
	//        where tagname = 't3'
	//      )
	//    )
	//
	// That's the longest and less readable form but seems to work.
	//

	int nesting = 0;
	while (query != NULL) {
		ptree_and_node *tag = query->and_set;

		// preallocate 50Kbytes for the string
		// TODO valgrind says: check for leaks
		GString *statement = g_string_sized_new(51200);

		g_string_printf(statement, "create view tv%.16" PRIxPTR " as ", (uintptr_t) query);

		while (tag != NULL) {

			/* create the list of tags (natural or related) to match */
			GString *tag_condition = g_string_sized_new(1024);

			if (tag->tag) {
				g_string_printf(tag_condition, "tagname in (\"%s\"", tag->tag);
				if (tag->related != NULL) {
					ptree_and_node *related = tag->related;
					while (related != NULL) {
						g_string_append_printf(tag_condition, ", \"%s\"", related->tag);
						related = related->related;
					}
				}
				g_string_append(tag_condition, ")");
			} else if (tag->namespace && tag->key && tag->value) {
				switch (tag->operator) {
					case TAGSISTANT_CONTAINS:
						g_string_printf(tag_condition,
							"tagname = \"%s\" and key = \"%s\" and value like \"%%s%\"",
							tag->namespace, tag->key, tag->value);
						break;
					case TAGSISTANT_GREATER_THAN:
						g_string_printf(tag_condition,
							"tagname = \"%s\" and key = \"%s\" and value > \"%s\"",
							tag->namespace, tag->key, tag->value);
						break;
					case TAGSISTANT_SMALLER_THAN:
						g_string_printf(tag_condition,
							"tagname = \"%s\" and key = \"%s\" and value < \"%s\"",
							tag->namespace, tag->key, tag->value);
						break;
					case TAGSISTANT_EQUAL_TO:
					default:
						g_string_printf(tag_condition,
							"tagname = \"%s\" and key = \"%s\" and value = \"%s\"",
							tag->namespace, tag->key, tag->value);
						break;
				}
			}

			if (tagsistant.sql_backend_have_intersect) {

				/* shorter syntax for SQL dialects that provide INTERSECT */
				g_string_append_printf(statement,
					"select objectname, objects.inode as inode "
						"from objects "
						"join tagging on tagging.inode = objects.inode "
						"join tags on tags.tag_id = tagging.tag_id "
						"where %s ", tag_condition->str);

				if (tag->next != NULL)
					g_string_append(statement, " intersect ");

			} else {

				/* longer syntax for SQL dialects that do not provide INTERSECT */
				if (nesting) {
					g_string_append_printf(statement,
						" and objects.inode in ("
							"select distinct objects.inode from objects "
								"inner join tagging on tagging.inode = objects.inode "
								"inner join tags on tagging.tag_id = tags.tag_id "
								"where %s ", tag_condition->str);
				} else {
					g_string_append_printf(statement,
						" select distinct objectname, objects.inode as inode from objects "
							"inner join tagging on tagging.inode = objects.inode "
							"inner join tags on tagging.tag_id = tags.tag_id "
							"where %s ", tag_condition->str);
				}

				/* increment nesting counter, used to match parenthesis later */
				nesting++;
			}

			g_string_free(tag_condition, TRUE);
			tag = tag->next;
		}

		/* add closing parenthesis on the end of non-INTERSECT queries */
		if (!tagsistant.sql_backend_have_intersect) {
			nesting--; // we need one closed parenthesis less than nested subquery

			while (nesting) {
				g_string_append(statement, ")");
				nesting--;
			}
		}

		dbg('f', LOG_INFO, "SQL: filetree query [%s]", statement->str);

		/* create view */
		tagsistant_query(statement->str, conn, NULL, NULL);
		g_string_free(statement,TRUE);
		query = query->next;
	}

	/* format view statement */
	GString *view_statement = g_string_sized_new(10240);
	query = query_dup;
	while (query != NULL) {
		g_string_append_printf(view_statement, "select objectname, inode from tv%.16" PRIxPTR, (uintptr_t) query);

		if (query->next != NULL) g_string_append(view_statement, " union ");

		query = query->next;
	}

//	dbg('f', LOG_INFO, "SQL view query [%s]", view_statement->str);

	/* apply view statement */
	GHashTable *file_hash = g_hash_table_new(g_str_hash, g_str_equal);

	tagsistant_query(view_statement->str, conn, tagsistant_add_to_filetree, file_hash);

	/* free the SQL statement */
	g_string_free(view_statement, TRUE);

	/* drop the views */
	while (query_dup) {
		tagsistant_query("drop view tv%.16" PRIxPTR, conn, NULL, NULL, (uintptr_t) query_dup);
		query_dup = query_dup->next;
	}

	return(file_hash);
}

/**
 * Destroy a tagsistant_file_handle
 */


/**
 * Destroy a filetree element GList list of tagsistant_file_handle.
 * This will free the GList data structure by first calling
 * tagsistant_filetree_destroy_value() on each linked node.
 */
void tagsistant_filetree_destroy_value_list(gchar *key, GList *list, gpointer data)
{
	(void) data;

	g_free_null(key);

	if (list) g_list_free_full(list, (GDestroyNotify) g_free /* was tagsistant_filetree_destroy_value */);
}
