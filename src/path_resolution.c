/*
   Tagsistant (tagfs) -- path_resolution.c
   Copyright (C) 2006-2009 Tx0 <tx0@strumentiresistenti.org>

   Transform paths into queries and apply queries to file sets to
   grep files matching with queries.

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

#include "tagsistant.h"

gchar *tagsistant_querytree_types[QTYPE_TOTAL];

#if TAGSISTANT_ENABLE_QUERYTREE_CACHE
/**
 * tagsistant_querytree objects cache
 */
GHashTable *tagsistant_querytree_cache = NULL;
GRWLock tagsistant_querytree_cache_lock;
#endif

#if TAGSISTANT_ENABLE_AND_SET_CACHE
/**
 * Cache inode resolution from DB
 */
GHashTable *tagsistant_and_set_cache = NULL;
#endif

/**
 * Counts one single element of the querytree hashtable
 *
 * @return
 */
void tagsistant_querytree_cache_counter(gpointer key, gpointer value, gpointer user_data)
{
	(void) key;
	(void) value;

 	int *elements = (int *) user_data;
	*elements = *elements + 1;
}

/**
 * Count the elements contained in the querytree cache
 *
 * @param qtree
 */
int tagsistant_querytree_cache_total()
{
	int elements = 0;
	g_hash_table_foreach(tagsistant_querytree_cache, tagsistant_querytree_cache_counter, &elements);
	return (elements);
}

/**
 * Just a wrapper around tagsistant_querytree_destroy() called when
 * an element is removed from tagsistant_querytree_cache.
 *
 * @param qtree the querytree object to be removed
 */
void tagsistant_querytree_cache_destroy_element(tagsistant_querytree *qtree)
{
	tagsistant_querytree_destroy(qtree, 0);
}

/**
 * Initialize path_resolution.c module
 */
void tagsistant_path_resolution_init()
{
	/* prepare stringified version of tagsistant_querytree types */
	tagsistant_querytree_types[QTYPE_MALFORMED] = g_strdup("QTYPE_MALFORMED");
	tagsistant_querytree_types[QTYPE_ROOT] = g_strdup("QTYPE_ROOT");
	tagsistant_querytree_types[QTYPE_ARCHIVE] = g_strdup("QTYPE_ARCHIVE");
	tagsistant_querytree_types[QTYPE_TAGS] = g_strdup("QTYPE_TAGS");
	tagsistant_querytree_types[QTYPE_RELATIONS] = g_strdup("QTYPE_RELATIONS");
	tagsistant_querytree_types[QTYPE_STATS] = g_strdup("QTYPE_STATS");

#if TAGSISTANT_ENABLE_QUERYTREE_CACHE
	/* initialize the tagsistant_querytree object cache */
	tagsistant_querytree_cache = g_hash_table_new_full(
		g_str_hash,
		g_str_equal,
		NULL,
		(GDestroyNotify) tagsistant_querytree_cache_destroy_element);
#endif

#if TAGSISTANT_ENABLE_AND_SET_CACHE
	tagsistant_and_set_cache = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, g_free);
#endif
}

/**
 * Given a linked list of ptree_and_node objects (called an and-set)
 * return a string with a comma separated list of all the tags.
 *
 * @param and_set the linked and-set list
 * @return a string like "tag1, tag2, tag3"
 */
gchar *tagsistant_compile_and_set(ptree_and_node *and_set, int *total)
{
	// TODO valgrind says: check for leaks
	GString *str = g_string_sized_new(1024);
	ptree_and_node *and_pointer = and_set;
	*total = 0;

	/* compile the string */
	while (and_pointer) {
		g_string_append_printf(str, "\"%s\"", and_pointer->tag);

		/* look for related tags too */
		ptree_and_node *related = and_pointer->related;
		while (related) {
			g_string_append_printf(str, ",\"%s\"", related->tag);
			related = related->related;
		}

		/* check to avoid terminating the line with a ',' */
		if (and_pointer->next)
			g_string_append(str, ", ");

		and_pointer = and_pointer->next;
		*total += 1;
	}

	/* save a pointer to the result */
	gchar *result = str->str;

	/* destroy the GString but not its content */
	g_string_free(str, FALSE);

	return (result);
}

/**
 * Try to guess the inode of an object by comparing DB contents
 * with and and-set of tags
 *
 * @param and_set a pointer to a ptree_and_node and-set data structure
 * @param dbi a libDBI dbi_conn reference
 * @param objectname the name of the object we are looking up the inode
 * @return the inode of the object if found, zero otherwise
 */
tagsistant_inode tagsistant_guess_inode_from_and_set(ptree_and_node *and_set, dbi_conn dbi, gchar *objectname)
{
	tagsistant_inode inode = 0;
	int tags_total = 0;
	gchar *and_set_string = tagsistant_compile_and_set(and_set, &tags_total);

#if TAGSISTANT_ENABLE_AND_SET_CACHE
	gchar *search_key = g_strdup_printf("%s:%s", objectname, and_set_string);

	// if lookup succeed, returns the inode
	tagsistant_inode *value = (tagsistant_inode *) g_hash_table_lookup(tagsistant_and_set_cache, search_key);
	if (value) {
		g_free_null(search_key);
		g_free_null(and_set_string);
		return (*value);
	}
#endif

	// lookup the inode in the SQL db
	tagsistant_query(
		"select objects.inode from objects "
			"join tagging on objects.inode = tagging.inode "
			"join tags on tagging.tag_id = tags.tag_id "
			"where tags.tagname in (%s) and objects.objectname = \"%s\" "
			"group by objects.inode "
				"having count(distinct tags.tagname) = %d",
		dbi,
		tagsistant_return_integer,
		&inode,
		and_set_string,
		objectname,
		tags_total
	);

#if TAGSISTANT_ENABLE_AND_SET_CACHE
	if (inode) {
		tagsistant_inode *value = g_new(tagsistant_inode, 1);
		*value = inode;
		g_hash_table_insert(tagsistant_and_set_cache, search_key, value);
	} else {
		g_free_null(search_key);
	}
#endif

	g_free_null(and_set_string);

	return (inode);
}

/**
 * Check if the object_path of the qtree is contained in at least one
 * of the and_sets referenced by the query
 *
 * @param qtree the tagsistant_querytree object to check
 * @return true if contained, false otherwise
 */
int tagsistant_querytree_check_tagging_consistency(tagsistant_querytree *qtree)
{
	qtree->exists = 0;
	tagsistant_inode inode = 0;

	// 0. no object path means no need to check for inner consistency
	if (!qtree->object_path)
		return (0);

	if (strlen(qtree->object_path) == 0) {
		qtree->exists = 1;
		return (1);
	}

	// 1. get the object path first element
	gchar *object_first_element = g_strdup(qtree->object_path);
	gchar *first_slash = g_strstr_len(object_first_element, strlen(object_first_element), G_DIR_SEPARATOR_S);
	if (first_slash)
		*first_slash = '\0';
	else
		qtree->is_taggable = 1;

	// 2. use the object_first_element to guess if its tagged in the provided set of tags
	ptree_or_node *or_tmp = qtree->tree;
	while (or_tmp) {
		inode = tagsistant_guess_inode_from_and_set(or_tmp->and_set, qtree->dbi, object_first_element);

#if 0
		int tags_total = 0;
		gchar *and_set = tagsistant_compile_and_set(or_tmp->and_set, &tags_total);

		tagsistant_query(
			"select objects.inode from objects "
				"join tagging on objects.inode = tagging.inode "
				"join tags on tagging.tag_id = tags.tag_id "
				"where tags.tagname in (%s) and objects.objectname = \"%s\" "
				"group by objects.inode "
					"having count(distinct tags.tagname) = %d",
			qtree->dbi,
			tagsistant_return_integer,
			&inode,
			and_set,
			object_first_element,
			tags_total
		);

		g_free_null(and_set);
#endif

		if (inode) {
			qtree->exists = 1;
			break;
		}

		or_tmp = or_tmp->next;
	}

	g_free_null(object_first_element);

	return(qtree->exists);
}

/**
 * parse the query portion between tags/ and =/
 *
 * @param qtree the querytree object
 * @param path the parsed path
 * @param token_ptr a pointer to the tokenized path (three stars because we need to move it across the array even in calling function)
 * @param do_reasoning boolean flag which activate the reasoner if true
 * @return 1 on success, 0 on failure or errors
 */
int tagsistant_querytree_parse_tags (
		tagsistant_querytree *qtree,
		const char *path,
		gchar ***token_ptr,
		int do_reasoning)
{
	unsigned int orcount = 0, andcount = 0;

	// initialize iterator variables on query tree nodes
	ptree_or_node *last_or = qtree->tree = g_new0(ptree_or_node, 1);
	if (qtree->tree == NULL) {
		tagsistant_querytree_destroy(qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
		dbg(LOG_ERR, "Error allocating memory");
		return (0);
	}

	ptree_and_node *last_and = NULL;

	// state if the query is complete or not
	qtree->complete = (NULL == g_strstr_len(path, strlen(path), TAGSISTANT_QUERY_DELIMITER)) ? 0 : 1;
#if TAGSISTANT_VERBOSE_LOGGING
	dbg(LOG_INFO, "Path %s is %scomplete", path, qtree->complete ? "" : "not ");
#endif

	// by default a query is valid until something wrong happens while parsing it
	qtree->valid = 1;

	// begin parsing
	while (**token_ptr && (TAGSISTANT_QUERY_DELIMITER_CHAR != ***token_ptr)) {
		if (strlen(**token_ptr) == 0) {
			/* ignore zero length tokens */
		} else if (strcmp(**token_ptr, TAGSISTANT_ANDSET_DELIMITER) == 0) {
			/* open new entry in OR level */
			orcount++;
			andcount = 0;
			ptree_or_node *new_or = g_new0(ptree_or_node, 1);
			if (new_or == NULL) {
				dbg(LOG_ERR, "Error allocating memory");
				return (0);
			}
			last_or->next = new_or;
			last_or = new_or;
			last_and = NULL;
		} else {
			/* save next token in new ptree_and_node_t slot */
			ptree_and_node *and = g_new0(ptree_and_node, 1);
			if (and == NULL) {
				dbg(LOG_ERR, "Error allocating memory");
				return (0);
			}
			and->tag = g_strdup(**token_ptr);
			and->next = NULL;
			and->related = NULL;
			if (last_and == NULL) {
				last_or->and_set = and;
			} else {
				last_and->next = and;
			}
			last_and = and;

#if TAGSISTANT_VERBOSE_LOGGING
			dbg(LOG_INFO, "Query tree nodes %.2d.%.2d %s", orcount, andcount, **token_ptr);
#endif
			andcount++;

			/* search related tags */
			if (do_reasoning) {
#if TAGSISTANT_VERBOSE_LOGGING
				dbg(LOG_INFO, "Searching for other tags related to %s", and->tag);
#endif

				tagsistant_reasoning *reasoning = g_malloc(sizeof(tagsistant_reasoning));
				if (reasoning != NULL) {
					reasoning->start_node = and;
					reasoning->current_node = and;
					reasoning->added_tags = 0;
					reasoning->conn = qtree->dbi;
					int newtags = tagsistant_reasoner(reasoning);
#if TAGSISTANT_VERBOSE_LOGGING
					dbg(LOG_INFO, "Reasoning added %d tags", newtags);
#else
					(void) newtags;
#endif
					g_free_null(reasoning);
				}
			}
		}

		// save last tag found
		g_free_null(qtree->last_tag);
		qtree->last_tag = g_strdup(**token_ptr);

		(*token_ptr)++;
	}

	// if last token is TAGSISTANT_QUERY_DELIMITER_CHAR,
	// move the pointer one element forward
	if (**token_ptr && (TAGSISTANT_QUERY_DELIMITER_CHAR == ***token_ptr))
		(*token_ptr)++;

	return (1);
}

/**
 * parse the query portion after relations/
 *
 * @param qtree the querytree object
 * @param token_ptr a pointer to the tokenized path (three stars because we need to move it across the array even in calling function)
 * @return 1 on success, 0 on failure or errors
 */
int tagsistant_querytree_parse_relations (
	tagsistant_querytree* qtree,
	gchar ***token_ptr)
{
	/* parse a relations query */
	if (NULL != **token_ptr) {
		qtree->first_tag = g_strdup(**token_ptr);
		(*token_ptr)++;
		if (NULL != **token_ptr) {
			qtree->relation = g_strdup(**token_ptr);
			(*token_ptr)++;
			if (NULL != **token_ptr) {
				qtree->second_tag = g_strdup(**token_ptr);
				qtree->complete = 1;
				(*token_ptr)++;
			}
		}
	}

	return (1);
}

/**
 * parse the query portion after stats/
 *
 * @param qtree the querytree object
 * @param token_ptr a pointer to the tokenized path (three stars because we need to move it across the array even in calling function)
 * @return 1 on success, 0 on failure or errors
 */
int tagsistant_querytree_parse_stats (
		tagsistant_querytree* qtree,
		gchar ***token_ptr)
{
	if (NULL != **token_ptr) {
		qtree->stats_path = g_strdup(**token_ptr);
		qtree->complete = 1;
	}

	return (1);
}

/**
 * set the object_path field of a tagsistant_querytree object and
 * then update all the other depending fields
 *
 * @param qtree the tagsistant_querytree object
 * @param path the new object_path which is copied by g_strdup()
 */
void tagsistant_querytree_set_object_path(tagsistant_querytree *qtree, char *new_object_path)
{
	if (!new_object_path) return;

	if (qtree->object_path) g_free_null(qtree->object_path);
	qtree->object_path = g_strdup(new_object_path);

	tagsistant_querytree_rebuild_paths(qtree);
}

/**
 * set the object inode and rebuild the paths
 *
 * @param qtree the tagsistant_querytree object
 * @param inode the new inode
 */
void tagsistant_querytree_set_inode(tagsistant_querytree *qtree, tagsistant_inode inode)
{
	if (!qtree || !inode) return;

	qtree->inode = inode;
	tagsistant_querytree_rebuild_paths(qtree);
}

/**
 * rebuild the paths of a tagsistant_querytree object
 *
 * @param qtree the tagsistant_querytree object
 */
void tagsistant_querytree_rebuild_paths(tagsistant_querytree *qtree)
{
	if (!qtree->inode) return;

	if (qtree->archive_path) g_free_null(qtree->archive_path);
	qtree->archive_path = g_strdup_printf("%d" TAGSISTANT_INODE_DELIMITER "%s", qtree->inode, qtree->object_path);

	if (qtree->full_archive_path) g_free_null(qtree->full_archive_path);
	qtree->full_archive_path = g_strdup_printf("%s%s", tagsistant.archive, qtree->archive_path);
}

#if TAGSISTANT_ENABLE_QUERYTREE_CACHE

/**
 * Duplicate the related branch of a ptree_and_tree branch
 *
 * @param origin
 * @return
 */
ptree_and_node *tagsistant_querytree_duplicate_ptree_and_node_related(ptree_and_node *origin)
{
	if (!origin) return (NULL);

	ptree_and_node *copy = g_new0(ptree_and_node, 1);
	if (!copy) return (NULL);

	copy->tag = g_strdup(origin->tag);
	copy->related = tagsistant_querytree_duplicate_ptree_and_node_related(origin->related);

	return (copy);
}

/**
 * Duplicate a ptree_and_node branch
 *
 * @param origin
 * @return
 */
ptree_and_node *tagsistant_querytree_duplicate_ptree_and_node(ptree_and_node *origin)
{
	if (!origin) return (NULL);

	ptree_and_node *copy = g_new0(ptree_and_node, 1);
	if (!copy) return (NULL);

	copy->tag = g_strdup(origin->tag);
	copy->related = tagsistant_querytree_duplicate_ptree_and_node_related(origin->related);
	copy->next = tagsistant_querytree_duplicate_ptree_and_node(origin->next);

	return (copy);
}

/**
 * Duplicate a ptree_or_node tree from a querytree
 *
 * @param origin
 * @return
 */
ptree_or_node *tagsistant_querytree_duplicate_ptree_or_node(ptree_or_node *origin)
{
	if (!origin) return (NULL);

	ptree_or_node *copy = g_new0(ptree_or_node, 1);
	if (!copy) return (NULL);

	copy->next = tagsistant_querytree_duplicate_ptree_or_node(origin->next);
	copy->and_set = tagsistant_querytree_duplicate_ptree_and_node(origin->and_set);

	return (copy);
}

/**
 * Duplicate a querytree, assigning a new connection
 *
 * @param qtree
 * @return
 */
tagsistant_querytree *tagsistant_querytree_duplicate(tagsistant_querytree *qtree)
{
	tagsistant_querytree *duplicated = g_new0(tagsistant_querytree, 1);
	if (!duplicated) return (NULL);

	duplicated->full_path = g_strdup(qtree->full_path);
	duplicated->full_archive_path = g_strdup(qtree->full_archive_path);
	duplicated->archive_path = g_strdup(qtree->archive_path);
	duplicated->object_path = g_strdup(qtree->object_path);
	duplicated->last_tag = g_strdup(qtree->last_tag);
	duplicated->first_tag = g_strdup(qtree->first_tag);
	duplicated->second_tag = g_strdup(qtree->second_tag);
	duplicated->relation = g_strdup(qtree->relation);
	duplicated->stats_path = g_strdup(qtree->stats_path);

	duplicated->inode = qtree->inode;
	duplicated->type = qtree->type;
	duplicated->points_to_object = qtree->points_to_object;
	duplicated->is_taggable = qtree->is_taggable;
	duplicated->is_external = qtree->is_external;
	duplicated->valid = qtree->valid;
	duplicated->complete = qtree->complete;
	duplicated->exists = qtree->exists;
	duplicated->transaction_started = qtree->transaction_started;

	/* duplicate tag tree */
	duplicated->tree = tagsistant_querytree_duplicate_ptree_or_node(qtree->tree);

	/* return the querytree */
	return (duplicated);
}

/**
 * Lookup a querytree from the cache
 *
 * @param path the query path
 * @return a tagsistant_querytree duplicated object
 */
tagsistant_querytree *tagsistant_querytree_lookup(const char *path)
{
	/* lookup the querytree */
	tagsistant_querytree *qtree = NULL;
	g_rw_lock_reader_lock(&tagsistant_querytree_cache_lock);
	qtree = g_hash_table_lookup (tagsistant_querytree_cache, path);
	g_rw_lock_reader_unlock(&tagsistant_querytree_cache_lock);

	/* not found, return and proceed to normal creation */
	if (!qtree) return (NULL);

	/* the querytree is no longer valid, so we destroy it and return NULL */
	struct stat st;
	if (qtree->full_archive_path && (0 != stat(qtree->full_archive_path, &st))) {
		g_rw_lock_reader_lock(&tagsistant_querytree_cache_lock);
		g_hash_table_remove(tagsistant_querytree_cache, path);
		g_rw_lock_reader_unlock(&tagsistant_querytree_cache_lock);

		return (NULL);
	}

	/*
	 * set the last_access_microsecond time
	 * will be used in future code to decide if a cached entry
	 * could be removed for memory management
	 */
	qtree->last_access_microsecond = g_get_real_time();

	/* return a duplicate of the qtree */
	return (tagsistant_querytree_duplicate(qtree));
}

/**
 * Return true if entry->full_path contains qtree->first_tag or qtree->second_tag
 * or both.
 *
 * @param key unused
 * @param entry the querytree object from the cache being evaluated
 * @param qtree the querytree object invalidating the cache
 * @return TRUE if a match is found, FALSE otherwise
 */
gboolean tagsistant_invalidate_querytree_entry(gpointer key_p, gpointer entry_p, gpointer qtree_p)
{
	(void) key_p;
	tagsistant_querytree *entry = (tagsistant_querytree *) entry_p;
	tagsistant_querytree *qtree = (tagsistant_querytree *) qtree_p;

	gboolean matches = FALSE;

	gchar *first_tag = g_strdup_printf("/%s/", qtree->first_tag);
	gchar *second_tag = g_strdup_printf("/%s/", qtree->second_tag);

	if (strstr(entry->full_path, first_tag) || strstr(entry->full_path, second_tag))
		matches = TRUE;

	g_free_null(first_tag);
	g_free_null(second_tag);

	return (matches);
}

/**
 * Delete the cache entries which involve one of the tags qtree->first_tag
 * and qtree->second_tag
 *
 * @param qtree the querytree object which is invalidating the cache
 */
void tagsistant_invalidate_querytree_cache(tagsistant_querytree *qtree)
{
	g_rw_lock_writer_lock(&tagsistant_querytree_cache_lock);
	g_hash_table_foreach_remove(tagsistant_querytree_cache, tagsistant_invalidate_querytree_entry, qtree);
	g_rw_lock_writer_unlock(&tagsistant_querytree_cache_lock);
}
#endif /* TAGSISTANT_ENABLE_QUERYTREE_CACHE */

/**
 * Build query tree from path. A querytree is composed of a linked
 * list of ptree_or_node_t objects. Each or object has a descending
 * linked list of ptree_and_node_t objects. This kind of structure
 * should be considered as a collection (the or horizontal level)
 * of queries (the and vertical lists). Each and list should be
 * resolved using SQLite INTERSECT compound operator. All results
 * should be then merged in a unique list of filenames.
 *
 * @param path the path to be converted in a logical query
 * @param do_reasoning if true, use the reasoner
 */
tagsistant_querytree *tagsistant_querytree_new(const char *path, int do_reasoning, int assign_inode, int start_transaction)
{
	int tagsistant_errno;
	tagsistant_querytree *qtree = NULL;

#if TAGSISTANT_ENABLE_QUERYTREE_CACHE
	/* first look in the cache */
	qtree = tagsistant_querytree_lookup(path);
	if (qtree) {
		/* assign a new connection */
		qtree->dbi = tagsistant_db_connection(start_transaction);
		qtree->transaction_started = start_transaction;
		return (qtree);
	}
#endif

	/* the qtree object has not been found so lets allocate the querytree structure */
	qtree = g_new0(tagsistant_querytree, 1);
	if (qtree == NULL) {
		dbg(LOG_ERR, "Error allocating memory");
		return(NULL);
	}

	/* tie this query to a DBI handle */
	qtree->dbi = tagsistant_db_connection(start_transaction);
	qtree->transaction_started = start_transaction;

	/* duplicate the path inside the struct */
	qtree->full_path = g_strdup(path);

#if TAGSISTANT_VERBOSE_LOGGING
	dbg(LOG_INFO, "Building querytree for %s", qtree->full_path);
#endif

	/* split the path */
	gchar **splitted = g_strsplit(path, "/", 512); /* split up to 512 tokens */
	gchar **token_ptr = splitted + 1; /* first element is always "" since path begins with '/' */

	/* set default values */
	qtree->inode = 0;
	qtree->type = 0;
	qtree->points_to_object = 0;
	qtree->is_taggable = 0;
	qtree->is_external = 0;
	qtree->valid = 0;
	qtree->complete = 0;
	qtree->exists = 0;

	/* guess the type of the query by first token */
	if ('\0' == **token_ptr)							qtree->type = QTYPE_ROOT;
	else if (g_strcmp0(*token_ptr, "tags") == 0)		qtree->type = QTYPE_TAGS;
	else if (g_strcmp0(*token_ptr, "archive") == 0)		qtree->type = QTYPE_ARCHIVE;
	else if (g_strcmp0(*token_ptr, "relations") == 0)	qtree->type = QTYPE_RELATIONS;
	else if (g_strcmp0(*token_ptr, "stats") == 0)		qtree->type = QTYPE_STATS;
//	else if (g_strcmp0(*token_ptr, "retag") == 0)		qtree->type = QTYPE_RETAG;
	else {
		qtree->type = QTYPE_MALFORMED;
		dbg(LOG_ERR, "Malformed or nonexistant path (%s)", path);
		goto RETURN;
	}

	/* then skip first token (used for guessing query type) */
	token_ptr++;

	/* do selective parsing of the query */
	if (QTREE_IS_TAGS(qtree)) {
		if (!tagsistant_querytree_parse_tags(qtree, path, &token_ptr, do_reasoning)) goto RETURN;
	} else if (QTREE_IS_RELATIONS(qtree)) {
		tagsistant_querytree_parse_relations(qtree, &token_ptr);
	} else if (QTREE_IS_STATS(qtree)) {
		tagsistant_querytree_parse_stats(qtree, &token_ptr);
	}

	/* remaining part is the object pathname */
	if (QTREE_IS_ARCHIVE(qtree)) {

		qtree->object_path = g_strjoinv(G_DIR_SEPARATOR_S, token_ptr);
		qtree->inode = tagsistant_inode_extract_from_path(qtree);
		if (!qtree->inode && assign_inode) {
			tagsistant_force_create_and_tag_object(qtree, &tagsistant_errno);
		} else {
			tagsistant_querytree_set_inode(qtree, qtree->inode);
		}

		if (strlen(qtree->object_path) == 0) {
			qtree->archive_path = g_strdup("");
			qtree->full_archive_path = g_strdup(tagsistant.archive);
		}

	} else if (QTREE_IS_TAGS(qtree) && qtree->complete) {

		// get the object path name joining the remaining part of tokens
		qtree->object_path = g_strjoinv(G_DIR_SEPARATOR_S, token_ptr);

		// look for an inode in object_path
		qtree->inode = tagsistant_inode_extract_from_path(qtree);

		// if no inode is found, try to guess it resolving the querytree
		// we just need to check if an object with *token_ptr objectname and
		// a matching or_node->and_set->tag named tag is listed
		if (!qtree->inode) {
			ptree_or_node *or_tmp = qtree->tree;
			while (or_tmp && !qtree->inode && strlen(qtree->object_path)) {
				qtree->inode = tagsistant_guess_inode_from_and_set(or_tmp->and_set, qtree->dbi, *token_ptr);

#if 0
				int and_total = 0;
				gchar *and_set = tagsistant_compile_and_set(or_tmp->and_set, &and_total);

				tagsistant_query(
					"select objects.inode from objects "
						"join tagging on objects.inode = tagging.inode "
						"join tags on tagging.tag_id = tags.tag_id "
						"where tags.tagname in (%s) and objects.objectname = \"%s\" "
						"group by objects.inode "
							"having count(distinct tags.tagname) = %d",
					qtree->dbi,
					tagsistant_return_integer,
					&(qtree->inode),
					and_set,
					*token_ptr,
					and_total
				);

				g_free_null(and_set);
#endif

				or_tmp = or_tmp->next;
			}
		} else {
			//
			// check if the inode found in the object_path refers to an object
			// which is really tagged by the tagset described in the tags/.../=
			// path. a query is valid if the object is tagged in at least one
			// and set
			//
			int valid_query = 0;
			ptree_or_node *or_tmp = qtree->tree;

			while (or_tmp) {
				//
				// check if the object is tagged in this andset
				//
				int valid_and_set = 1;
				ptree_and_node *and_tmp = or_tmp->and_set;

				while (and_tmp) {
					tagsistant_inode tmp_inode;
					tagsistant_query(
						"select tagging.inode from tagging "
							"join tags on tagging.tag_id = tags.tag_id "
							"where tagging.inode = %d and tags.tag_name = \"%s\"",
						qtree->dbi,
						tagsistant_return_integer,
						&tmp_inode,
						qtree->inode,
						and_tmp->tag);

					//
					// if just one tag is not valid, the whole andset is not valid
					//
					if (tmp_inode != qtree->inode) {
						valid_and_set = 0;
						break;
					}

					and_tmp = and_tmp->next;
				}

				//
				// if at least one andset is valid, the whole query is valid
				// and we don't need further checking
				//
				if (valid_and_set) {
					valid_query = 1;
					break;
				}

				or_tmp = or_tmp->next;
			}

			//
			// if the whole query is not valid
			// we should reset the inode to 0
			//
			if (!valid_query)
				tagsistant_querytree_set_inode(qtree, 0);
		}

		// if an inode has been found, form the archive_path and full_archive_path
		if (qtree->inode)
			tagsistant_querytree_set_inode(qtree, qtree->inode);

		if (strlen(qtree->object_path))
			qtree->points_to_object = qtree->valid = qtree->complete = 1;

		// check if the query is consistent or not
		// TODO is this really necessary?
		// Wouldn't be enough to do it explicitly in getattr()?
		tagsistant_querytree_check_tagging_consistency(qtree);
	}

#if TAGSISTANT_VERBOSE_LOGGING
	dbg(LOG_INFO, "inode = %d", qtree->inode);
	dbg(LOG_INFO, "object_path = \"%s\"", qtree->object_path);
	dbg(LOG_INFO, "archive_path = \"%s\"", qtree->archive_path);
	dbg(LOG_INFO, "full_archive_path = \"%s\"", qtree->full_archive_path);
#endif

	/*
	 * guess if query points to an object on disk or not
	 * that's true if object_path property is not null or zero length
	 * and both query is /archive or query is a complete /tags (including = sign)
	 */
	if (
		(QTREE_IS_ARCHIVE(qtree) || (QTREE_IS_TAGS(qtree) && qtree->complete && qtree->object_path))
		&&
		(strlen(qtree->object_path) > 0)
	) {
		qtree->points_to_object = 1;

#if TAGSISTANT_VERBOSE_LOGGING
		if (!qtree->inode)
			dbg(LOG_INFO, "Qtree path %s points to an object but does NOT contain an inode", qtree->full_path);
#endif
	} else {
		qtree->points_to_object = 0;
	}

RETURN:
	g_strfreev(splitted);

#if TAGSISTANT_ENABLE_QUERYTREE_CACHE
	if ((!qtree->points_to_object) || qtree->inode) {
		/* save the querytree in the cache */
		tagsistant_querytree *duplicated = tagsistant_querytree_duplicate(qtree);

		g_rw_lock_writer_lock(&tagsistant_querytree_cache_lock);
		g_hash_table_insert(tagsistant_querytree_cache, duplicated->full_path, duplicated);
		g_rw_lock_writer_unlock(&tagsistant_querytree_cache_lock);
	}
#endif

	return(qtree);
}

/**
 * deduplication function called by tagsistant_calculate_object_checksum
 *
 * @param inode the object inode
 * @param hex the checksum string
 * @param dbi DBI connection handle
 */
void tagsistant_querytree_find_duplicates(tagsistant_querytree *qtree, gchar *hex)
{
	tagsistant_inode main_inode = 0;

	/* get the first inode matching the checksum */
	tagsistant_query(
		"select inode from objects where checksum = \"%s\" order by inode limit 1",
		qtree->dbi,	tagsistant_return_integer, &main_inode,	hex);

	/* if we have just one file, we can return */
	if (qtree->inode == main_inode) return;

	dbg(LOG_INFO, "Deduplicating %s: %d -> %d", qtree->full_archive_path, qtree->inode, main_inode);

	/* first move all the tags of inode to main_inode */
	tagsistant_query(
		"update tagging set inode = %d where inode = %d",
		qtree->dbi,	NULL, NULL,	main_inode,	qtree->inode);

	/* then delete records left because of duplicates in key(inode, tag_id) in the tagging table */
	tagsistant_query(
		"delete from tagging where inode = %d",
		qtree->dbi,	NULL, NULL,	qtree->inode);

	/* and finally unlink the removable inode */
	tagsistant_query(
		"delete from objects where inode = %d",
		qtree->dbi, NULL, NULL,	qtree->inode);
}

/**
 * Deduplicate the object pointed by the querytree
 *
 * @param qtree the querytree object
 */
void tagsistant_querytree_deduplicate(tagsistant_querytree *qtree)
{
	/* guess if the object is a file or a symlink */
	struct stat buf;
	if ((-1 == lstat(qtree->full_archive_path, &buf)) || (!S_ISREG(buf.st_mode) && !S_ISLNK(buf.st_mode)))
		return;

	dbg(LOG_INFO, "Checksumming %s", qtree->full_archive_path);

	/* open the file and read its content */
	int fd = open(qtree->full_archive_path, O_RDONLY|O_NOATIME);
	if (-1 != fd) {
		GChecksum *checksum = g_checksum_new(G_CHECKSUM_SHA1);
		guchar *buffer = g_new0(guchar, 65535);

		if (checksum && buffer) {
			/* feed the checksum object */
			do {
				int length = read(fd, buffer, 65535);
				if (length > 0)
					g_checksum_update(checksum, buffer, length);
				else
					break;
			} while (1);

			/* get the hexadecimal checksum string */
			gchar *hex = g_strdup(g_checksum_get_string(checksum));

			/* destroy the checksum object */
			g_checksum_free(checksum);
			g_free_null(buffer);

			/* save the string into the objects table */
			tagsistant_query(
				"update objects set checksum = '%s' where inode = %d;",
				qtree->dbi, NULL, NULL, hex, qtree->inode);

			/* look for duplicated objects */
			tagsistant_querytree_find_duplicates(qtree, hex);

			/* free the hex checksum string */
			g_free_null(hex);
		}
		close(fd);
	}
}

/**
 * destroy a tagsistant_querytree_t structure
 *
 * @param qtree the tagsistant_querytree_t to be destroyed
 */
void tagsistant_querytree_destroy(tagsistant_querytree *qtree, uint commit_transaction)
{
	if (!qtree) return;

	if (qtree->transaction_started) {
		if (commit_transaction)
			tagsistant_commit_transaction(qtree->dbi);
		else
			tagsistant_rollback_transaction(qtree->dbi);
	}

	/* mark the connection as available */
	tagsistant_db_connection_release(qtree->dbi);

	g_free_null(qtree->full_path);
	g_free_null(qtree->object_path);
	g_free_null(qtree->archive_path);
	g_free_null(qtree->full_archive_path);

	if (QTREE_IS_TAGS(qtree)) {
		ptree_or_node *node = qtree->tree;
		while (node != NULL) {

			ptree_and_node *tag = node->and_set;
			while (tag != NULL) {

				// walk related tags
				while (tag->related) {
					ptree_and_node *related = tag->related;
					tag->related = related->related;
					g_free_null(related->tag);
					g_free_null(related);
				}

				// free the ptree_and_node_t node
				ptree_and_node *next = tag->next;
				g_free_null(tag->tag);
				g_free_null(tag);
				tag = next;
			}

			// free the ptree_or_node_t node
			ptree_or_node *next = node->next;
			g_free_null(node);
			node = next;
		}
		g_free_null(qtree->last_tag);
	} else if (QTREE_IS_RELATIONS(qtree)) {
		g_free_null(qtree->first_tag);
		g_free_null(qtree->second_tag);
		g_free_null(qtree->relation);
	} else if (QTREE_IS_STATS(qtree)) {
		g_free_null(qtree->stats_path);
	}

	// free the structure
	g_free_null(qtree);
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

	fh->name = name;
	fh->inode = inode;

	/* add the new element */
	// TODO valgrind says: check for leaks
	g_hash_table_insert(hash_table, name, g_list_prepend(list, fh));

//	dbg(LOG_INFO, "adding (%d,%s) to filetree", fh->inode, fh->name);

	return (0);
}

void tagsistant_filetree_destroy_value_list(gpointer list_pointer);

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
		dbg(LOG_ERR, "NULL path_tree_t object provided to %s", __func__);
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

		// TODO valgrind says: check for leaks
		GString *statement = g_string_sized_new(10240);
		g_string_printf(statement, "create view tv%.16" PRIxPTR " as ", (uintptr_t) query);
		
		while (tag != NULL) {
			if (tagsistant.sql_backend_have_intersect) {
				g_string_append(statement, "select objectname, objects.inode as inode from objects join tagging on tagging.inode = objects.inode join tags on tags.tag_id = tagging.tag_id where tagname = \"");
				g_string_append(statement, tag->tag);
				g_string_append(statement, "\"");
			} else {
				if (nesting) {
					g_string_append(statement, " and objects.inode in (select distinct objects.inode from objects inner join tagging on tagging.inode = objects.inode inner join tags on tagging.tag_id = tags.tag_id where tagname = '");
				} else {
					g_string_append(statement, " select distinct objectname, objects.inode as inode from objects inner join tagging on tagging.inode = objects.inode inner join tags on tagging.tag_id = tags.tag_id where tagname = '");
				}
				g_string_append(statement, tag->tag);
				g_string_append(statement, "'");	
				nesting++;
			}
			
			/* add related tags */
			if (tag->related != NULL) {
				ptree_and_node *related = tag->related;
				while (related != NULL) {
					g_string_append(statement, " or tagname = \"");
					g_string_append(statement, related->tag);
					g_string_append(statement, "\"");
					related = related->related;
				}
			}

			if (tagsistant.sql_backend_have_intersect && (tag->next != NULL))
				g_string_append(statement, " intersect ");

			tag = tag->next;
		}

		if (!tagsistant.sql_backend_have_intersect) {
			nesting--; // we need one closed parenthesis less than nested subquery
			while (nesting) {
				g_string_append(statement, ")");
				nesting--;
			}
		}

#if TAGSISTANT_VERBOSE_LOGGING
		dbg(LOG_INFO, "SQL: filetree query [%s]", statement->str);
#endif

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

#if TAGSISTANT_VERBOSE_LOGGING
	dbg(LOG_INFO, "SQL view query [%s]", view_statement->str);
#endif

	/* apply view statement */
	GHashTable *file_hash = g_hash_table_new_full(
		g_str_hash,
		g_str_equal,
		NULL,
		(GDestroyNotify) tagsistant_filetree_destroy_value_list);

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
void tagsistant_filetree_destroy_value(gpointer fh_p)
{
	if (!fh_p) return;

	tagsistant_file_handle *fh = (tagsistant_file_handle *) fh_p;
	g_free_null(fh->name);
	g_free_null(fh);
}

/**
 * Destroy a filetree element GList list of tagsistant_file_handle.
 * This will free the GList data structure by first calling
 * tagsistant_filetree_destroy_value() on each linked node.
 */
void tagsistant_filetree_destroy_value_list(gpointer list_p)
{
	if (!list_p) return;

	GList *list = (GList *) list_p;
	g_list_free_full(list, tagsistant_filetree_destroy_value);
}
