/*
   Tagsistant (tagfs) -- path_resolution.c
   Copyright (C) 2006-2013 Tx0 <tx0@strumentiresistenti.org>

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
GRWLock tagsistant_and_set_cache_lock;
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

#if TAGSISTANT_ENABLE_QUERYTREE_CACHE
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
#endif /* TAGSISTANT_ENABLE_QUERYTREE_CACHE */

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
	tagsistant_querytree_types[QTYPE_STORE] = g_strdup("QTYPE_STORE");

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

/*
 * TODO: The algorithm implemented by this function is very ugly.
 * There must be a better way to express it.
 */
int tagsistant_and_set_includes_and_set(GList *master_list, GList *comparable_list)
{
	/*
	 * If a list is compared to itself, we return 0 to avoid
	 * deleting every list in the main GList
	 */
	if (master_list == comparable_list) return (0);

	/*
	 * Loop through the lists to check if ALL the elements in the
	 * master list are contained in the comparable list
	 */
	GList *master_ptr = master_list;
MASTER:
	while (master_ptr) {
		GList *comparable_ptr = comparable_list;
		while (comparable_ptr) {
			/* if we find a match, break the inner loop */
			if (strcmp(master_ptr->data, comparable_ptr->data) == 0) {
				master_ptr = master_ptr->next;
				goto MASTER;
			}
			comparable_ptr = comparable_ptr->next;
		}
		return (1);
	}

	return (0);
}

/**
 * Given a linked list of ptree_and_node objects (called an and-set)
 * return a string with a comma separated list of all the tags.
 *
 * @param and_set the linked and-set list
 * @return a string like "tag1, tag2, tag3"
 */
gchar *tagsistant_compile_grouped_and_set(ptree_and_node *and_set, int *total)
{
	*total = 0;

	/*
	 * 1. scan all the tags and build a GList of ordered GList.
	 *    Every second-level GList will hold the entire set of
	 *    tags contained in an and_set node, ordered by name. So
	 *    if and_set contains {t2, t1, t5, t3, t2, t1}, the
	 *    corresponding GList will contain {t1, t2, t3, t5} in
	 *    this order.
	 */
	ptree_and_node *and_set_ptr = and_set;
	GList *tags_list = NULL;

	while (and_set_ptr) {
		/* init a GList with the main tag of this and node */
		GList *this_tag = g_list_append(NULL, and_set_ptr->tag);

		/* scan related tags */
		ptree_and_node *related = and_set_ptr->related;
		while (related) {
			/* if the tag is not present in the list, add it */
			if (g_list_find_custom(this_tag, related->tag, (GCompareFunc) strcmp) == NULL) {
				this_tag = g_list_insert_sorted(this_tag, related->tag, (GCompareFunc) strcmp);
			}

			related = related->related;
		}

		/* add the sub-GList (the one with the list of tags) to the main GList */
		tags_list = g_list_prepend(tags_list, this_tag);

		and_set_ptr = and_set_ptr->next;
	}

	/*
	 * 2. scan all the sets; every one that turns out to be a subset of
	 *    another will be discarded
	 */
	GList *tags_list_ptr = tags_list;

	while (tags_list_ptr) {
		GList *and_tags_subptr = tags_list;
		while (and_tags_subptr) {
			if (tagsistant_and_set_includes_and_set(tags_list_ptr, and_tags_subptr)) {
				tags_list = g_list_remove_link(tags_list_ptr, and_tags_subptr);
				g_list_free(and_tags_subptr);
			}
			and_tags_subptr = and_tags_subptr->next;
		}
		tags_list_ptr = tags_list_ptr->next;
	}

	/*
	 * 3. scan tags_list again and format the query string
	 */
	GString *and_condition = g_string_sized_new(10240);
	tags_list_ptr = tags_list;

	while (tags_list_ptr) {
		GList *this_tag = (GList *) tags_list_ptr->data;

		/* if the string already contains data, chain them with an "or" */
		if (*total > 0) g_string_append(and_condition, " or ");

		/* append the new sub-clause, putting the and tag */
		g_string_append(and_condition, "tags.tagname in (");
		int tags_total = 0;
		while (this_tag) {
			/* if the tag is not the first, chain with a comma and a space */
			if (tags_total > 0) g_string_append(and_condition, ", ");

			/* add the tag */
			g_string_append_printf(and_condition, "\"%s\"", (gchar *) this_tag->data);

			/* increment tags count */
			tags_total++;

			/* go further */
			this_tag = this_tag->next;
		}

		/* close the and sub-clause and free the corresponding sub-GList */
		g_string_append(and_condition, ")");
		g_list_free(this_tag);

		/* increment the tag counter */
		*total += 1;

		tags_list_ptr = tags_list_ptr->next;
	}

	/* free the GList object */
	g_list_free(tags_list);

	/* dispose and_condition, but don't free its content, which is returned */
	return (g_string_free(and_condition, FALSE));
}

/**
 * Invalidate a tagsistant_and_set_cache entry
 *
 * @param objectname the key of the object to be invalidated
 */
void tagsistant_invalidate_and_set_cache_entries(tagsistant_querytree *qtree)
{
#if TAGSISTANT_ENABLE_AND_SET_CACHE
	ptree_or_node *ptr = qtree->tree;
	while (ptr) {
		int tags_total = 0;

		// compute the key
		gchar *and_set_string = tagsistant_compile_and_set(ptr->and_set, &tags_total);
		gchar *search_key = g_strdup_printf("%s:%s", qtree->object_path, and_set_string);

		// if lookup succeed, removes the entry
		g_rw_lock_writer_lock(&tagsistant_and_set_cache_lock);
		if (g_hash_table_remove(tagsistant_and_set_cache, search_key)) {
			dbg('F', LOG_INFO, "Cache entry %s invalidated", search_key);
		} else {
			dbg('F', LOG_INFO, "Cache entry %s not found!", search_key);
		}
		g_rw_lock_writer_unlock(&tagsistant_and_set_cache_lock);

		// free memory
		g_free(search_key);
		g_free(and_set_string);

		// next or node
		ptr = ptr->next;
	}
#endif
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

#if TAGSISTANT_ENABLE_AND_SET_CACHE
	int tags_total = 0;

	gchar *and_set_string = tagsistant_compile_and_set(and_set, &tags_total);
	gchar *search_key = g_strdup_printf("%s:%s", objectname, and_set_string);

	// if lookup succeed, returns the inode
	g_rw_lock_reader_lock(&tagsistant_and_set_cache_lock);
	tagsistant_inode *value = (tagsistant_inode *) g_hash_table_lookup(tagsistant_and_set_cache, search_key);
	g_rw_lock_reader_unlock(&tagsistant_and_set_cache_lock);

	if (value && *value) {
		g_free_null(search_key);
		g_free_null(and_set_string);
		return (*value);
	}
#endif

	int grouped_tags_total = 0;
	gchar *and_condition = tagsistant_compile_grouped_and_set(and_set, &grouped_tags_total);

	// lookup the inode in the SQL db
	tagsistant_query(
		"select objects.inode from objects "
			"join tagging on objects.inode = tagging.inode "
			"join tags on tagging.tag_id = tags.tag_id "
			"where (%s) and objects.objectname = \"%s\" "
			"group by objects.inode "
			"having count(*) >= %d "
			,
		dbi,
		tagsistant_return_integer,
		&inode,
		and_condition,
		objectname,
		grouped_tags_total
	);

	g_free(and_condition);

#if TAGSISTANT_ENABLE_AND_SET_CACHE
	if (inode) {
		tagsistant_inode *value = g_new(tagsistant_inode, 1);
		*value = inode;
		g_rw_lock_writer_lock(&tagsistant_and_set_cache_lock);
		g_hash_table_insert(tagsistant_and_set_cache, search_key, value);
		g_rw_lock_writer_lock(&tagsistant_and_set_cache_lock);
	} else {
		g_free_null(search_key);
	}

	g_free_null(and_set_string);
#endif

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
int tagsistant_querytree_parse_store (
		tagsistant_querytree *qtree,
		const char *path,
		gchar ***token_ptr)
{
	unsigned int orcount = 0, andcount = 0;

	// initialize iterator variables on query tree nodes
	ptree_or_node *last_or = qtree->tree = g_new0(ptree_or_node, 1);
	if (qtree->tree == NULL) {
		tagsistant_querytree_destroy(qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
		dbg('q', LOG_ERR, "Error allocating memory");
		return (0);
	}

	ptree_and_node *last_and = NULL;

	// state if the query is complete or not
	size_t path_length = strlen(path);
	if (g_strstr_len(path, path_length, "/" TAGSISTANT_QUERY_DELIMITER)) {
		qtree->complete = 1;
		qtree->do_reasoning = g_strstr_len(path, path_length, "/" TAGSISTANT_QUERY_DELIMITER_NO_REASONING) ? 0 : 1;
	}
	dbg('q', LOG_INFO, "Path %s is %scomplete", path, qtree->complete ? "" : "not ");

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
				dbg('q', LOG_ERR, "Error allocating memory");
				return (0);
			}
			last_or->next = new_or;
			last_or = new_or;
			last_and = NULL;
		} else {
			/* save next token in new ptree_and_node_t slot */
			ptree_and_node *and = g_new0(ptree_and_node, 1);
			if (and == NULL) {
				dbg('q', LOG_ERR, "Error allocating memory");
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

			dbg('q', LOG_INFO, "Query tree nodes %.2d.%.2d %s", orcount, andcount, **token_ptr);
			andcount++;

			/* search related tags */
			if (qtree->do_reasoning) {
				dbg('q', LOG_INFO, "Searching for other tags related to %s", and->tag);

				tagsistant_reasoning *reasoning = g_malloc(sizeof(tagsistant_reasoning));
				if (reasoning != NULL) {
					reasoning->start_node = and;
					reasoning->current_node = and;
					reasoning->added_tags = 0;
					reasoning->conn = qtree->dbi;
					int newtags = tagsistant_reasoner(reasoning);
					dbg('q', LOG_INFO, "Reasoning added %d tags", newtags);
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
int tagsistant_querytree_parse_tags (
	tagsistant_querytree* qtree,
	gchar ***token_ptr)
{
	/* parse a relations query */
	if (NULL != **token_ptr) {
		qtree->first_tag = g_strdup(**token_ptr);
		(*token_ptr)++;
		if (NULL != **token_ptr) {
			qtree->second_tag = g_strdup(**token_ptr);
		}
	}

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
 * @param assign_inode if the path has not an associated inode, assign one (used in mknod() and such)
 * @param start_transaction do a "start transaction" on the DB connection
 * @return a tagsistant_querytree object
 */
tagsistant_querytree *tagsistant_querytree_new(const char *path, int assign_inode, int start_transaction, int provide_connection)
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
		dbg('q', LOG_ERR, "Error allocating memory");
		return(NULL);
	}

	/* tie this query to a DBI handle */
	if (provide_connection) {
		qtree->dbi = tagsistant_db_connection(start_transaction);
		qtree->transaction_started = start_transaction;
	} else {
		qtree->dbi = NULL;
	}

	/* duplicate the path inside the struct */
	qtree->full_path = g_strdup(path);

	dbg('q', LOG_INFO, "Building querytree for %s", qtree->full_path);

	/* split the path */
	gchar **splitted = g_strsplit(path, "/", 512); /* split up to 512 tokens */
	gchar **token_ptr = splitted + 1; /* first element is always "" since path begins with '/' */

	/*
	 * set default values
	 * TODO: can this be avoided since we use g_new0() to create the struct?
	 */
	qtree->inode = 0;
	qtree->type = 0;
	qtree->points_to_object = 0;
	qtree->is_taggable = 0;
	qtree->is_external = 0;
	qtree->valid = 0;
	qtree->complete = 0;
	qtree->exists = 0;
	qtree->do_reasoning = 0;
	qtree->schedule_for_unlink = 0;

	/* guess the type of the query by first token */
	if ('\0' == **token_ptr)							qtree->type = QTYPE_ROOT;
	else if (g_strcmp0(*token_ptr, "tags") == 0)		qtree->type = QTYPE_TAGS;
	else if (g_strcmp0(*token_ptr, "archive") == 0)		qtree->type = QTYPE_ARCHIVE;
	else if (g_strcmp0(*token_ptr, "relations") == 0)	qtree->type = QTYPE_RELATIONS;
	else if (g_strcmp0(*token_ptr, "stats") == 0)		qtree->type = QTYPE_STATS;
	else if (g_strcmp0(*token_ptr, "store") == 0)		qtree->type = QTYPE_STORE;
//	else if (g_strcmp0(*token_ptr, "retag") == 0)		qtree->type = QTYPE_RETAG;
	else {
		qtree->type = QTYPE_MALFORMED;
		dbg('q', LOG_ERR, "Malformed or nonexistant path (%s)", path);
		goto RETURN;
	}

	/* then skip first token (used for guessing query type) */
	token_ptr++;

	/* do selective parsing of the query */
	if (QTREE_IS_STORE(qtree)) {
		if (!tagsistant_querytree_parse_store(qtree, path, &token_ptr)) goto RETURN;
	} else if (QTREE_IS_TAGS(qtree)) {
		if (!tagsistant_querytree_parse_tags(qtree, &token_ptr)) goto RETURN;
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

	} else if (QTREE_IS_STORE(qtree) && qtree->complete) {

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

		/*
		 * checking if the query is consistent or not has been
		 * moved where the check matters:
		 *
		 *   flushc(), getattr(), link(), mkdir(), mknod(), open(),
		 *   release(), rename(), rmdir(), symlink(), unlink()
		 *
		 * Despite it may seems odd to place an explicit call on all
		 * that methods, the main reason to move this call outside of
		 * here is to avoid it in highly frequent calls, namely read()
		 * and write()! The performance improvement is huge, especially
		 * when reading and writing big files.
		 */
		// tagsistant_querytree_check_tagging_consistency(qtree);
	}

	dbg('q', LOG_INFO, "inode = %d", qtree->inode);
	dbg('q', LOG_INFO, "object_path = \"%s\"", qtree->object_path);
	dbg('q', LOG_INFO, "archive_path = \"%s\"", qtree->archive_path);
	dbg('q', LOG_INFO, "full_archive_path = \"%s\"", qtree->full_archive_path);

	/*
	 * guess if query points to an object on disk or not
	 * that's true if object_path property is not null or zero length
	 * and both query is /archive or query is a complete /tags (including = sign)
	 */
	if (
		(QTREE_IS_ARCHIVE(qtree) || (QTREE_IS_STORE(qtree) && qtree->complete && qtree->object_path))
		&&
		(strlen(qtree->object_path) > 0)
	) {
		qtree->points_to_object = 1;

		if (!qtree->inode)
			dbg('q', LOG_INFO, "Qtree path %s points to an object but does NOT contain an inode", qtree->full_path);
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
 * destroy a tagsistant_querytree_t structure
 *
 * @param qtree the tagsistant_querytree_t to be destroyed
 */
void tagsistant_querytree_destroy(tagsistant_querytree *qtree, guint commit_transaction)
{
	if (!qtree) return;

	/* if scheduled for unlink, unlink it */
	if (qtree->schedule_for_unlink)
		unlink(qtree->full_archive_path);

	/* commit the transaction, if any, and mark the connection as available */
	if (qtree->dbi) {
		if (qtree->transaction_started) {
			if (commit_transaction)
				tagsistant_commit_transaction(qtree->dbi);
			else
				tagsistant_rollback_transaction(qtree->dbi);
		}

		tagsistant_db_connection_release(qtree->dbi);
	}

	/* free the paths */
	g_free_null(qtree->full_path);
	g_free_null(qtree->object_path);
	g_free_null(qtree->archive_path);
	g_free_null(qtree->full_archive_path);

	if (QTREE_IS_STORE(qtree)) {
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
			GString *tag_and_related = g_string_sized_new(1024);
			g_string_printf(tag_and_related, "\"%s\"", tag->tag);
			if (tag->related != NULL) {
				ptree_and_node *related = tag->related;
				while (related != NULL) {
					g_string_append_printf(tag_and_related, ", \"%s\"", related->tag);
					related = related->related;
				}
			}

			if (tagsistant.sql_backend_have_intersect) {

				/* shorter syntax for SQL dialects that provide INTERSECT */
				g_string_append_printf(statement,
					"select objectname, objects.inode as inode "
						"from objects "
						"join tagging on tagging.inode = objects.inode "
						"join tags on tags.tag_id = tagging.tag_id "
						"where tagname in (%s) ", tag_and_related->str);

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
								"where tagname in (%s) ", tag_and_related->str);
				} else {
					g_string_append_printf(statement,
						" select distinct objectname, objects.inode as inode from objects "
							"inner join tagging on tagging.inode = objects.inode "
							"inner join tags on tagging.tag_id = tags.tag_id "
							"where tagname in (%s) ", tag_and_related->str);
				}

				/* increment nesting counter, used to match parenthesis later */
				nesting++;
			}

			g_string_free(tag_and_related, TRUE);
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
void tagsistant_filetree_destroy_value(tagsistant_file_handle *fh)
{
	g_free_null(fh);
}

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
