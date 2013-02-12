/*
   Tagsistant (tagfs) -- path_resolution.h
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
tagsistant_querytree *tagsistant_querytree_new(const char *path, int do_reasoning, int assign_inode)
{
	int tagsistant_errno;

	/* allocate the querytree structure */
	tagsistant_querytree *qtree = g_new0(tagsistant_querytree, 1);
	if (qtree == NULL) {
		dbg(LOG_ERR, "Error allocating memory");
		return(NULL);
	}

	/* set default values */
	qtree->inode = qtree->type = qtree->points_to_object = qtree->is_taggable =
		qtree->is_external = qtree->valid = qtree->complete = qtree->exists = 0;


	/* duplicate the path inside the struct */
	qtree->full_path = g_strdup(path);

	dbg(LOG_INFO, "Building querytree for %s", qtree->full_path);

	/* split the path */
	gchar **splitted = g_strsplit(path, "/", 512); /* split up to 512 tokens */
	gchar **token_ptr = splitted + 1; /* first element is always "" since path begins with '/' */

	/* guess the type of the query by first token */
	qtree->type = tagsistant_querytree_guess_type(token_ptr);
	if (QTYPE_MALFORMED == qtree->type) {
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
			while (or_tmp && !qtree->inode) {
				GString *and_set = g_string_new("");
				ptree_and_node *and_tmp = or_tmp->and_set;

				while (and_tmp) {
					g_string_append_printf(and_set, "\"%s\"", and_tmp->tag);
					if (and_tmp->next) g_string_append(and_set, ",");
					and_tmp = and_tmp->next;
				}

				tagsistant_query(
					"select objects.inode from objects "
						"join tagging on objects.inode = tagging.inode "
						"join tags on tagging.tag_id = tags.tag_id "
						"where tags.tagname in (%s) and objects.objectname = \"%s\"",
					tagsistant_return_integer,
					&(qtree->inode),
					and_set->str,
					*token_ptr
				);

				g_string_free(and_set, TRUE); // destroy the GString and its content
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
		tagsistant_querytree_check_tagging_consistency(qtree);
	}

	dbg(LOG_INFO, "inode = %d", qtree->inode);
	dbg(LOG_INFO, "object_path = \"%s\"", qtree->object_path);
	dbg(LOG_INFO, "archive_path = \"%s\"", qtree->archive_path);
	dbg(LOG_INFO, "full_archive_path = \"%s\"", qtree->full_archive_path);

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
		if (!qtree->inode) dbg(LOG_INFO, "Qtree path %s points to an object but does NOT contain an inode", qtree->full_path);
	} else {
		qtree->points_to_object = 0;
	}

	/* get the id of the object referred by first element */
//	if (!qtree->inode) qtree->inode = tagsistant_inode_extract_from_path(path);

RETURN:
	g_strfreev(splitted);
	return(qtree);
}

/**
 * guess the type of a query
 *
 * @param token_ptr the list of tokenized path
 * @return the query type as listed in tagsistant_query_type struc
 */
tagsistant_query_type tagsistant_querytree_guess_type(gchar **token_ptr)
{
	if (g_strcmp0(*token_ptr, "tags") == 0) {
		return QTYPE_TAGS;
	} else if (g_strcmp0(*token_ptr, "archive") == 0) {
		return QTYPE_ARCHIVE;
	} else if (g_strcmp0(*token_ptr, "relations") == 0) {
		return QTYPE_RELATIONS;
	} else if (g_strcmp0(*token_ptr, "stats") == 0) {
		return QTYPE_STATS;
	} else if (g_strcmp0(*token_ptr, "retag") == 0) {
		return QTYPE_RETAG;
	} else if ((NULL == *token_ptr) || (g_strcmp0(*token_ptr, "") == 0 || (g_strcmp0(*token_ptr, "/") == 0))) {
		return QTYPE_ROOT;
	} else {
		return QTYPE_MALFORMED;
	}
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
	if (!qtree->object_path) return 0;

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
		GString *and_set = g_string_new("");
		ptree_and_node *and_tmp = or_tmp->and_set;

		while (and_tmp) {
			g_string_append_printf(and_set, "\"%s\"", and_tmp->tag);
			if (and_tmp->next) g_string_append(and_set, ",");
			and_tmp = and_tmp->next;
		}

		tagsistant_query(
			"select objects.inode from objects "
				"join tagging on objects.inode = tagging.inode "
				"join tags on tagging.tag_id = tags.tag_id "
				"where tags.tagname in (%s) and objects.objectname = \"%s\"",
			tagsistant_return_integer,
			&inode,
			and_set->str,
			object_first_element
		);

		g_string_free(and_set, TRUE); // destroy the GString and its content

		if (inode) {
			qtree->exists = 1;
			break;
		}

		or_tmp = or_tmp->next;
	}

	g_free(object_first_element);

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
		tagsistant_querytree_destroy(qtree);
		dbg(LOG_ERR, "Error allocating memory");
		return 0;
	}

	ptree_and_node *last_and = NULL;

	// state if the query is complete or not
	qtree->complete = (NULL == g_strstr_len(path, strlen(path), "=")) ? 0 : 1;
	dbg(LOG_INFO, "Path %s is %scomplete", path, qtree->complete ? "" : "not ");

	// by default a query is valid until something wrong happens while parsing it
	qtree->valid = 1;

	// begin parsing
	while (**token_ptr && ('=' != ***token_ptr)) {
		if (strlen(**token_ptr) == 0) {
			/* ignore zero length tokens */
		} else if (strcmp(**token_ptr, "+") == 0) {
			/* open new entry in OR level */
			orcount++;
			andcount = 0;
			ptree_or_node *new_or = g_new0(ptree_or_node, 1);
			if (new_or == NULL) {
				dbg(LOG_ERR, "Error allocating memory");
				return 0;
			}
			last_or->next = new_or;
			last_or = new_or;
			last_and = NULL;
			dbg(LOG_INFO, "Allocated new OR node...");
		} else {
			/* save next token in new ptree_and_node_t slot */
			ptree_and_node *and = g_new0(ptree_and_node, 1);
			if (and == NULL) {
				dbg(LOG_ERR, "Error allocating memory");
				return 0;
			}
			and->tag = g_strdup(**token_ptr);
			// dbg(LOG_INFO, "New AND node allocated on tag %s...", and->tag);
			and->next = NULL;
			and->related = NULL;
			if (last_and == NULL) {
				last_or->and_set = and;
			} else {
				last_and->next = and;
			}
			last_and = and;

			dbg(LOG_INFO, "Query tree nodes %.2d.%.2d %s", orcount, andcount, **token_ptr);
			andcount++;

			/* search related tags */
			if (do_reasoning) {
				dbg(LOG_INFO, "Searching for other tags related to %s", and->tag);

				tagsistant_reasoning *reasoning = g_malloc(sizeof(tagsistant_reasoning));
				if (reasoning != NULL) {
					reasoning->start_node = and;
					reasoning->current_node = and;
					reasoning->added_tags = 0;
					int newtags = tagsistant_reasoner(reasoning);
					dbg(LOG_INFO, "Reasoning added %d tags", newtags);
				}
			}
		}

		// save last tag found
		freenull(qtree->last_tag);
		qtree->last_tag = g_strdup(**token_ptr);

		(*token_ptr)++;
	}

	// if last token is '=', move the pointer one element forward
	if (**token_ptr && ('=' == ***token_ptr))
		(*token_ptr)++;

	return 1;
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

	return 1;
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

	return 1;
}

/**
 * return(querytree type as a printable string.)
 * the string MUST NOT be freed
 */
gchar *tagsistant_querytree_type(tagsistant_querytree *qtree)
{
	static int initialized = 0;
	static gchar *tagsistant_querytree_types[QTYPE_TOTAL];

	if (!initialized) {
		tagsistant_querytree_types[QTYPE_MALFORMED] = g_strdup("QTYPE_MALFORMED");
		tagsistant_querytree_types[QTYPE_ROOT] = g_strdup("QTYPE_ROOT");
		tagsistant_querytree_types[QTYPE_ARCHIVE] = g_strdup("QTYPE_ARCHIVE");
		tagsistant_querytree_types[QTYPE_TAGS] = g_strdup("QTYPE_TAGS");
		tagsistant_querytree_types[QTYPE_RELATIONS] = g_strdup("QTYPE_RELATIONS");
		tagsistant_querytree_types[QTYPE_STATS] = g_strdup("QTYPE_STATS");
		initialized = 1;
	}

	return(tagsistant_querytree_types[qtree->type]);
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

	if (qtree->object_path) g_free(qtree->object_path);
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

	if (qtree->archive_path) g_free(qtree->archive_path);
	qtree->archive_path = g_strdup_printf("%d" TAGSISTANT_INODE_DELIMITER "%s", qtree->inode, qtree->object_path);

	if (qtree->full_archive_path) g_free(qtree->full_archive_path);
	qtree->full_archive_path = g_strdup_printf("%s%s", tagsistant.archive, qtree->archive_path);
}

/**
 * destroy a tagsistant_querytree_t structure
 *
 * @param qtree the tagsistant_querytree_t to be destroyed
 */
void tagsistant_querytree_destroy(tagsistant_querytree *qtree)
{
	if (NULL == qtree) return;

	// destroy the tree
	ptree_or_node *node = qtree->tree;
	while (node != NULL) {
		ptree_and_node *tag = node->and_set;
		while (tag != NULL) {
			// walk related tags
			while (tag->related != NULL) {
				ptree_and_node *related = tag->related;
				tag->related = related->related;
				freenull(related->tag);
				freenull(related);
			}

			// free the ptree_and_node_t node
			ptree_and_node *next = tag->next;
			freenull(tag->tag);
			freenull(tag);
			tag = next;
		}
		// free the ptree_or_node_t node
		ptree_or_node *next = node->next;
		freenull(node);
		node = next;
	}

	// free paths and other strings
	freenull(qtree->full_path);
	freenull(qtree->object_path);
	freenull(qtree->archive_path);
	freenull(qtree->full_archive_path);
	freenull(qtree->first_tag);
	freenull(qtree->second_tag);
	freenull(qtree->relation);
	freenull(qtree->stats_path);
	freenull(qtree->last_tag);

	// free the structure
	freenull(qtree);
}

/************************************************************************************/
/***                                                                              ***/
/*** Reasoner                                                                     ***/
/***                                                                              ***/
/************************************************************************************/

/**
 * SQL callback. Add new tag derived from reasoning to a ptree_and_node_t structure.
 *
 * @param _reasoning pointer to be casted to reasoning_t* structure
 * @param result dbi_result pointer
 * @return 0 always, due to SQLite policy, may change in the future
 */
static int tagsistant_add_reasoned_tag(void *_reasoning, dbi_result result)
{
	/* point to a reasoning_t structure */
	tagsistant_reasoning *reasoning = (tagsistant_reasoning *) _reasoning;
	assert(reasoning != NULL);
	assert(reasoning->start_node != NULL);
	assert(reasoning->current_node != NULL);
	assert(reasoning->added_tags >= 0);

	const char *t1 = dbi_result_get_string_idx(result, 1);
	const char *rel = dbi_result_get_string_idx(result, 2);
	const char *t2 = dbi_result_get_string_idx(result, 3);

	ptree_and_node *and = reasoning->start_node;
	while (and->related != NULL) {
		assert(and->tag != NULL);
		if (strcmp(and->tag, t1) == 0) {
			/* tag is already present, avoid looping */
			return(0);
		}
		and = and->related;
	}

	/* adding tag */
	and->related = g_malloc(sizeof(ptree_and_node));
	if (and->related == NULL) {
		dbg(LOG_ERR, "Error allocating memory");
		return(1);
	}

	and->related->next = NULL;
	and->related->related = NULL;
	and->related->tag = g_strdup(t1);

	assert(and != NULL);
	assert(and->related != NULL);
	assert(and->related->tag != NULL);

	reasoning->added_tags += 1;

	dbg(LOG_INFO, "Adding related tag %s (because %s %s)", and->related->tag, rel, t2);
	return(0);
}

/**
 * Search and add related tags to a ptree_and_node_t,
 * enabling tagsistant_build_filetree to later add more criteria to SQL
 * statements to retrieve files
 *
 * @param reasoning the reasoning structure the tagsistant_reasoner should work on
 * @return number of tags added
 */
int tagsistant_reasoner(tagsistant_reasoning *reasoning)
{
	assert(reasoning != NULL);
	assert(reasoning->start_node != NULL);
	assert(reasoning->current_node != NULL);
	assert(reasoning->current_node->tag != NULL);

	tagsistant_query(
		"select tag1, tag2, relation from relations where tag2 = \"%s\" and relation = \"is_equivalent\";",
		tagsistant_add_reasoned_tag, reasoning, reasoning->current_node->tag);

	tagsistant_query(
		"select tag2, tag1, relation from relations where tag1 = \"%s\" and relation = \"is_equivalent\";",
		tagsistant_add_reasoned_tag, reasoning, reasoning->current_node->tag);

	tagsistant_query(
		"select tag2, tag1, relation from relations where tag1 = \"%s\" and relation = \"includes\";",
		tagsistant_add_reasoned_tag, reasoning, reasoning->current_node->tag);

	if (reasoning->current_node->related != NULL) {
		reasoning->current_node = reasoning->current_node->related;
		tagsistant_reasoner(reasoning);
	}

	return(reasoning->added_tags);
}

/************************************************************************************/
/***                                                                              ***/
/*** FileTree translation                                                         ***/
/***                                                                              ***/
/************************************************************************************/

void key_destroyed(gpointer key)
{
	fprintf(stderr, "Destroying key %s\n", key);
}

void value_destroyed(gpointer value)
{
	tagsistant_file_handle *fh = (tagsistant_file_handle *) value;
	if (fh && fh->name) {
		fprintf(stderr, "Destroying value %s\n", fh->name);
	}
}

/**
 * add a file to the file tree (callback function)
 */
static int tagsistant_add_to_filetree(void *hash_table_pointer, dbi_result result)
{
	/* Cast the hash table */
	GHashTable *hash_table = (GHashTable *) hash_table_pointer;

	/* fetch query results into tagsistant_file_handle struct */
	tagsistant_file_handle *fh = g_new0(tagsistant_file_handle, 1);
	fh->name = g_strdup(dbi_result_get_string_idx(result, 1));
	fh->inode = dbi_result_get_uint_idx(result, 2);

	if (!fh->name || (strlen(fh->name) == 0)) {
		g_free(fh);
		return 0;
	}

	/* lookup the GList object */
	GList *list = g_hash_table_lookup(hash_table, fh->name);
	list = g_list_append(list, fh);
	g_hash_table_insert(hash_table, g_strdup(fh->name), list);

	dbg(LOG_INFO, "adding (%d,%s) to filetree", fh->inode, fh->name);

	return(0);
}

/**
 * drop all the views related to a ptree_or_node structure
 *
 * @param query the ptree_or_node_t structure that originated the views to be dropped
 */
void tagsistant_drop_views(ptree_or_node *query)
{
	while (query != NULL) {
		tagsistant_query("drop view tv%.16" PRIxPTR, NULL, NULL, (uintptr_t) query);
		query = query->next;
	}
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
GHashTable *tagsistant_filetree_new(ptree_or_node *query)
{
	if (query == NULL) {
		dbg(LOG_ERR, "NULL path_tree_t object provided to %s", __func__);
		return(NULL);
	}

	/* save a working pointer to the query */
	ptree_or_node *query_dup = query;

	dbg(LOG_INFO, "building filetree...");

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
		GString *statement = g_string_new("");
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

		g_string_append(statement, ";");
		dbg(LOG_INFO, "SQL: final statement is [%s]", statement->str);

		/* create view */
		tagsistant_query(statement->str, NULL, NULL);
		g_string_free(statement,TRUE);
		query = query->next;
	}

	/* format view statement */
	GString *view_statement = g_string_new("");
	query = query_dup;
	while (query != NULL) {
		g_string_append_printf(view_statement, "select objectname, inode from tv%.16" PRIxPTR, (uintptr_t) query);
		
		if (query->next != NULL) g_string_append(view_statement, " union ");
		
		query = query->next;
	}

	g_string_append(view_statement, ";");
	dbg(LOG_INFO, "SQL view statement: %s", view_statement->str);

	/* apply view statement */
	GHashTable *file_hash = g_hash_table_new_full(
		g_str_hash,
		g_str_equal,
		(GDestroyNotify) key_destroyed,
		(GDestroyNotify) value_destroyed);

	tagsistant_query(view_statement->str, tagsistant_add_to_filetree, file_hash);

	/* free the SQL statement */
	g_string_free(view_statement, TRUE);

	/* drop the views */
	tagsistant_drop_views(query_dup);

	return(file_hash);
}

/**
 * Destroy a filetree node
 */
void tagsistant_file_handle_destroy(gpointer unused_key, gpointer fh_pointer, gpointer unused_data)
{
	// TODO we are leaking memory here...
	return;
	if (!fh_pointer) return;
	
	(void) unused_key;
	(void) unused_data;

	tagsistant_file_handle *fh = (tagsistant_file_handle *) fh_pointer;

	freenull(fh->name);
	freenull(fh);
}

/**
 * Destroy a filetree
 */
void tagsistant_filetree_destroy(GHashTable *hash_table)
{
	if (!hash_table) return;

	g_hash_table_foreach(hash_table, tagsistant_file_handle_destroy, NULL);
	g_hash_table_destroy(hash_table);
}

// vim:ts=4:nowrap:nocindent
