/*
   Tagsistant (tagfs) -- reasoner.c
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

/************************************************************************************/
/***                                                                              ***/
/*** Reasoner                                                                     ***/
/***                                                                              ***/
/************************************************************************************/

static GHashTable *tagsistant_reasoner_cache;

/**
 * Destroy the values of the resoner cache.
 *
 * @param data pointer to GList
 */
void tagsistant_destroy_reasoner_value(gpointer data)
{
	GList *list = (GList *) data;
	g_list_free_full(list, g_free);
}

/**
 * Initialize reasoner library
 */
void tagsistant_reasoner_init()
{
	tagsistant_reasoner_cache = g_hash_table_new_full(
		g_str_hash,		/* key hashing */
		g_str_equal,	/* key comparison */
		g_free,			/* key destroy */
		tagsistant_destroy_reasoner_value);	/* value destroy (it's a GList, we must free is gchar * values) */
}

/**
 * Add a reasoned tag to a node. Used by both tagsistant_add_reasoned_tag_callback()
 * and tagsistant_reasoner()
 *
 * @param _reasoning
 * @param result
 * @return
 */
static int tagsistant_add_reasoned_tag(const gchar *reasoned_tag, tagsistant_reasoning *reasoning)
{
	/* check for duplicates */
	ptree_and_node *and = reasoning->start_node;
	while (and) {
		if (strcmp(and->tag, reasoned_tag) == 0) return (0); // duplicate, don't add

		ptree_and_node *related = and->related;
		while (related && related->tag) {
			if (strcmp(related->tag, reasoned_tag) == 0) return (0); // duplicate, don't add
			related = related->related;
		}

		and = and->next;
	}

	/* adding tag */
	ptree_and_node *reasoned = g_new0(ptree_and_node, 1);

	if (!reasoned) {
		dbg('r', LOG_ERR, "Error allocating memory");
		return (-1);
	}

	reasoned->next = NULL;
	reasoned->related = NULL;
	reasoned->tag = g_strdup(reasoned_tag);

	/* prepend the reasoned tag */
	reasoned->related = reasoning->current_node->related;
	reasoning->current_node->related = reasoned;

	reasoning->added_tags += 1;
	return (reasoning->added_tags);
}

/**
 * SQL callback. Add new tag derived from reasoning to a ptree_and_node_t structure.
 *
 * @param _reasoning pointer to be casted to reasoning_t* structure
 * @param result dbi_result pointer
 * @return 0 always, due to SQLite policy, may change in the future
 */
static int tagsistant_add_reasoned_tag_callback(void *_reasoning, dbi_result result)
{
	/* point to a reasoning_t structure */
	tagsistant_reasoning *reasoning = (tagsistant_reasoning *) _reasoning;

	/* fetch the reasoned tag */
	const char *reasoned_tag = dbi_result_get_string_idx(result, 1);

	/* add the tag */
	if (-1 == tagsistant_add_reasoned_tag(reasoned_tag, reasoning)) {
		dbg('r', LOG_ERR, "Error adding reasoned tag %s", reasoned_tag);
		return (1);
	}

	dbg('r', LOG_INFO, "Adding related tag %s", reasoned_tag);
	return (0);
}

/**
 * Search and add related tags to a ptree_and_node_t,
 * enabling tagsistant_build_filetree to later add more criteria to SQL
 * statements to retrieve files
 *
 * @param reasoning the reasoning structure the tagsistant_reasoner should work on
 */
int tagsistant_reasoner_inner(tagsistant_reasoning *reasoning, int do_caching)
{
	(void) do_caching;

#if TAGSISTANT_ENABLE_REASONER_CACHE
	GList *cached = NULL;
	gchar *key = NULL;
	int found = g_hash_table_lookup_extended(
		tagsistant_reasoner_cache,
		reasoning->current_node->tag,
		(gpointer *) &key,
		(gpointer *) &cached);

	if (found)
		// the result was cached, just add it
		g_list_foreach(cached, (GFunc) tagsistant_add_reasoned_tag, reasoning);
	else

#endif /* TAGSISTANT_ENABLE_REASONER_CACHE*/

		// the result wasn't cached, so we lookup it in the DB
		tagsistant_query(
			"select tags2.tagname from relations "
				"join tags as tags1 on tags1.tag_id = relations.tag1_id "
				"join tags as tags2 on tags2.tag_id = relations.tag2_id "
				"where tags1.tagname = '%s' "
			"union all "
			"select tags1.tagname from relations "
				"join tags as tags1 on tags1.tag_id = relations.tag1_id "
				"join tags as tags2 on tags2.tag_id = relations.tag2_id "
				"where tags2.tagname = '%s' and relation = 'is_equivalent'",
			reasoning->conn,
			tagsistant_add_reasoned_tag_callback,
			reasoning,
			reasoning->current_node->tag,
			reasoning->current_node->tag);

	/* reason on related tags */
	if (reasoning->current_node->related) {
		reasoning->current_node = reasoning->current_node->related;
		tagsistant_reasoner_inner(reasoning, 0); // don't do_caching
	}

#if TAGSISTANT_ENABLE_REASONER_CACHE
	/*
	 * Cache the result only if the cache doesn't contain it.
	 */
	if (do_caching && !found) {
		// first we must build a GList holding all the reasoned tags...
		GList *reasoned_list = NULL;
		ptree_and_node *reasoned = reasoning->start_node->related;
		while (reasoned) {
			reasoned_list = g_list_append(reasoned_list, g_strdup(reasoned->tag));
			reasoned = reasoned->related;
		}

		// ...and then we add the tag list to the cache
		g_hash_table_insert(tagsistant_reasoner_cache, g_strdup(reasoning->start_node->tag), reasoned_list);
	}
#endif /* TAGSISTANT_ENABLE_REASONER_CACHE */

	return (reasoning->added_tags);
}

void tagsistant_invalidate_reasoning_cache(gchar *tag)
{
#if TAGSISTANT_ENABLE_REASONER_CACHE
	g_hash_table_remove(tagsistant_reasoner_cache, tag);
#else
	(void) tag;
#endif
}
