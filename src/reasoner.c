/*
   Tagsistant (tagfs) -- reasoner.c
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

/************************************************************************************/
/***                                                                              ***/
/*** Reasoner                                                                     ***/
/***                                                                              ***/
/************************************************************************************/

GHashTable *tagsistant_reasoner_cache;

/**
 * Initialize reasoner library
 */
void tagsistant_reasoner_init()
{
	tagsistant_reasoner_cache = g_hash_table_new(g_str_hash, g_str_equal);
}

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
	assert(reasoning);
	assert(reasoning->start_node);
	assert(reasoning->current_node);
	assert(reasoning->added_tags >= 0);

	/* fetch the reasoned tag */
	const char *reasoned_tag = dbi_result_get_string_idx(result, 1);

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
		dbg(LOG_ERR, "Error allocating memory");
		return (1);
	}

	reasoned->next = NULL;
	reasoned->related = NULL;
	reasoned->tag = g_strdup(reasoned_tag);

	assert(reasoned->tag);

	/* append the reasoned tag */
	ptree_and_node *related = reasoning->current_node;
	while (related->related) related = related->related;
	related->related = reasoned;

	reasoning->added_tags += 1;

#if TAGSISTANT_VERBOSE_LOGGING
	dbg(LOG_INFO, "Adding related tag %s", reasoned->tag);
#endif

	return(0);
}

/**
 * Search and add related tags to a ptree_and_node_t,
 * enabling tagsistant_build_filetree to later add more criteria to SQL
 * statements to retrieve files
 *
 * @param reasoning the reasoning structure the tagsistant_reasoner should work on
 */
void tagsistant_reasoner_recursive(tagsistant_reasoning *reasoning)
{
	assert(reasoning);
	assert(reasoning->current_node);
	assert(reasoning->current_node->tag);
	assert(reasoning->conn);

	tagsistant_query(
		"select tags2.tagname from relations "
			"join tags as tags1 on tags1.tag_id = relations.tag1_id "
			"join tags as tags2 on tags2.tag_id = relations.tag2_id "
			"where tags1.tagname = \"%s\" "
				/* "and (relation = \"is_equivalent\" or relation = \"includes\") " */,
		reasoning->conn,
		tagsistant_add_reasoned_tag,
		reasoning,
		reasoning->current_node->tag);

	tagsistant_query(
		"select tags1.tagname from relations "
			"join tags as tags1 on tags1.tag_id = relations.tag1_id "
			"join tags as tags2 on tags2.tag_id = relations.tag2_id "
			"where tags2.tagname = \"%s\" and relation = \"is_equivalent\";",
		reasoning->conn,
		tagsistant_add_reasoned_tag,
		reasoning,
		reasoning->current_node->tag);

	if (reasoning->current_node->related) {
		reasoning->current_node = reasoning->current_node->related;
		tagsistant_reasoner_recursive(reasoning);
	}
}

/**
 * Reasoner wrapper. Do reasoning and cache the results (TODO)
 *
 * @param reasoning
 * @return number of tags added
 */
int tagsistant_reasoner(tagsistant_reasoning *reasoning)
{
	tagsistant_reasoner_recursive(reasoning);

	return(reasoning->added_tags);
}
