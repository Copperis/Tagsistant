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
 * SQL callback. Add new tag derived from reasoning to a ptree_and_node_t structure.
 *
 * \param vreasoning pointer to be casted to reasoning_t* structure
 * \param argc counter of argv arguments
 * \param argv array of SQL given results
 * \param azColName array of SQL column names
 * \return 0 (always, due to SQLite policy)
 */
static int add_alias_tag(void *vreasoning, int argc, char **argv, char **azColName)
{
	(void) argc;
	(void) azColName;

	/* point to a reasoning_t structure */
	reasoning_t *reasoning = (reasoning_t *) vreasoning;
	assert(reasoning != NULL);
	assert(reasoning->start_node != NULL);
	assert(reasoning->actual_node != NULL);
	assert(reasoning->added_tags >= 0);

	assert(argv[0]);
	assert(argv[1]);
	assert(argv[2]);

	ptree_and_node_t *and = reasoning->start_node;
	while (and->related != NULL) {
		assert(and->tag != NULL);
		if (strcmp(and->tag, argv[0]) == 0) {
			/* tag is already present, avoid looping */
			return 0;
		}
		and = and->related;
	}

	/* adding tag */
	and->related = g_malloc(sizeof(ptree_and_node_t));
	if (and->related == NULL) {
		dbg(LOG_ERR, "Error allocating memory");
		return 1;
	}

	and->related->next = NULL;
	and->related->related = NULL;
	and->related->tag = strdup(argv[0]);

	assert(and != NULL);
	assert(and->related != NULL);
	assert(and->related->tag != NULL);

	reasoning->added_tags += 1;

	dbg(LOG_INFO, "Adding related tag %s (because %s %s)", and->related->tag, argv[1], argv[2]);
	return 0;
}

/**
 * Search and add related tags to a ptree_and_node_t,
 * enabling build_filetree to later add more criteria to SQL
 * statements to retrieve files
 *
 * \param reasoning the reasoning structure the reasoner should work on
 * \return number of tags added
 */
int reasoner(reasoning_t *reasoning)
{
	assert(reasoning != NULL);
	assert(reasoning->start_node != NULL);
	assert(reasoning->actual_node != NULL);
	assert(reasoning->actual_node->tag != NULL);

	tagsistant_query(
		"select tag1, tag2, relation from relations where tag2 = \"%s\" and relation = \"is equivalent\";",
		add_alias_tag, reasoning, reasoning->actual_node->tag);
	
	tagsistant_query(
		"select tag2, tag1, relation from relations where tag1 = \"%s\" and relation = \"is equivalent\";",
		add_alias_tag, reasoning, reasoning->actual_node->tag);
	
	tagsistant_query(
		"select tag2, tag1, relation from relations where tag1 = \"%s\" and relation = \"includes\";",
		add_alias_tag, reasoning, reasoning->actual_node->tag);
	
	if (reasoning->actual_node->related != NULL) {
		reasoning->actual_node = reasoning->actual_node->related;
		reasoner(reasoning);
	}

	return reasoning->added_tags;
}

/**
 * allocate a new querytree_t structure
 */
querytree_t *new_querytree(const gchar *path)
{
	// allocate the struct
	querytree_t *qtree = g_new0(querytree_t, 1);
	if (qtree == NULL) {
		dbg(LOG_ERR, "Error allocating memory");
		return NULL;
	}

	// duplicate the path inside the struct
	qtree->full_path = g_strdup(path);

	return qtree;
}

/**
 * destroy a querytree_t structure
 */
void destroy_querytree(querytree_t *qtree)
{
	if (NULL == qtree) return;

	// destroy the tree
	ptree_or_node_t *node = qtree->tree;
	while (node != NULL) {
		ptree_and_node_t *tag = node->and_set;
		while (tag != NULL) {
			// walk related tags
			while (tag->related != NULL) {
				ptree_and_node_t *related = tag->related;
				tag->related = related->related;
				freenull(related->tag);
				freenull(related);
			}

			// free the ptree_and_node_t node
			ptree_and_node_t *next = tag->next;
			freenull(tag->tag);
			freenull(tag);
			tag = next;
		}
		// free the ptree_or_node_t node
		ptree_or_node_t *next = node->next;
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


/**
 * Build query tree from path. A querytree is composed of a linked
 * list of ptree_or_node_t objects. Each or object has a descending
 * linked list of ptree_and_node_t objects. This kind of structure
 * should be considered as a collection (the or horizontal level)
 * of queries (the and vertical lists). Each and list should be
 * resolved using SQLite INTERSECT compound operator. All results
 * should be then merged in a unique list of filenames.
 *
 * \param @path the path to be converted in a logical query
 */
querytree_t *build_querytree(const char *path, int do_reasoning)
{
	unsigned int orcount = 0, andcount = 0;

	// dbg(LOG_INFO, "Building querytree for %s", path);

	// allocate the querytree structure
	querytree_t *qtree = new_querytree(path);
	if (qtree == NULL) return NULL;

	// initialize iterator variables on query tree nodes
	ptree_or_node_t *last_or = qtree->tree = g_new0(ptree_or_node_t, 1);
	if (qtree->tree == NULL) {
		freenull(qtree);
		dbg(LOG_ERR, "Error allocating memory");
		return NULL;
	}
	ptree_and_node_t *last_and = NULL;

	// split the path
	gchar **splitted = g_strsplit(path, "/", 512); /* split up to 512 tokens */
	gchar **token_ptr = splitted + 1; /* first element is always "" since path begins with '/' */

#if VERBOSE_DEBUG
	gchar **tkp = splitted + 1;
	while (*tkp != NULL) {
		dbg(LOG_INFO, " Token: [%s]", *tkp);
		tkp++;
	}
#endif

	// guess the type of the query by first token
	if (g_strcmp0(*token_ptr, "tags") == 0) {
		qtree->type = QTYPE_TAGS;
	} else if (g_strcmp0(*token_ptr, "archive") == 0) {
 		qtree->type = QTYPE_ARCHIVE;
	} else if (g_strcmp0(*token_ptr, "relations") == 0) {
 		qtree->type = QTYPE_RELATIONS;
	} else if (g_strcmp0(*token_ptr, "stats") == 0) {
 		qtree->type = QTYPE_STATS;
	} else if ((NULL == *token_ptr) || (g_strcmp0(*token_ptr, "") == 0 || (g_strcmp0(*token_ptr, "/") == 0))) {
		qtree->type = QTYPE_ROOT;
	} else {
		qtree->type = QTYPE_MALFORMED;
		dbg(LOG_ERR, "Non existant path (%s)", path);
		goto RETURN;
	}

	// skip first token
	token_ptr++;

	// if the query is a QTYPE_TAGS query, parse it
	if (QTREE_IS_TAGS(qtree)) {
		// state if the query is complete or not
		qtree->complete = (NULL == g_strstr_len(path, strlen(path), "=")) ? 0 : 1;
		// dbg(LOG_INFO, "Path %s is %scomplete", path, qtree->complete ? "" : "not ");

		// by default a query is valid until somethig wrong happens while parsing it
		qtree->valid = 1;

		// begin parsing
		while ((*token_ptr != NULL) && (**token_ptr != '=')) {
			if (strlen(*token_ptr) == 0) {
				/* ignore zero length tokens */
			} else if (strcmp(*token_ptr, "=") == 0) {
				/* query end reached, jump out */
				goto RETURN;
			} else if (strcmp(*token_ptr, "+") == 0) {
				/* open new entry in OR level */
				orcount++;
				andcount = 0;
				ptree_or_node_t *new_or = g_new0(ptree_or_node_t, 1);
				if (new_or == NULL) {
					dbg(LOG_ERR, "Error allocating memory");
					goto RETURN;
				}
				last_or->next = new_or;
				last_or = new_or;
				last_and = NULL;
				dbg(LOG_INFO, "Allocated new OR node...");
			} else {
				/* save next token in new ptree_and_node_t slot */
				ptree_and_node_t *and = g_new0(ptree_and_node_t, 1);
				if (and == NULL) {
					dbg(LOG_ERR, "Error allocating memory");
					goto RETURN;
				}
				and->tag = g_strdup(*token_ptr);
				// dbg(LOG_INFO, "New AND node allocated on tag %s...", and->tag);
				and->next = NULL;
				and->related = NULL;
				if (last_and == NULL) {
					last_or->and_set = and;
				} else {
					last_and->next = and;
				}
				last_and = and;
	
				// dbg(LOG_INFO, "Query tree: %.2d.%.2d %s", orcount, andcount, *token_ptr);
				andcount++;
	
				/* search related tags */
				if (do_reasoning) {
#if VERBOSE_DEBUG
					dbg(LOG_INFO, "Searching for other tags related to %s", and->tag);
#endif
					reasoning_t *reasoning = g_malloc(sizeof(reasoning_t));
					if (reasoning != NULL) {
						reasoning->start_node = and;
						reasoning->actual_node = and;
						reasoning->added_tags = 0;
						int newtags = reasoner(reasoning);
						dbg(LOG_INFO, "Reasoning added %d tags", newtags);
					}
				}
			}

			// save last tag found
			freenull(qtree->last_tag);
			qtree->last_tag = g_strdup(*token_ptr);

			token_ptr++;
		}
	} else if (QTREE_IS_RELATIONS(qtree)) {
		/* parse a relations query */
		if (NULL != *token_ptr) {
			qtree->first_tag = g_strdup(*token_ptr);
			token_ptr++;
			if (NULL != *token_ptr) {	
				qtree->relation = g_strdup(*token_ptr);
				token_ptr++;
				if (NULL != *token_ptr) {
					qtree->second_tag = g_strdup(*token_ptr);
					qtree->complete = 1;
				}
			}
		}
	} else if (QTREE_IS_STATS(qtree)) {
		/* parse a stats query */
		if (NULL != *token_ptr) {
			qtree->stats_path = g_strdup(*token_ptr);
			qtree->complete = 1;
		}
	}

	/* remaining part is the object pathname */
	if (QTREE_IS_ARCHIVE(qtree) || (QTREE_IS_TAGS(qtree) && qtree->complete)) {
		if (QTREE_IS_TAGS(qtree) && qtree->complete) token_ptr++; // skip '='

		if (*token_ptr == NULL) {
			// set a null path
			qtree_set_object_path(qtree, g_strdup(""));
		} else {
			// set the object path and compute the relative paths
			qtree_set_object_path(qtree, g_strjoinv(G_DIR_SEPARATOR_S, token_ptr));

			// a path points is_taggable if it does not contains "/"
			// as in "23892.mydocument.odt" and not in "23893.myfolder/photo.jpg"
			if (
				(strlen(qtree->object_path) > 0) &&
				(g_strstr_len(qtree->object_path, strlen(qtree->object_path), G_DIR_SEPARATOR_S) == NULL)
			) {
				qtree->is_taggable = 1;
			}
		}
	}

#if 0
	/* object_path can't be null for completed /tags queries or /archive queries */
	/*
	 * update: that's not true! object_path must be zero length if nothing follows
	 * the = sign in /tags queries. that path is supposed to be relative to
	 * tagsistant.archive. the path used on all disk ops is ->full_archive_path
	 * which must actually be not null
	 */
	if (
		(strlen(qtree->object_path) == 0) &&
		(
			(QTREE_IS_TAGS(qtree) && qtree->complete) || 
			QTREE_IS_ARCHIVE(qtree) || QTREE_IS_STATS(qtree) || QTREE_IS_RELATIONS(qtree)
		)
	) {
		qtree->object_path = strdup(tagsistant.archive);
	}
#endif

	/*
	 * guess if query points to an object on disk or not
	 * that's true if object_path property is not null or zero length
	 * and both query is /archive or query is a complete /tags (including = sign)
	 */
	if (
		(strlen(qtree->object_path) > 0) &&
		(
		 	QTREE_IS_ARCHIVE(qtree) ||
			(QTREE_IS_TAGS(qtree) && qtree->complete)
		)
	) {
		qtree->points_to_object = 1;

		/*
		 * try to guess the object ID
		 */
		qtree->object_id = tagsistant_get_object_id(qtree->object_path, NULL);
	} else {
		qtree->points_to_object = 0;
	}

	/* get the id of the object referred by first element */
	gchar *dot = (NULL == *token_ptr) ? NULL : g_strstr_len(*token_ptr, -1, ".");
	if (NULL != dot) {
		qtree->object_id = strtol(*token_ptr, NULL, 10);
		dot++;
	}

RETURN:
	g_strfreev(splitted);
#if VERBOSE_DEBUG
	dbg(LOG_INFO, "Returning from build_querytree...");
#endif
	return qtree;
}

/* a struct used by add_to_filetree function */
struct atft {
	sqlite_int64 id;
	file_handle_t **fh;
	sqlite3 *dbh;
};

/**
 * add a file to the file tree (callback function)
 */
static int add_to_filetree(void *atft_struct, int argc, char **argv, char **azColName)
{
	(void) argc;
	(void) azColName;
	struct atft *atft = (struct atft*) atft_struct;
	file_handle_t **fh = atft->fh;

#if VERBOSE_DEBUG
	dbg(LOG_INFO, "add_to_file_tree: %s", argv[0]);
#endif

	/* no need to add empty files */
	if (argv[0] == NULL || strlen(argv[0]) == 0)
		return 0;

	(*fh)->name = g_strdup_printf("%s.%s", argv[1], argv[0]); // strdup(argv[0]);
	dbg(LOG_INFO, "adding %s to filetree", (*fh)->name);
	(*fh)->next = g_new0(file_handle_t, 1);
	if ((*fh)->next == NULL) {
		dbg(LOG_ERR, "Can't allocate memory in build_filetree");
		return 1;
	}
	(*fh) = (*fh)->next;
	(*fh)->next = NULL;
	(*fh)->name = NULL;

#if VERBOSE_DEBUG
	dbg(LOG_INFO, "add_to_file_tree %s done!", argv[0]);
#endif
	return 0;
}

void drop_views(ptree_or_node_t *query, sqlite3 *dbh)
{
	while (query != NULL) {
		char *mini = g_strdup_printf("drop view tv%.8X;", (unsigned int) query);
		do_sql(&dbh, mini, NULL, NULL);
		freenull(mini);
		query = query->next;
	}
}

/**
 * build a linked list of filenames that apply to querytree
 * query expressed in query. querytree is used as follows:
 * 
 * 1. each ptree_and_node_t list converted in a INTERSECT
 * multi-SELECT query, and is saved as a VIEW. The name of
 * the view is the string tv%.8X where %.8X is the memory
 * location of the ptree_or_node_t to which the list is
 * linked.
 *
 * 2. a global select is built using all the views previously
 * created joined by UNION operators. a sqlite3_exec call
 * applies it using add_to_filetree() as callback function.
 *
 * 3. all the views are removed with a DROP VIEW query.
 *
 * @param query the ptree_or_node_t* query structure to
 * be resolved.
 */
file_handle_t *build_filetree(ptree_or_node_t *query, const char *path)
{
	(void) path;

	/* opening a persistent connection */
	sqlite3 *dbh = NULL;
	int res = sqlite3_open(tagsistant.tags, &dbh);
	if (res != SQLITE_OK) {
		dbg(LOG_ERR, "Error [%d] opening database %s", res, tagsistant.tags);
		dbg(LOG_ERR, "%s", sqlite3_errmsg(dbh));
		return NULL;
	}

	if (query == NULL) {
		dbg(LOG_ERR, "NULL path_tree_t object provided to build_filetree");
		sqlite3_close(dbh);
		return NULL;
	}

	ptree_or_node_t *query_dup = query;

	file_handle_t *fh = g_new0(file_handle_t, 1);
	if ( fh == NULL ) {
		dbg(LOG_ERR, "Can't allocate memory in build_filetree");
		sqlite3_close(dbh);
		return NULL;
	}
	fh->next = NULL;
	fh->name = NULL;

	file_handle_t *result = fh;

	dbg(LOG_INFO, "building filetree...");

	while (query != NULL) {
		ptree_and_node_t *tag = query->and_set;
		GString *statement = g_string_new("");
		g_string_printf(statement, "create view tv%.8x as ", (unsigned int) query);
		
		while (tag != NULL) {
			g_string_append(statement, "select objectname, object_id from objects join tagging on tagging.object_id = objects.object_id join on tags on tags.tag_id = tagging.tag_id where tagname = \"");
			g_string_append(statement, tag->tag);
			g_string_append(statement, "\"");
			
			/* add related tags */
			if (tag->related != NULL) {
				ptree_and_node_t *related = tag->related;
				while (related != NULL) {
					g_string_append(statement, " or tagname = \"");
					g_string_append(statement, related->tag);
					g_string_append(statement, "\"");
					related = related->related;
				}
			}

			if (tag->next != NULL) g_string_append(statement, " intersect ");

			tag = tag->next;
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
		g_string_append_printf(view_statement, "select filename, id from tv%.8X", (unsigned int) query);
		
		if (query->next != NULL) g_string_append(view_statement, " union ");
		
		query = query->next;
	}

	g_string_append(view_statement, ";");
	dbg(LOG_INFO, "SQL view statement: %s", view_statement->str);

	struct atft *atft = g_new0(struct atft, 1);
	if (atft == NULL) {
		dbg(LOG_ERR, "Error allocating memory");
		g_string_free(view_statement, TRUE);
		destroy_filetree(result);
		drop_views(query_dup, dbh);
		sqlite3_close(dbh);
		return NULL;
	}
	atft->fh = &fh;
	atft->dbh = dbh;

	/* apply view statement */
	do_sql(&dbh, view_statement->str, add_to_filetree, atft);
	freenull(atft);

	g_string_free(view_statement, TRUE);

	drop_views(query_dup, dbh);
	sqlite3_close(dbh);
	return result;
}

void destroy_filetree(file_handle_t *fh)
{
	if (fh == NULL)
		return;
	
	if (fh->name != NULL)
		freenull(fh->name);
	
	if (fh->next != NULL)
		destroy_filetree(fh->next);

	freenull(fh);
}

// vim:ts=4:nowrap:nocindent
