/*
   Tagsistant (tagfs) -- path_resolution.h
   Copyright (C) 2006-2007 Tx0 <tx0@strumentiresistenti.org>

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

char *get_tag_name(const char *path)
{
	char *idx = rindex(path, '/');
	if (idx != NULL) {
		idx++;
		return strdup(idx);
	} else {
		return strdup(path);
	}
}

char *get_file_path(const char *tag)
{
	char *filepath = malloc(strlen(tagsistant.archive) + strlen(tag) + 1);
	if (filepath == NULL) {
		dbg(LOG_ERR, "error allocating memory in get_file_path");
		return NULL;
	}
	strcpy(filepath,tagsistant.archive);
	strcat(filepath,tag);
	return filepath;
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
ptree_or_node_t *build_querytree(const char *path)
{
	const char *pathptx = path + 1; /* skip first slash */

	ptree_or_node_t *result = calloc(sizeof(ptree_or_node_t), 1);
	if (result == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return NULL;
	}
	ptree_or_node_t *last_or = result;;
	ptree_and_node_t *last_and = NULL;

	unsigned int orcount = 0, andcount = 0;
	const char *idx = pathptx;
	int next_should_be_logical_op = 0;
	while (*idx != '\0') {
		/* get next slash. if there are no more slashes, reach end of string */
		idx = index(pathptx, '/');
		if (idx == NULL) idx = index(pathptx, '\0');

		/* duplicate next element */
		char *element = strndup(pathptx, idx - pathptx);

		if (next_should_be_logical_op) {
			if (strcmp(element, "AND") == 0) {
				/* skip it? nothing should be done with AND elements */
			} else if (strcmp(element, "OR") == 0) {
				/* open new entry in OR level */
				orcount++;
				andcount = 0;
				ptree_or_node_t *new_or = calloc(sizeof(ptree_or_node_t), 1);
				if (new_or == NULL) {
					dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
					return NULL;
				}
				last_or->next = new_or;
				last_or = new_or;
				last_and = NULL;
			} else {
				/* filename or error in path */
#if VERBOSE_DEBUG
				dbg(LOG_INFO, "%s is not AND/OR operator, exiting build_querytree()", element);
#endif
				free(element);
				return result;
			}
			next_should_be_logical_op = 0;
		} else {
			/* save next element in new ptree_and_node_t slot */
			ptree_and_node_t *and = calloc(sizeof(ptree_and_node_t), 1);
			if (and == NULL) {
				dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
				return NULL;
			}
			and->tag = strdup(element);
			if (last_and == NULL) {
				last_or->and_set = and;
			} else {
				last_and->next = and;
			}
			last_and = and;
#if VERBOSE_DEBUG
			dbg(LOG_INFO, "Query tree: %.2d.%.2d %s", orcount, andcount, element);
#endif
			andcount++;
			next_should_be_logical_op = 1;
		}

		free(element);
		pathptx = idx + 1;
	}
	dbg(LOG_INFO, "returning from build_querytree...");
	return result;
}

void destroy_querytree(ptree_or_node_t *qt)
{
	while (qt != NULL) {
		ptree_and_node_t *tag = qt->and_set;
		while (tag != NULL) {
			ptree_and_node_t *next = tag->next;
			free(tag->tag);
			free(tag);
			tag = next;
		}
		ptree_or_node_t *next = qt->next;
		free(qt);
		qt = next;
	}
}

struct atft {
	sqlite_int64 id;
	file_handle_t **fh;
};

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

	(*fh)->name = strdup(argv[0]);
	dbg(LOG_INFO, "adding %s to filetree", (*fh)->name);
	(*fh)->next = calloc(sizeof(file_handle_t), 1);
	if ((*fh)->next == NULL) {
		dbg(LOG_ERR, "Can't allocate memory in build_filetree");
		return 1;
	}
	(*fh) = (*fh)->next;
	(*fh)->next = NULL;
	(*fh)->name = NULL;

	/* add this entry to cache */
	char *sql = calloc(sizeof(char), strlen(ADD_RESULT_ENTRY) + strlen(argv[0]) + 14);
	if (sql == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
	} else {
		sprintf(sql, ADD_RESULT_ENTRY, atft->id, argv[0]);
		do_sql(NULL, sql, NULL, NULL);
		free(sql);
	}

#if VERBOSE_DEBUG
	dbg(LOG_INFO, "add_to_file_tree %s done!", argv[0]);
#endif
	return 0;
}

#if 0
static int get_id(void *id_buffer, int argc, char **argv, char **azColName)
{
	(void) argc;
	(void) azColName;
	int *exists = (int *) id_buffer;
	if (argv[0] != NULL) {
		sscanf(argv[0], "%d", exists);
		dbg(LOG_INFO, "Last query id is %s (%d)", argv[0], *exists);
	} else {
		*exists = 0;
	}
	return 0;
}
#endif

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
	if (query == NULL) {
		dbg(LOG_ERR, "NULL path_tree_t object provided to build_filetree");
		return NULL;
	}

	ptree_or_node_t *query_dup = query;

	file_handle_t *fh = calloc(sizeof(file_handle_t), 1);
	if ( fh == NULL ) {
		dbg(LOG_ERR, "Can't allocate memory in build_filetree");
		return NULL;
	}
	fh->next = NULL;
	fh->name = NULL;

	file_handle_t *result = fh;

	dbg(LOG_INFO, "building filetree...");

	unsigned int view_query_length = 2; /* for ending semicolon and \0 terminator */
	while (query != NULL) {
		ptree_and_node_t *tag = query->and_set;

		/* calculate number of tags and query string length */
		/* using memory addresses as temporary view names */
		unsigned int query_length = strlen("create temp view tv as ;") + 8 + 1;

		while (tag != NULL) {
			query_length += strlen(ALL_FILES_TAGGED) + strlen(tag->tag);
			if (tag->next != NULL) query_length += strlen(" intersect ");
			tag = tag->next;
		}

		/* format the query */
		char *statement = calloc(sizeof(char), query_length);
		if (statement == NULL) {
			dbg(LOG_ERR, "Error mallocating statement in build_filetree");
			return 0;
		}

		sprintf(statement, "create temp view tv%.8X as ", (unsigned int) query);

		view_query_length += strlen("select filename from tv") + 8;
		if (query->next != NULL) {
			view_query_length += strlen(" union ");
		}

		tag = query->and_set;
		while (tag != NULL) {
			/* formatting sub-select */
			char *mini = calloc(sizeof(char), strlen(ALL_FILES_TAGGED) + strlen(tag->tag) + 1);
			if (mini == NULL) {
				dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
				return NULL;
			}
			sprintf(mini, ALL_FILES_TAGGED, tag->tag);

			/* add sub-select to main statement */
			strcat(statement, mini);
			free(mini);

			if (tag->next != NULL) strcat(statement, " intersect ");

			dbg(LOG_INFO, "SQL: [%s]", statement);

			tag = tag->next;
		}

		strcat(statement, ";");
		assert(query_length > strlen(statement));
		dbg(LOG_INFO, "SQL: final statement is [%s]", statement);

		/* create view */
		do_sql(NULL, statement, NULL, NULL);
		free(statement);
		query = query->next;
	}

	char *sql = calloc(sizeof(char), strlen(ADD_CACHE_ENTRY) + strlen(path) + 1);
	if (sql == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return 0;
	}
	sprintf(sql, ADD_CACHE_ENTRY, path);

	dbg(LOG_INFO, "Adding path %s to cache", path);
	do_sql(NULL, sql, NULL, NULL);
	free(sql);
 
	sqlite_int64 id = sqlite3_last_insert_rowid(tagsistant.dbh);

#if 0
	sql = calloc(sizeof(char), strlen(GET_ID_OF_CACHE) + strlen(path) + 1);
	if (sql == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return 0;
	}

	if (sqlite3_exec(tagsistant.dbh, sql, get_id, &id, &sqlerror) != SQLITE_OK) {
		dbg(LOG_ERR, "SQL error: %s @%s:%d", sqlerror, __FILE__, __LINE__);
		sqlite3_free(sqlerror);
	}
	free(sql);
#endif

	/* format view statement */
	char *view_statement = calloc(sizeof(char), view_query_length);
	if (view_statement == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return NULL;
	}
	query = query_dup;
	while (query != NULL) {
		char *mini = calloc(sizeof(char), strlen("select filename from tv") + 8 + 1);
		if (sql == NULL) {
			dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		} else {
			sprintf(mini, "select filename from tv%.8X", (unsigned int) query);
			strcat(view_statement, mini);
			free(mini);

			if (query->next != NULL) strcat(view_statement, " union ");
		}

		query = query->next;
	}

	strcat(view_statement, ";");
	assert(view_query_length > strlen(view_statement));
	dbg(LOG_INFO, "SQL view statement: %s", view_statement);

	struct atft *atft = calloc(sizeof(struct atft), 1);
	if (atft == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return 0;
	}
	atft->fh = &fh;
	atft->id = id;

	/* apply view statement */
	do_sql(NULL, view_statement, add_to_filetree, atft);

	free(view_statement);

	/* drop select views */
	query = query_dup;
	while (query != NULL) {
		char *mini = calloc(sizeof(char), strlen("drop view tv;") + 8 + 1);
		if (mini == NULL) {
			dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		} else {
			sprintf(mini, "drop view tv%.8X;", (unsigned int) query);
			do_sql(NULL, mini, NULL, NULL);
			free(mini);
		}
		query = query->next;
	}

	return result;
}

void destroy_file_tree(file_handle_t *fh)
{
	if (fh == NULL)
		return;
	
	if (fh->name != NULL)
		free(fh->name);
	
	if (fh->next != NULL)
		destroy_file_tree(fh->next);
}

// vim:ts=4:nowrap:nocindent
