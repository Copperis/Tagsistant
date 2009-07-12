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

#ifdef MACOSX
char *strndup(const char *s, size_t n)
{
	char *result = calloc(sizeof(char), n+1);
	if (result == NULL)
		return NULL;

	memcpy(result, s, n);
	result[n] = '\0';
	return result;
}
#endif

typedef struct reasoning {
	ptree_and_node_t *start_node;
	ptree_and_node_t *actual_node;
	int added_tags;
} reasoning_t;

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
	and->related = malloc(sizeof(ptree_and_node_t));
	if (and->related == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
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
					freenull(element);
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
				freenull(element);
				return result;
			}
			next_should_be_logical_op = 0;
		} else {
			/* save next element in new ptree_and_node_t slot */
			ptree_and_node_t *and = calloc(sizeof(ptree_and_node_t), 1);
			if (and == NULL) {
				dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
				freenull(element);
				return NULL;
			}
			and->tag = strdup(element);
			and->next = NULL;
			and->related = NULL;
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

			/* search related tags */
#if VERBOSE_DEBUG
			dbg(LOG_INFO, "Searching for other tags related to %s", and->tag);
#endif
			reasoning_t *reasoning = malloc(sizeof(reasoning_t));
			if (reasoning != NULL) {
				reasoning->start_node = and;
				reasoning->actual_node = and;
				reasoning->added_tags = 0;
				int newtags = reasoner(reasoning);
			}
		}

		freenull(element);
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
			while (tag->related != NULL) {
				ptree_and_node_t *related = tag->related;
				tag->related = related->related;
				freenull(related->tag);
				freenull(related);
			}

			ptree_and_node_t *next = tag->next;
			freenull(tag->tag);
			freenull(tag);
			tag = next;
		}
		ptree_or_node_t *next = qt->next;
		freenull(qt);
		qt = next;
	}
}

struct atft {
	sqlite_int64 id;
	file_handle_t **fh;
	sqlite3 *dbh;
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

#if VERBOSE_DEBUG
	dbg(LOG_INFO, "add_to_file_tree %s done!", argv[0]);
#endif
	return 0;
}

void drop_views(ptree_or_node_t *query, sqlite3 *dbh)
{
	/* drop select views */
	int len = strlen("drop view tv;") + 8 + 1;
	char *mini = malloc(len);
	if (mini == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
	} else {
		while (query != NULL) {
			memset(mini, 0, len);
			sprintf(mini, "drop view tv%.8X;", (unsigned int) query);
			do_sql(&dbh, mini, NULL, NULL);
			query = query->next;
		}
		freenull(mini);
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

	file_handle_t *fh = calloc(sizeof(file_handle_t), 1);
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
		gchar *statement = g_strdup_printf("create temp view tv%.8x as ", (unsigned int) query);
		
		while (tag != NULL) {
			g_strlcat(statement, "select filename from tagged where tagname = \"", SQLITE_MAX_SQL_LENGTH);
			g_strlcat(statement, tag->tag, SQLITE_MAX_SQL_LENGTH);
			g_strlcat(statement, "\"", SQLITE_MAX_SQL_LENGTH);
			g_strlcat(statement, "select filename from tagged where tagname = \"", SQLITE_MAX_SQL_LENGTH);
			g_strlcat(statement, tag->tag, SQLITE_MAX_SQL_LENGTH);
			g_strlcat(statement, "\"", SQLITE_MAX_SQL_LENGTH);
			
			/* add related tags */
			if (tag->related != NULL) {
				ptree_and_node_t *related = tag->related;
				while (related != NULL) {
					g_strlcat(statement, " or tagname = \"", SQLITE_MAX_SQL_LENGTH);
					g_strlcat(statement, related->tag, SQLITE_MAX_SQL_LENGTH);
					g_strlcat(statement, "\"", SQLITE_MAX_SQL_LENGTH);
					related = related->related;
				}
			}

			if (tag->next != NULL) g_strlcat(statement, " intersect ", SQLITE_MAX_SQL_LENGTH);

			tag = tag->next;
		}

		g_strlcat(statement, ";", SQLITE_MAX_SQL_LENGTH);
		dbg(LOG_INFO, "SQL: final statement is [%s]", statement);

		/* create view */
		tagsistant_query(statement, NULL, NULL);
		g_free(statement);
		query = query->next;
	}

	/* format view statement */
	gchar *view_statement = NULL;
	query = query_dup;
	while (query != NULL) {
		gchar *mini = g_strdup_printf("select filename from tv%.8X", (unsigned int) query);
		g_strlcat(view_statement, mini, SQLITE_MAX_SQL_LENGTH);
		g_free(mini);
		
		if (query->next != NULL) g_strlcat(view_statement, " union ", SQLITE_MAX_SQL_LENGTH);
		
		query = query->next;
	}

	strcat(view_statement, ";");
	dbg(LOG_INFO, "SQL view statement: %s", view_statement);

	struct atft *atft = calloc(sizeof(struct atft), 1);
	if (atft == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		freenull(view_statement);
		destroy_filetree(result);
		drop_views(query_dup, dbh);
		sqlite3_close(dbh);
		return NULL;
	}
	atft->fh = &fh;
	atft->dbh = dbh;

	/* apply view statement */
	do_sql(&dbh, view_statement, add_to_filetree, atft);
	freenull(atft);

	freenull(view_statement);

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
