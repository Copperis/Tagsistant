/*
   TAGFS -- path_resolution.h
   Copyright (C) 2006-2007 Tx0 <tx0@autistici.org>

   Transform paths in queries and apply queries to file sets to
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

#include "mount.tagfs.h"

char *get_tag_name(const char *path)
{
	char *idx = rindex(path, '/');
	idx++;
	char *tagname = strdup(idx);
	return tagname;
}

char *get_file_path(const char *tag)
{
	char *filepath = malloc(strlen(tagfs.archive) + strlen(tag) + 1);
	if (filepath == NULL) {
		dbg(LOG_ERR, "error allocating memory in get_file_path");
		return NULL;
	}
	strcpy(filepath,tagfs.archive);
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
	ptree_or_node_t *last_or = result;;
	ptree_and_node_t *last_and = NULL;

	unsigned int orcount = 0, andcount = 0;
	const char *idx = pathptx;
	while (*idx != '\0') {
		/* get next slash. if there are no more slashes, reach end of string */
		idx = index(pathptx, '/');
		if (idx == NULL) idx = index(pathptx, '\0');

		/* duplicate next element */
		char *element = strndup(pathptx, idx - pathptx);

		if (strcmp(element, "AND") == 0) {
			/* skip it? nothing should be done with AND elements */
		} else if (strcmp(element, "OR") == 0) {
			/* open new entry in OR level */
			orcount++;
			andcount = 0;
			ptree_or_node_t *new_or = calloc(sizeof(ptree_or_node_t), 1);
			last_or->next = new_or;
			last_or = new_or;
			last_and = NULL;
		} else {
			/* save next element in new ptree_and_node_t slot */
			ptree_and_node_t *and = calloc(sizeof(ptree_and_node_t), 1);
			and->tag = strdup(element);
			if (last_and == NULL) {
				last_or->and_set = and;
			} else {
				last_and->next = and;
			}
			last_and = and;
			dbg(LOG_INFO, "Query tree: %.2d.%.2d %s", orcount, andcount, element);
			andcount++;
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

static int add_to_filetree(void *fhvoid, int argc, char **argv, char **azColName)
{
	(void) argc;
	(void) azColName;
	file_handle_t **fh = fhvoid;

	dbg(LOG_INFO, "add_to_file_tree: %s", argv[0]);

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

	dbg(LOG_INFO, "add_to_file_tree %s done!", argv[0]);
	return 0;
}

file_handle_t *build_filetree(ptree_or_node_t *query)
{
	if (query == NULL) {
		dbg(LOG_ERR, "NULL path_tree_t object provided to build_filetree");
		return NULL;
	}

	file_handle_t *fh = calloc(sizeof(file_handle_t), 1);
	if ( fh == NULL ) {
		dbg(LOG_ERR, "Can't allocate memory in build_filetree");
		return NULL;
	}
	fh->next = NULL;
	fh->name = NULL;

	file_handle_t *result = fh;

	dbg(LOG_INFO, "building filetree...");

	while (query != NULL) {
		ptree_and_node_t *tag = query->and_set;

		/* calculate number of tags and query string length */
		int query_length = 0;
		while (tag != NULL) {
			query_length += strlen(ALL_FILES_TAGGED) + strlen(tag->tag);
			if (tag->next != NULL) query_length += strlen(" intersect ");
			tag = tag->next;
		}
		query_length++; /* closing semicolon */
		dbg(LOG_INFO, "Query will be %d char long", query_length);

		/* format the query */
		char *statement = calloc(sizeof(char), query_length);
		if (statement == NULL) {
			dbg(LOG_ERR, "Error mallocating statement in build_filetree");
			return 0;
		}

		tag = query->and_set;
		while (tag != NULL) {
			/* formatting sub-select */
			char *mini = calloc(sizeof(char), strlen(ALL_FILES_TAGGED) + strlen(tag->tag) + strlen(" intersect ") + 1);
			sprintf(mini, ALL_FILES_TAGGED, tag->tag);
			if (tag->next != NULL) strcat(mini, " intersect ");

			/* add sub-select to main statement */
			strcat(statement, mini);
			free(mini);

			dbg(LOG_INFO, "SQL: [%s]", statement);

			tag = tag->next;
		}

		strcat(statement, ";");
		dbg(LOG_INFO, "SQL: final statement is [%s]", statement);

		/* perform query */
		dbg(LOG_INFO, "SQL query: %s", statement);
		char *sqlerror;
		if (sqlite3_exec(tagfs.dbh, statement, add_to_filetree, &fh, &sqlerror) != SQLITE_OK) {
			dbg(LOG_ERR, "SQL error: %s @%s:%d", sqlerror, __FILE__, __LINE__);
			sqlite3_free(sqlerror);
		}
		free(statement);
		query = query->next;
	}

	dbg(LOG_INFO, "filetree built!");

	/* set unique list of files */
	fh = result;
	file_handle_t *previous = fh;
	file_handle_t *test = fh->next;
	while ((fh != NULL) && (fh->name != NULL)) {
		while ((test != NULL) && (test->name != NULL)) {
			if (strcmp(test->name, fh->name) == 0) {
				test = test->next;
				free(previous->next->name);
				free(previous->next);
				previous->next = test;
			} else {
				previous = test;
				test = test->next;
			}
		}
		fh = fh->next;
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
