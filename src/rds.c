/*
   Tagsistant (tagfs) -- rds.c
   Copyright (C) 2006-2013 Tx0 <tx0@strumentiresistenti.org>

   Tagsistant Resilient Data Sets.

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

/*
   Tagsistant Resilient Data Set (RDS) are cached collections of query
   results which can be thrown away and rebuild at every moment. An RDS
   saves the results of a single subquery. As an example, the query:

     store/tag1/tag2/+/tag3/-/tag4/@

   which is formed by two subqueries:

     1. tag1/tag2/
     2. tag3/-/tag4

   will produce two RDS, one for each subquery.

   Tagsistant creates and uses RDS to:

     1. resolve queries (readdir.c)
     2. check objects existence (getattr.c)

   and discard them on:

     1. object creation (mknod.c, mkdir.c) inside store/
     2. object deletion (unlink.c, rmdir.c) inside store/
     3. tags deletion (rmdir.c) inside store/ or tags/
     4. relations creation (mkdir.c) inside relations/
     5. relations deletion (rmdir.c) inside relations/

   An RDS persists across many queries and can be discarded for performance
   reason, if the DB is being clogged by the RDS cache.

   RDS are saved in the RDS table:

     create RDS (
       rds_id int not null,
       inode int not null,
       objectname varchar(255)
     );

   Every RDS is listed once in the RDS catalog table:

     create table RDS_catalog (
       rds_id int primary key not null auto_increment,
       creation date not null default now(),
       query varchar(1024) not null
     );

   When a getattr() call or a readdir() call needs to process a single
   subquery, first checks if the corresponding RDS has been created:

     select rds_id from RDS_catalog where query = '...';

   If the RDS is available, the query results can be loaded from the
   RDS table with:

     select distinct inode, objectname from RDS where rds_id in (...);

   For example, if the query store/tag1/tag2/+/tag3/-/tag4/@ can be
   answered from the RDS #314 (tag1/tag2) and #315 (tag3/-/tag4/),
   the previous query just becomes:

     select distinct inode, objectname from RDS where rds_id in (314, 315);

 */

#include "tagsistant.h"

/**
 * a callback that adds an object entry (inode + objectname) to a
 * GHashTable holding the RDS results
 *
 * @param hash_table_pointer a GHashTable to hold results
 * @param result a DBI result
 * @return always returns zero (DBI requirement)
 */
static int tagsistant_RDS_add_entry_callback(void *hash_table_pointer, dbi_result result)
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
 * Add a filter criterion to a WHERE clause based on a qtree_and_node object
 *
 * @param statement a GString object holding the building query statement
 * @param and_set the qtree_and_node object describing the tag to be added as a criterion
 */
void tagsistant_query_add_and_set(GString *statement, qtree_and_node *and_set)
{
	if (and_set->tag_id) {
		g_string_append_printf(statement, "tagging.tag_id = %d ", and_set->tag_id);
	} else if (and_set->tag) {
		g_string_append_printf(statement, "tagname = '%s' ", and_set->tag);
	} else if (and_set->value) {
		switch (and_set->operator) {
			case TAGSISTANT_EQUAL_TO:
				g_string_append_printf(statement,
					"tagname = '%s' and `key` = '%s' and value = '%s' ",
					and_set->namespace,
					and_set->key,
					and_set->value);
				break;
			case TAGSISTANT_CONTAINS:
				g_string_append_printf(statement,
					"tagname = '%s' and `key` = '%s' and value like '%%%s%%' ",
					and_set->namespace,
					and_set->key,
					and_set->value);
				break;
			case TAGSISTANT_GREATER_THAN:
				g_string_append_printf(statement,
					"tagname = '%s' and `key` = '%s' and value > '%s' ",
					and_set->namespace,
					and_set->key,
					and_set->value);
				break;
			case TAGSISTANT_SMALLER_THAN:
				g_string_append_printf(statement,
					"tagname = '%s' and `key` = '%s' and value < '%s' ",
					and_set->namespace,
					and_set->key,
					and_set->value);
				break;
		}
	}
}

/**
 * Append a tag (a qtree_and_node) to a GString object
 *
 * @param subquery the GString object
 * @param node the qtree_and_node
 */
void tagsistant_RDS_subquery_add_tag(GString *subquery, qtree_and_node *node, int negated)
{
	if (negated) {
		g_string_append(subquery, "-/");
	}

	if (node->tag) {
		g_string_append_printf(subquery, "%s/", node->tag);
	} else {
		g_string_append_printf(subquery, "%s/%s/", node->namespace, node->key);
		switch (node->operator) {
			case TAGSISTANT_EQUAL_TO:     g_string_append(subquery, "eq/");  break;
			case TAGSISTANT_CONTAINS:     g_string_append(subquery, "inc/"); break;
			case TAGSISTANT_GREATER_THAN: g_string_append(subquery, "gt/");  break;
			case TAGSISTANT_SMALLER_THAN: g_string_append(subquery, "lt/");  break;
		}
		g_string_append_printf(subquery, "%s/", node->value);
	}
}

/**
 * Build the string of a subquery
 *
 * @param query the qtree_or_node holding the subquery
 * @return a string with the subquery representation
 */
gchar *tagsistant_RDS_build_subquery(qtree_or_node *query)
{
	GString *subquery = g_string_new("");
	qtree_and_node *node_ptr = query->and_set;
	while (node_ptr) {
		tagsistant_RDS_subquery_add_tag(subquery, node_ptr, 0);
		node_ptr = node_ptr->next;
	}

	node_ptr = query->and_set;
	while (node_ptr) {
		qtree_and_node *negated = node_ptr->negated;
		while (negated) {
			tagsistant_RDS_subquery_add_tag(subquery, negated, 1);
			negated = negated->next;
		}
		node_ptr = node_ptr->next;
	}

	gchar *subquery_path = subquery->str;
	g_string_free(subquery, FALSE);

	return (subquery_path);
}

/**
 * Fetch the rds_id of a subquery
 *
 * @param subquery the string representing the subquery
 * @param conn a dbi_conn handle
 * @return the rds_id
 */
int tagsistant_RDS_fetch_id(gchar *subquery, dbi_conn conn, int rebuild_expired_RDS)
{
	if (rebuild_expired_RDS) {
		tagsistant_query(
			"delete from RDS where rds_id = ("
				"select rds_id "
				"from RDS_catalog "
				"where subquery = '%s')",
			conn, NULL, NULL, subquery);

		tagsistant_query(
			"delete from RDS_catalog "
				"where subquery = '%s'",
			conn, NULL, NULL, subquery);
	}

	int rds_id = 0;

	tagsistant_query(
		"select rds_id from RDS_catalog where subquery = '%s'",
		conn,
		tagsistant_return_integer,
		&rds_id,
		subquery);

	return (rds_id);
}

/**
 * Build an RDS
 *
 * @param query
 */
int tagsistant_RDS_build(qtree_or_node *query, gchar *subquery, dbi_conn conn)
{
	int rds_id = 0;

	/*
	 * PHASE 1.
	 * register the RDS on the RDS_catalog
	 */
	tagsistant_query(
		"insert into RDS_catalog (subquery) values ('%s')",
		conn,
		NULL,
		NULL,
		subquery);

	rds_id = tagsistant_last_insert_id(conn);

	/*
	 * PHASE 2.
	 * create the RDS including all the objects tagged by the first tag
	 */
	GString *phase_2 = g_string_sized_new(51200);
	g_string_append_printf(phase_2,
		"insert into RDS "
		"select %d, objects.inode, objects.objectname "
			"from objects "
			"join tagging on tagging.inode = objects.inode "
			"join tags on tags.tag_id = tagging.tag_id "
			"where ",
		rds_id);

	/*
	 * add each qtree_and_node (main and ->related) to the query
	 */
	if (query->and_set) {
		tagsistant_query_add_and_set(phase_2, query->and_set);

		qtree_and_node *related = query->and_set->related;
		while (related) {
			g_string_append(phase_2, " or ");
			tagsistant_query_add_and_set(phase_2, related);
			related = related->related;
		}

		/*
		 * create the table and dispose the statement GString
		 */
		tagsistant_query(phase_2->str, conn, NULL, NULL);
		g_string_free(phase_2, TRUE);

		/*
		 * PHASE 3.
		 * for each ->next linked node, subtract from the RDS
		 * the objects not matching it
		 */
		qtree_and_node *next = query->and_set->next;
		while (next) {
			GString *phase_3 = g_string_sized_new(51200);
			g_string_append_printf(phase_3,
				"delete from RDS "
				"where rds_id = %d "
				"and inode not in ("
					"select objects.inode from objects "
						"join tagging on tagging.inode = objects.inode "
						"join tags on tags.tag_id = tagging.tag_id "
						"where ",
				rds_id);

			/*
			 * add each qtree_and_node (main and ->related) to the query
			 */
			tagsistant_query_add_and_set(phase_3, next);

			qtree_and_node *related = next->related;
			while (related) {
				g_string_append(phase_3, " or ");
				tagsistant_query_add_and_set(phase_3, related);
				related = related->related;
			}

			/*
			 * close the subquery
			 */
			g_string_append(phase_3, ")");

			/*
			 * apply the query and dispose the statement GString
			 */
			tagsistant_query(phase_3->str, conn, NULL, NULL);
			g_string_free(phase_3, TRUE);

			next = next->next;
		}

		/*
		 * PHASE 4.
		 * for each ->negated linked node, subtract from the base table
		 * the objects that do match it
		 */
		next = query->and_set;
		while (next) {
			qtree_and_node *negated = next->negated;
			while (negated) {
				GString *phase_4 = g_string_sized_new(51200);
				g_string_append_printf(phase_4,
					"delete from RDS "
					"where rds_id = %d "
					"and inode in ("
						"select objects.inode from objects "
							"join tagging on tagging.inode = objects.inode "
							"join tags on tags.tag_id = tagging.tag_id "
							"where ",
					rds_id);

				/*
				 * add each qtree_and_node (main and ->related) to the query
				 */
				tagsistant_query_add_and_set(phase_4, negated);

				qtree_and_node *related = negated->related;
				while (related) {
					g_string_append(phase_4, " or ");
					tagsistant_query_add_and_set(phase_4, related);
					related = related->related;
				}

				/*
				 * close the subquery
				 */
				g_string_append(phase_4, ")");

				/*
				 * apply the query and dispose the statement GString
				 */
				tagsistant_query(phase_4->str, conn, NULL, NULL);
				g_string_free(phase_4, TRUE);

				negated = negated->negated;
			}

			next = next->next;
		}
	}

	return (rds_id);
}

GMutex tagsistant_RDS_mutex;

/**
 * build a linked list of filenames that satisfy the querytree
 * object. This is translated in a two phase flow:
 *
 * 1. each qtree_and_node list is translated into one
 * (temporary) table
 *
 * 2. the content of all tables are read in with a UNION
 * chain inside a super-select to apply the ORDER BY clause.
 *
 * 3. all the (temporary) tables are removed
 *
 * @param query the qtree_or_node query structure to be resolved
 * @param conn a libDBI dbi_conn handle
 * @param is_all_path is true when the path includes the ALL/ tag
 * @return a pointer to a GHashTable of tagsistant_file_handle objects
 */
gchar *tagsistant_RDS_prepare(
	qtree_or_node *query,
	dbi_conn conn,
	int is_all_path,
	int rebuild_expired_RDS)
{
	/*
	 * If the query contains the ALL meta-tag, just select all the available
	 * objects and return them
	 */
	if (is_all_path) {
		return (NULL);
	}

	/*
	 * a NULL query can't be processed
	 */
	if (!query) {
		dbg('f', LOG_ERR, "NULL path_tree_t object provided to %s", __func__);
		return(NULL);
	}

	/*
	 * this GString object will contain all the rds_id required to
	 * answer to the query
	 */
	GString *RDS_ids = g_string_new("");

	/*
	 * RDS creation
	 */
	while (query) {
		/*
		 * build the subquery part
		 */
		gchar *subquery = tagsistant_RDS_build_subquery(query);

		g_mutex_lock(&tagsistant_RDS_mutex);

		/*
		 * Check if the RDS has been already built
		 */
		int rds_id = tagsistant_RDS_fetch_id(subquery, conn, rebuild_expired_RDS);

		/*
		 * if the RDS is not available, build it
		 */
		if (!rds_id) {
			rds_id = tagsistant_RDS_build(query, subquery, conn);
		}

		g_mutex_unlock(&tagsistant_RDS_mutex);

		/*
		 * save the RDS id for later extraction
		 */
		g_string_append_printf(RDS_ids, ", %d", rds_id);

		/*
		 * move to the next qtree_or_node in the linked list
		 */
		query = query->next;
	}

	/*
	 * after building all the RDS, return the set of rds_id
	 * called a RDS_fingerprint
	 */
	gchar *RDS_fingerprint = g_strdup(RDS_ids->str + 2); // skip the starting ", "
	g_string_free(RDS_ids, TRUE);

	return(RDS_fingerprint);
}

/**
 * load an RDS into a GHashTable
 *
 * @param RDS_fingerprint the set of rds_id returned by tagsistant_RDS_prepare()
 * @param conn a dbi_conn handle
 * @return a GHashTable of tagsistant_filehandle
 */
GHashTable *tagsistant_RDS_load(gchar *RDS_fingerprint, dbi_conn conn)
{
	GHashTable *file_hash = g_hash_table_new(g_str_hash, g_str_equal);

	tagsistant_query(
		"select distinct objectname, inode from RDS where rds_id in (%s)",
		conn,
		tagsistant_RDS_add_entry_callback,
		file_hash,
		RDS_fingerprint);

	return (file_hash);
}

/**
 * Destroy a filetree element GList list of tagsistant_file_handle.
 * This will free the GList data structure by first calling
 * tagsistant_filetree_destroy_value() on each linked node.
 */
void tagsistant_RDS_destroy_value_list(gchar *key, GList *list, gpointer data)
{
	(void) data;

	g_free_null(key);

	if (list) g_list_free_full(list, (GDestroyNotify) g_free);
}

/**
 * return true if an object is included in a set of RDS
 * specified by an RDS fingerprint
 *
 * @param RDS_fingerprint the string returned by tagsistant_RDS_prepare
 * @param conn a dbi_conn handle
 * @param objectname the name of the object
 * @param inode the optional inode of the object
 * @return true if the object exists, false otherwise
 */
tagsistant_inode tagsistant_RDS_contains_object(tagsistant_querytree *qtree)
{
	tagsistant_inode exists = 0;

	if (qtree->inode) {
		tagsistant_query(
			"select inode from RDS where objectname = '%s' and inode = %d and rds_id in (%s)",
			qtree->dbi,
			tagsistant_return_integer,
			&exists,
			qtree->object_path,
			qtree->inode,
			qtree->RDS_fingerprint);
	} else {
		tagsistant_query(
			"select inode from RDS where objectname = '%s' and rds_id in (%s)",
			qtree->dbi,
			tagsistant_return_integer,
			&exists,
			qtree->object_path,
			qtree->RDS_fingerprint);
	}

	return (exists);
}

void tagsistant_RDS_invalidate_single_tag(qtree_and_node *and, dbi_conn conn)
{
	if (and->tag) {
		tagsistant_query(
			"delete from RDS where rds_id in ("
				"select rds_id from RDS_catalog where subquery like '%%%s%%')",
			conn, NULL, NULL, and->tag);

		tagsistant_query(
			"delete from RDS_catalog where subquery like '%%%s%%'",
			conn, NULL, NULL, and->tag);
	} else {
		tagsistant_query(
			"delete from RDS where rds_id in ("
				"select rds_id from RDS_catalog where subquery like '%%%s/%s%%')",
			conn, NULL, NULL, and->namespace, and->key);

		tagsistant_query(
			"delete from RDS_catalog where subquery like '%%%s/%s%%'",
			conn, NULL, NULL, and->namespace, and->key);
	}
}

/**
 * invalidate all the RDS involved in a query
 *
 * @param query a qtree_or_node holding a query
 * @param conn a dbi_conn handle
 */
void tagsistant_RDS_invalidate(tagsistant_querytree *qtree)
{
	tagsistant_query(
		"update RDS_catalog set expired = 1 where rds_id in (%s)",
		qtree->dbi, NULL, NULL, qtree->RDS_fingerprint);

#if 0
	while (query) {
		qtree_and_node *and = query->and_set;
		while (and) {
			qtree_and_node *negated = and->negated;
			while (negated) {
				tagsistant_RDS_invalidate_single_tag(negated, conn);
				negated = negated->negated;
			}

			tagsistant_RDS_invalidate_single_tag(and, conn);
			and = and->next;
		}

		query = query->next;
	}
#endif
}

#if 0
/**
 * SQL callback. Return an integer from a query
 *
 * @param return_integer integer pointer cast to void* which holds the integer to be returned
 * @param result dbi_result pointer
 * @return 0 (always, due to SQLite policy, may change in the future)
 */
int tagsistant_RDX_expand_single_tag_callback(void *qtree_pointer, dbi_result result)
{
	uint32_t rds_id = 0;
	tagsistant_return_integer(&rds_id, result);

	tagsistant_querytree *qtree = (tagsistant_querytree *) qtree_pointer;

	tagsistant_query(
		"insert into RDS (rds_id, inode, objectname) values (%d, %d, '%s')",
		qtree->dbi,
		NULL,
		NULL,
		rds_id,
		qtree->inode,
		qtree->object_path);

	return (0);
}


void tagsistant_RDS_expand_single_tag(qtree_and_node *and, tagsistant_querytree *qtree)
{
	if (and->tag) {
		tagsistant_query(
			"select rds_id from RDS_catalog "
				"where subquery like '%%/%s/%%' "
				"or subquery like '%s/%%' "
				"or subquery like '%%/%s'",
			qtree->dbi,
			tagsistant_RDX_expand_single_tag_callback,
			qtree,
			and->tag,
			and->tag,
			and->tag);

	} else {
		tagsistant_query(
			"select rds_id from RDS_catalog "
				"where subquery like '%s/%s/eq/%s/%%' "
				"or subquery like '%%/%s/%s/eq/%s/%%'",
			qtree->dbi,
			tagsistant_RDX_expand_single_tag_callback,
			qtree,
			and->namespace, and->key, and->value,
			and->namespace, and->key, and->value);

		tagsistant_query(
			"delete from RDS where rds_id in ("
				"select rds_id from RDS_catalog "
				"where subquery like '%%/%s/%s/inc/%%' "
				"or subquery like '%%/%s/%s/gt/%%' "
				"or subquery like '%%/%s/%s/lt/%%' "
				"or subquery like '%s/%s/inc/%%' "
				"or subquery like '%s/%s/gt/%%' "
				"or subquery like '%s/%s/lt/%%' ",
			qtree->dbi,
			NULL,
			NULL,
			and->namespace, and->key,
			and->namespace, and->key,
			and->namespace, and->key,
			and->namespace, and->key,
			and->namespace, and->key,
			and->namespace, and->key);
	}
}

void tagsistant_RDS_expand(tagsistant_querytree *qtree)
{
	tagsistant_file_handle *fh = g_new0(tagsistant_file_handle, 1);
	memcpy(fh->name, qtree->object_path, strlen(qtree->object_path));
	fh->inode = qtree->inode;

	qtree_or_node *query = qtree->tree;
	while (query) {
		qtree_and_node *and = query->and_set;
		while (and) {
			tagsistant_RDS_expand_single_tag(and, qtree);
			and = and->next;
		}

		query = query->next;
	}

	g_free(fh);
}
#endif
