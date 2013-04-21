/*
   Tagsistant (tagfs) -- path_resolution.h
   Copyright (C) 2006-2009 Tx0 <tx0@strumentiresistenti.org>

   Tagsistant (tagfs) mount binary written using FUSE userspace library.
   Header file

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

/**
 * defines an AND token in a query path
 */
typedef struct ptree_and_node {
	/** the name of this token */
	char *tag;

	/** list of all related tags **/
	struct ptree_and_node *related;

	/** next AND token */
	struct ptree_and_node *next;
} ptree_and_node;

/**
 * define an OR section in a query path
 */
typedef struct ptree_or_node {
	/** the next OR section */
	struct ptree_or_node *next;

	/** the list of AND tokens */
	struct ptree_and_node *and_set;
} ptree_or_node;

/*
 * depeding on relative path, a query can be one in the following:
 */
typedef enum {
	QTYPE_MALFORMED,	// wrong path (not starting by /tags, /archive, /stats or /relations)
	QTYPE_ROOT,			// no path, that's a special case for root directory
	QTYPE_ARCHIVE,		// path pointing to objects on disk, begins with /archive/
	QTYPE_TAGS,			// path that's a query, begins with /tags/
	QTYPE_RETAG,		// experimental path used for object retagging
	QTYPE_RELATIONS,	// path that's a relation between two or more tags, begins with /relations/
	QTYPE_STATS,		// path that's a special query for internal status, begins with /stats/
	QTYPE_TOTAL
} tagsistant_query_type;

extern gchar *tagsistant_querytree_types[QTYPE_TOTAL];

/**
 * returns the type of query described by a tagsistant_querytree struct
 * WARNING: the returned string MUST NOT BE FREED!
 */
#define tagsistant_querytree_type(qtree) tagsistant_querytree_types[qtree->type]

/*
 * to ease coding, there are some macros to check
 * if a query if of a given type
 */
#define QTREE_IS_MALFORMED(qtree) (QTYPE_MALFORMED == qtree->type)
#define QTREE_IS_ROOT(qtree) (QTYPE_ROOT == qtree->type)
#define QTREE_IS_TAGS(qtree) (QTYPE_TAGS == qtree->type)
#define QTREE_IS_ARCHIVE(qtree) (QTYPE_ARCHIVE == qtree->type)
#define QTREE_IS_RELATIONS(qtree) (QTYPE_RELATIONS == qtree->type)
#define QTREE_IS_STATS(qtree) (QTYPE_STATS == qtree->type)
#define QTREE_IS_RETAG(qtree) (QTYPE_RETAG == qtree->type)

/*
 * if a query points to an object on disk this returns true;
 * that's:
 *
 *   archive/something
 *   tags/t1/t2/.../tN/=/something
 */
#define QTREE_POINTS_TO_OBJECT(qtree) (qtree->points_to_object)

/*
 * some more info about a query:
 * is_taggable -> points_to_object but on first level (so not on tags/t1/t2/.../tN/=/something/more/...)
 * is_complete -> query is of type tags/ and has an =/
 * is_external -> the query points outside tagsistant mountpoint
 * is_internal -> the query points inside tagsistant mountpoint
 */
#define QTREE_IS_TAGGABLE(qtree) (qtree->is_taggable)
#define QTREE_IS_COMPLETE(qtree) (qtree->complete)
#define QTREE_IS_EXTERNAL(qtree) (qtree->is_external)
#define QTREE_IS_INTERNAL(qtree) (!qtree->is_external)

/*
 * two queries are of the same type and are both complete
 * the second is true for tags/ if both are complete,
 * and always for other types of queries
 */
#define QTREES_ARE_SIMILAR(qtree1, qtree2) ((qtree1->type == qtree2->type) && (qtree1->complete == qtree2->complete))

/*
 * check if a path is external to tagsistant mountpoint
 * without requiring query resolution and querytree building
 */
#define TAGSISTANT_PATH_IS_EXTERNAL(path) (g_strstr_len(path, strlen(path), tagsistant.mountpoint) != path)

/**
 * define the querytree structure
 * that holds a tree of ptree_or_node_t
 * and ptree_and_node_t and a string
 * containing the file part of the path.
 */
typedef struct querytree {
	/** the complete path that generated the tree */
	/** i.e. <MPOINT>/tags/t1/+/t2/=/object/path.txt */
	gchar *full_path;

	/** the path of the object, if provided */
	/** i.e. object/path.txt */
	gchar *object_path;

	/** the path of the object on disk */
	/** NNN___object/path.txt */
	gchar *archive_path;

	/** like the previous one, but with current archive path prefixed */
	/** ~/.tagsistant/archive/NNN___object/path.txt */
	gchar *full_archive_path;

	/** the inode of the object, if directly managed by tagsistant */
	tagsistant_inode inode;

	/** which kind of path is this? see tagsistant_query_type */
	int type;

	/** the query points to an object on disk? */
	/** true if its an archive/ query or a complete tags/ query */
	int points_to_object;

	/** the object path pointed to is taggable? (one element path) */
	int is_taggable;

	/** the object is external to tagsistant mountpoint */
	int is_external;

	/** last tag found while parsing a /tags query */
	gchar *last_tag;

	/** the query is valid */
	int valid;

	/** the tags/ query is complete? it has a '=' sign? */
	int complete;

	/** the object pointed by is currently in the database? */
	int exists;

	/** the query tree in a tags/ query */
	ptree_or_node *tree;

	/** the first tag in a relations/ query */
	gchar *first_tag;

	/** the second tag in a relations/ query */
	gchar *second_tag;

	/** the relation in a relations/ query */
	gchar *relation;

	/** the path in a stats/ query */
	gchar *stats_path;

	/** libDBI connection handle */
	dbi_conn dbi;

	/** record if a transaction has been opened on this connection */
	int transaction_started;

	/** last time the cached copy of this querytree has been accessed */
	gint64 last_access_microsecond;
} tagsistant_querytree;

/**
 * used in linked list of returned results
 *
 * Alessandro AkiRoss Re reported a conflict with the structure
 * file_handle in /usr/include/bits/fcntl.h on Fedora 15
 */
typedef struct {
	char *name;					/** object filename */
	tagsistant_inode inode;		/** object inode */
} tagsistant_file_handle;

/**
 * reasoning structure to trace reasoning process
 */
typedef struct {
	ptree_and_node *start_node;
	ptree_and_node *current_node;
	int added_tags;
	dbi_conn conn;
} tagsistant_reasoning;

/**
 * evaluates true if string "relation" matches at least
 * one of available relations
 */
#define IS_VALID_RELATION(relation) ((g_strcmp0(relation, "is_equivalent")) == 0 || (g_strcmp0(relation, "includes") == 0))

/**
 * allows for applying a function to all the ptree_and_node_t nodes of
 * a tagstistant_querytree_t structure. the function applied must be declared as:
 *   void function(ptree_and_node *node, ...)
 * while can be of course provided with fixed length argument list
 *
 * @param qtree the tagsistant_querytree_t structure to traverse
 * @param funcpointer the pointer to the function (barely the function name)
 */
#define tagsistant_querytree_traverse(qtree, funcpointer, ...) {\
	if (NULL != qtree) {\
		ptree_or_node *ptx = qtree->tree;\
		while (NULL != ptx) {\
			ptree_and_node *andptx = ptx->and_set;\
			while (NULL != andptx) {\
				funcpointer(qtree->dbi, andptx->tag, ##__VA_ARGS__);\
				andptx = andptx->next;\
			}\
			ptx = ptx->next;\
		}\
	}\
}

// querytree functions
extern void						tagsistant_path_resolution_init();
extern void						tagsistant_reasoner_init();

extern tagsistant_querytree *	tagsistant_querytree_new(const char *path, int do_reasoning, int assign_inode, int start_transaction);
extern void 					tagsistant_querytree_destroy(tagsistant_querytree *qtree, uint commit_transaction);

extern void						tagsistant_querytree_set_object_path(tagsistant_querytree *qtree, char *new_object_path);
extern void						tagsistant_querytree_set_inode(tagsistant_querytree *qtree, tagsistant_inode inode);
extern void						tagsistant_querytree_rebuild_paths(tagsistant_querytree *qtree);
extern tagsistant_query_type	tagsistant_querytree_guess_type(gchar **token_ptr);
extern void						tagsistant_querytree_deduplicate(tagsistant_querytree *qtree);
extern int						tagsistant_querytree_cache_total();
extern void						tagsistant_invalidate_querytree_cache(tagsistant_querytree *qtree);

extern tagsistant_inode			tagsistant_inode_extract_from_path(tagsistant_querytree *qtree);
extern tagsistant_inode			tagsistant_inode_extract_from_querytree(tagsistant_querytree *qtree);

extern int						tagsistant_reasoner(tagsistant_reasoning *reasoning);

// filetree functions
extern GHashTable *				tagsistant_filetree_new(ptree_or_node *query, dbi_conn conn);
extern void 					tagsistant_filetree_destroy(GHashTable *hash_table);
