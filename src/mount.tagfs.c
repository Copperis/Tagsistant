/*
   TAGFS -- mount.tagfs.c
   Copyright (C) 2006-2007 Tx0 <tx0@autistici.org>

   TAGFS mount binary written using FUSE userspace library.

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

#define REGISTER_CLEANUP 0
#include "mount.tagfs.h"

int debug = 0;
int log_enabled = 0;

/* defines command line options for tagfs mount tool */
/* static */ struct tagfs tagfs;

#ifdef _DEBUG_SYSLOG
void init_syslog()
{
	if (log_enabled)
		return;

	openlog("mount.tagfs", LOG_PID, LOG_DAEMON);
	log_enabled = 1;
}
#endif

static int report_if_exists(void *exists_buffer, int argc, char **argv, char **azColName)
{
	(void) argc;
	(void) azColName;
	int *exists = (int *) exists_buffer;
	if (argv[0] != NULL) {
		*exists = 1;
	} else {
		*exists = 0;
	}
	return 0;
}

/**
 * return 1 if file filename is tagged with tag tagname, 0 otherwise.
 */
int is_tagged(char *filename, char *tagname)
{
	int exists = 0;
	char *statement = calloc(sizeof(char), strlen(IS_TAGGED) + strlen(filename) + strlen(tagname));
	if (statement == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return 1;
	}
	sprintf(statement, IS_TAGGED, filename, tagname);
	assert(strlen(IS_TAGGED) + strlen(filename) + strlen(tagname) > strlen(statement));
	char *sqlerror;
	if (sqlite3_exec(tagfs.dbh, statement, report_if_exists, &exists, &sqlerror) != SQLITE_OK) {
		dbg(LOG_ERR, "SQL error: %s @%s:%d", sqlerror, __FILE__, __LINE__);
		sqlite3_free(sqlerror);
	}
	free(statement);
	return exists;
}

static int drop_single_query(void *voidtagname, int argc, char **argv, char **azColName)
{
	(void) argc;
	(void) azColName;
	char *tagname = voidtagname;

	if (argv[0] == NULL) {
		return 1;
	}

	dbg(LOG_INFO, "Dropping tag %s.%s", argv[0], tagname);

	/* drop all files in cache_results with id == argv[0] */
	char *sql = calloc(sizeof(char), strlen(DROP_FILES) + strlen(argv[0]) + 1);
	if (sql == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return 1;
	}
	sprintf(sql, DROP_FILES, argv[0]);
	assert(strlen(DROP_FILES) + strlen(argv[0]) + 1 > strlen(sql));
	dbg(LOG_INFO, "SQL statement: %s", sql);

	char *sqlerror;
	if (sqlite3_exec(tagfs.dbh, sql, NULL, NULL, &sqlerror) != SQLITE_OK) {
		dbg(LOG_ERR, "SQL error: %s @%s:%d", sqlerror, __FILE__, __LINE__);
		sqlite3_free(sqlerror);
		return 1;
	}
	free(sql);

	return 0;
}

int drop_cached_queries(char *tagname)
{
	char *sql = calloc(sizeof(char), strlen(GET_ID_OF_TAG) + strlen(tagname) * 2 + 1);
	if (sql == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return 0;
	}
	sprintf(sql, GET_ID_OF_TAG, tagname, tagname);
	assert(strlen(GET_ID_OF_TAG) + strlen(tagname) * 2 + 1 > strlen(sql));
	dbg(LOG_INFO, "SQL statement: %s", sql);

	char *sqlerror;
	if (sqlite3_exec(tagfs.dbh, sql, drop_single_query, tagname, &sqlerror) != SQLITE_OK) {
		dbg(LOG_ERR, "SQL error: %s @%s:%d", sqlerror, __FILE__, __LINE__);
		sqlite3_free(sqlerror);
		free(sql);
		return 0;
	}
	free(sql);

	/* then drop the query in the cache_queries table */
	sql = calloc(sizeof(char), strlen(DROP_QUERY) + strlen(tagname) * 2 + 1);
	if (sql == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return 0;
	}
	sprintf(sql, DROP_QUERY, tagname, tagname);
	assert(strlen(DROP_QUERY) + strlen(tagname) * 2 + 1 > strlen(sql));
	dbg(LOG_INFO, "SQL statement: %s", sql);

	if (sqlite3_exec(tagfs.dbh, sql, NULL, NULL, &sqlerror) != SQLITE_OK) {
		dbg(LOG_ERR, "SQL error: %s @%s:%d", sqlerror, __FILE__, __LINE__);
		sqlite3_free(sqlerror);
		free(sql);
		return 0;
	}
	free(sql);

	return 1;
}

/**
 * add tag tagname to file filename
 */
int tag_file(char *filename, char *tagname)
{
	char *statement = NULL;
	char *sqlerror = NULL;

	/* drop cached queries containing tagname */
	drop_cached_queries(tagname);

	/* check if file is already tagged */
	if (is_tagged(filename, tagname)) return 1;

	/* add tag to file */
	statement = calloc(sizeof(char), strlen(TAG_FILE) + strlen(tagname) + strlen(filename));
	if (statement == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return 1;
	}
	sprintf(statement, TAG_FILE, tagname, filename);
	assert(strlen(TAG_FILE) + strlen(tagname) + strlen(filename) > strlen(statement));
	if (sqlite3_exec(tagfs.dbh, statement, NULL, NULL, &sqlerror) != SQLITE_OK) {
		dbg(LOG_ERR, "SQL error: %s @%s:%d", sqlerror, __FILE__, __LINE__);
		sqlite3_free(sqlerror);
	}
	free(statement);
	dbg(LOG_INFO, "File %s tagged as %s", filename, tagname);

	/* check if tag is already in db */
	statement = calloc(sizeof(char), strlen(TAG_EXISTS) + strlen(tagname));
	if (statement == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return 0;
	}
	sprintf(statement, TAG_EXISTS, tagname);
	assert(strlen(TAG_EXISTS) + strlen(tagname) > strlen(statement));
	dbg(LOG_INFO, "SQL statement: %s", statement);
	char exists = 0;
	if (sqlite3_exec(tagfs.dbh, statement, report_if_exists, &exists, &sqlerror) != SQLITE_OK) {
		dbg(LOG_ERR, "SQL error: %s @%s:%d", sqlerror, __FILE__, __LINE__);
		sqlite3_free(sqlerror);
	}
	free(statement);
	if (exists) {
		dbg(LOG_INFO, "Tag %s already exists", tagname);
		return 1;
	}

	/* add tag to taglist */
	dbg(LOG_INFO, "Tag %s don't exists, creating it...", tagname);
	statement = calloc(sizeof(char), strlen(CREATE_TAG) + strlen(tagname));
	if (statement == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return 1;
	}
	sprintf(statement, CREATE_TAG, tagname);
	assert(strlen(CREATE_TAG) + strlen(tagname) > strlen(statement));
	dbg(LOG_INFO, "SQL statement: %s", statement);
	if (sqlite3_exec(tagfs.dbh, statement, NULL, NULL, &sqlerror) != SQLITE_OK) {
		dbg(LOG_ERR, "SQL error: %s @%s:%d", sqlerror, __FILE__, __LINE__);
		sqlite3_free(sqlerror);
	}
	free(statement);
	dbg(LOG_INFO, "Tag %s created", tagname);

	return 1;
}

int is_cached(const char *path)
{
	char *mini;
	char *sqlerror;
	int result;

	/* first clean cache table from old data */
	result = sqlite3_exec(tagfs.dbh, CLEAN_CACHE, NULL, NULL, &sqlerror); 
	if (result != SQLITE_OK) {
		dbg(LOG_ERR, "SQL error: %s @%s:%d", sqlerror, __FILE__, __LINE__);
		sqlite3_free(sqlerror);
		return 0;
	}

	mini = calloc(sizeof(char), strlen(IS_CACHED) + strlen(path) + 1);
	if (mini == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return 0;
	}
	sprintf(mini, IS_CACHED, path);
	assert(strlen(IS_CACHED) + strlen(path) + 1 > strlen(mini));

	int exists = 0;
	result = sqlite3_exec(tagfs.dbh, mini, report_if_exists, &exists, &sqlerror); 
	if (result != SQLITE_OK) {
		dbg(LOG_ERR, "SQL error: %s @%s:%d", sqlerror, __FILE__, __LINE__);
		sqlite3_free(sqlerror);
	}

	free(mini);
	return exists;
}

/**
 * lstat equivalent
 */
static int tagfs_getattr(const char *path, struct stat *stbuf)
{
    int res = 0, tagfs_errno = 0;

	init_time_profile();
	start_time_profile();
	if (strcmp(path, "/") == 0) {
		dbg(LOG_INFO, "GETATTR on /!");
		res = lstat(tagfs.archive, stbuf);
		return (res == -1) ? -errno : 0;
	}

	/* last is the last token in path */
	char *dup = strdup(path);
	char *last  = rindex(dup, '/');
	*last = '\0';
	last++;

	/* special case */
	if ((strcasecmp(last, "AND") == 0)
	 || (strcasecmp(last, "OR") == 0)) {
	 	dbg(LOG_INFO, "GETATTR on AND/OR logical operator!");
	 	lstat(tagfs.archive, stbuf);	
		return 0;
	}

	/*
	 * last2 is the token before last (can be null for 1 token paths, like "/photos",
	 * where last == "photos" and last2 == NULL)
	 */
	char *last2 = rindex(dup, '/');
	if (last2 != NULL) last2++;

	/* last token in path is a file or a tag? */
	if ((last2 == NULL) || (strcmp(last2, "AND") == 0) || (strcmp(last2, "OR") == 0)) {
		/* is a dir-tag */
		/* last is the name of the tag */
		dbg(LOG_INFO, "GETATTR on tag: '%s'", last);
		char *statement = calloc(sizeof(char), strlen(TAG_EXISTS) + strlen(last));
		if (statement == NULL) {
			dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
			return 0;
		}
		sprintf(statement, TAG_EXISTS, last);
		assert(strlen(TAG_EXISTS) + strlen(last) > strlen(statement));
		dbg(LOG_INFO, "SQL: %s", statement);

		int exists = 0;
		char *sqlerror;
		int sqlcode = sqlite3_exec(tagfs.dbh, statement, report_if_exists, &exists, &sqlerror);
		if ( sqlcode != SQLITE_OK) {
			dbg(LOG_ERR, "SQL error [%d]: %s @%s:%d", sqlcode, sqlerror, __FILE__, __LINE__);
			sqlite3_free(sqlerror);
		}
		free(statement);

		if (exists) {
			res = lstat(tagfs.archive, stbuf);
			tagfs_errno = errno;
		} else {
			res = -1;
			tagfs_errno = ENOENT;
		}
	} else {
		/* is a file */
		/* last is the filename */
		/* last2 is the last tag in path */
		dbg(LOG_INFO, "GETATTR on file: %s", path);

		/* check if file is tagged */

		/* build querytree */
		ptree_or_node_t *pt = build_querytree(path);
		if (pt == NULL) {
			dbg(LOG_ERR, "Error building querytree @%s:%d", __FILE__, __LINE__);
			return -ENOENT;
		}
		ptree_or_node_t *ptx = pt;

		while (ptx != NULL) {
			ptree_and_node_t *andpt = pt->and_set;
			while (andpt != NULL) {
				if (is_tagged(last, andpt->tag)) {
					char *filepath = get_file_path(last);
					res = lstat(filepath, stbuf);
					tagfs_errno = (res == -1) ? errno : 0;
					free(filepath);
					destroy_querytree(pt);
					goto GETATTR_EXIT;
				} else {
					res = -1;
					tagfs_errno = ENOENT;
				}
				andpt = andpt->next;
			}
			ptx = ptx->next;
		}
		destroy_querytree(pt);
		res = -1;
		tagfs_errno = ENOENT;
	}

GETATTR_EXIT:

	free(dup);
	/* last and last2 are not malloc()ated! free(last) and free(last2) are errors! */

	stop_labeled_time_profile("getattr");
	if ( res == -1 ) {
		dbg(LOG_ERR, "GETATTR exited: %d %d: %s", res, tagfs_errno, strerror(tagfs_errno));
		return -tagfs_errno;
	} else {
		dbg(LOG_INFO, "GETATTR exited: %d", res);
		return 0;
	}
}

static int tagfs_readlink(const char *path, char *buf, size_t size)
{
	char *filename = get_tag_name(path);
	char *filepath = get_file_path(filename);
	free(filename);

	dbg(LOG_INFO, "READLINK on %s", filepath);

	int res = readlink(filepath, buf, size);
	int tagfs_errno = errno;

	free(filepath);
	return (res == -1) ? -tagfs_errno : 0;
}

struct use_filler_struct {
	fuse_fill_dir_t filler;
	void *buf;
	long int path_id;
	int add_to_cache;
};

static int add_entry_to_dir(void *filler_ptr, int argc, char **argv, char **azColName)
{
	struct use_filler_struct *ufs = (struct use_filler_struct *) filler_ptr;
	(void) argc;
	(void) azColName;

	if (argv[0] == NULL || strlen(argv[0]) == 0)
		return 0;

	dbg(LOG_INFO, "add_entry_to_dir: + %s", argv[0]);

	/* add also to cache */
	if (ufs->add_to_cache) {
		char *mini = calloc(sizeof(char), strlen(ADD_RESULT_ENTRY) + strlen(argv[0]) + 14);
		if (mini == NULL) {
			dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		} else {
			sprintf(mini, ADD_RESULT_ENTRY, (int64_t) ufs->path_id, argv[0]);
			assert(strlen(ADD_RESULT_ENTRY) + strlen(argv[0]) + 14 > strlen(mini));
			char *sqlerror;
			int result = sqlite3_exec(tagfs.dbh, mini, add_entry_to_dir, ufs, &sqlerror); 
			if (result != SQLITE_OK) {
				dbg(LOG_ERR, "SQL error: %s @%s:%d", sqlerror, __FILE__, __LINE__);
				sqlite3_free(sqlerror);
			}
			free(mini);
		}
	}

	return ufs->filler(ufs->buf, argv[0], NULL, 0);
}

static int tagfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
	(void) fi;
	(void) offset;

	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

	dbg(LOG_INFO, "READDIR on %s", path);

	char *tagname = get_tag_name(path);
	if (
		strlen(tagname) &&
		(strcasecmp(tagname,"AND") != 0) &&
		(strcasecmp(tagname,"OR") != 0)
	) {

		dbg(LOG_INFO, "%s is a tag(set)", path);

		/*
		 * if path does not terminates with a logical operator,
		 * directory should be filled with logical operators and
		 * with files from the filetree build on path
		 */

		/* add logical operators */
		filler(buf, "AND", NULL, 0);
		filler(buf, "OR", NULL, 0);

		if (is_cached(path)) {
			/* query result exists in cache */
			dbg(LOG_INFO, "Getting %s from cache", path);

			struct use_filler_struct *ufs = calloc(sizeof(struct use_filler_struct), 1);
			if (ufs == NULL) {
				dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
				return 1;
			}
	
			ufs->filler = filler;
			ufs->buf = buf;
			ufs->add_to_cache = 0;
	
			/* parse tagsdir list */
			char *mini = calloc(sizeof(char), strlen(ALL_FILES_IN_CACHE) + strlen(path) + 1);
			if (mini == NULL) {
				dbg(LOG_ERR, "Error allocating SQL statement @%s:%d", __FILE__, __LINE__);
				return 1;
			}
			sprintf(mini, ALL_FILES_IN_CACHE, path);
			assert(strlen(ALL_FILES_IN_CACHE) + strlen(path) + 1 > strlen(mini));
	
			char *sqlerror;
			int result = sqlite3_exec(tagfs.dbh, mini, add_entry_to_dir, ufs, &sqlerror); 
			if (result != SQLITE_OK) {
				dbg(LOG_ERR, "SQL error: %s @%s:%d", sqlerror, __FILE__, __LINE__);
				sqlite3_free(sqlerror);
			}
	
			free(mini);
			return 0;
		}

		char *pathcopy = strdup(path);
	
		ptree_or_node_t *pt = build_querytree(pathcopy);
		if (pt == NULL)
			return -EBADF;
	
		file_handle_t *fh = build_filetree(pt, path);
		if (fh == NULL)
			return -EBADF;
	
		do {
			if ( (fh->name != NULL) && strlen(fh->name)) {
				dbg(LOG_INFO, "Adding %s to directory", fh->name);
				if (filler(buf, fh->name, NULL, offset))
					break;
			}
			fh = fh->next;
		} while ( fh != NULL && fh->name != NULL );
	
		destroy_querytree(pt);
		destroy_file_tree(fh);

	} else {

		dbg(LOG_INFO, "%s terminate with and operator or is root", path);
		char *sqlerror;
		int result;

		/*
		 * if path does terminate with a logical operator
		 * directory should be filled with tagsdir registered tags
		 */
		struct use_filler_struct *ufs = calloc(sizeof(struct use_filler_struct), 1);
		if (ufs == NULL) {
			dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
			free(tagname);
			return 0;
		}

		ufs->filler = filler;
		ufs->buf = buf;
		ufs->add_to_cache = 0;

		/* parse tagsdir list */
		result = sqlite3_exec(tagfs.dbh, GET_ALL_TAGS, add_entry_to_dir, ufs, &sqlerror); 
		if (result != SQLITE_OK) {
			dbg(LOG_ERR, "SQL error: %s @%s:%d", sqlerror, __FILE__, __LINE__);
			sqlite3_free(sqlerror);
		}

		free(ufs);
	}
	free(tagname);

	return 0;
}

/* OK */
static int tagfs_mknod(const char *path, mode_t mode, dev_t rdev)
{
	int res = 0, tagfs_errno = 0;

	init_time_profile();
	start_time_profile();

	char *filename = get_tag_name(path);
	char *fullfilename = get_file_path(filename);

	dbg(LOG_INFO, "MKNOD on %s", path);

	res = mknod(fullfilename, mode, rdev);
	tagfs_errno = errno;

	/* tag the file */
	char *path_dup = strdup(path);
	if (path_dup != NULL) {
		dbg(LOG_INFO, "path_dup is %s", path_dup);
		char *tagname = path_dup + 1;
		char *ri = rindex(path_dup, '/');
		if (ri != NULL) {
			dbg(LOG_INFO, "Filename should be %s", ri + 1);
			*ri = '\0';
			ri = rindex(path_dup, '/');
			if (ri == NULL) {
				ri = path_dup;
			} else {
				ri++;
			}
			tagname = ri;
			dbg(LOG_INFO, "Tagname should be %s", tagname);
		}

		dbg(LOG_INFO, "Tagging file %s as %s inside tagfs_mknod()", filename, tagname);
		tag_file(filename, tagname);
		assert(path_dup != NULL);
		assert(strlen(path_dup));
		free(path_dup);
	}

	dbg(LOG_INFO, "Tagging done, if was the case");

	free(filename);
	free(fullfilename);

	stop_labeled_time_profile("mknod");
	return (res == -1) ? -tagfs_errno: 0;
}

/* OK */
static int tagfs_mkdir(const char *path, mode_t mode)
{
	(void) mode;
    int res = 0, tagfs_errno = 0;
	init_time_profile();
	start_time_profile();

	char *tagname = get_tag_name(path);

	dbg(LOG_INFO, "MKDIR on %s", tagname);

	char *statement = calloc(sizeof(char), strlen(CREATE_TAG) + strlen(tagname));
	if (statement == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return 1;
	}
	sprintf(statement, CREATE_TAG, tagname);
	assert(strlen(CREATE_TAG) + strlen(tagname) > strlen(statement));
	char *sqlerror;
	if (sqlite3_exec(tagfs.dbh, statement, NULL, NULL, &sqlerror) != SQLITE_OK) {
		dbg(LOG_ERR, "SQL error: %s @%s:%d", sqlerror, __FILE__, __LINE__);
		sqlite3_free(sqlerror);
	}

	free(statement);
	free(tagname);

	stop_labeled_time_profile("mkdir");
	return (res == -1) ? -tagfs_errno : 0;
}

static int drop_single_file(void *filenamevoid, int argc, char **argv, char **azColName)
{
	(void) argc;
	(void) azColName;
	char *filename = (char *) filenamevoid;

	if (argv[0] == NULL) {
		return 0;
	}

	char *sql = calloc(sizeof(char), strlen(DROP_FILE) + strlen(filename) + strlen(argv[0]) + 1);
	if (sql == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return 0;
	}

	sprintf(sql, DROP_FILE, filename, argv[0]);
	assert(strlen(DROP_FILE) + strlen(filename) + strlen(argv[0]) + 1 > strlen(sql));
	dbg(LOG_INFO, "SQL statement: %s", sql);
	char *sqlerror;
	if (sqlite3_exec(tagfs.dbh, sql, NULL, NULL, &sqlerror) != SQLITE_OK) {
		dbg(LOG_ERR, "SQL error: %s @%s:%d", sqlerror, __FILE__, __LINE__);
		sqlite3_free(sqlerror);
	}
	free(sql);
	
	return 0;
}

/* OK */
static int tagfs_unlink(const char *path)
{
    int res = 0, tagfs_errno = 0;

	init_time_profile();
	start_time_profile();

	char *filename = get_tag_name(path);

	dbg(LOG_INFO, "UNLINK on %s", filename);

	ptree_or_node_t *pt = build_querytree(path);
	ptree_or_node_t *ptx = pt;

	char *statement1 = calloc(sizeof(char), strlen(UNTAG_FILE) + strlen(filename) + MAX_TAG_LENGTH);
	if (statement1 == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return 1;
	}
	char *statement2 = calloc(sizeof(char), strlen(GET_ID_OF_TAG) + MAX_TAG_LENGTH * 2);
	if (statement2 == NULL) {
		free(statement1);
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return 1;
	}

	while (ptx != NULL) {
		ptree_and_node_t *element = ptx->and_set;
		while (element != NULL) {
			dbg(LOG_INFO, "removing tag %s from %s", element->tag, filename);

			/* should check here if strlen(element->tag) > MAX_TAG_LENGTH */
			sprintf(statement1, UNTAG_FILE, element->tag, filename);
			assert(strlen(UNTAG_FILE) + strlen(filename) + MAX_TAG_LENGTH > strlen(statement1));
			char *sqlerror;
			dbg(LOG_INFO, "SQL statement: %s", statement1);
			if (sqlite3_exec(tagfs.dbh, statement1, NULL, NULL, &sqlerror) != SQLITE_OK) {
				dbg(LOG_ERR, "SQL error: %s @%s:%d", sqlerror, __FILE__, __LINE__);
				sqlite3_free(sqlerror);
			}

			sprintf(statement2, GET_ID_OF_TAG, element->tag, element->tag);
			assert(strlen(GET_ID_OF_TAG) + MAX_TAG_LENGTH * 2 > strlen(statement2));
			dbg(LOG_INFO, "SQL statement: %s", statement2);
			if (sqlite3_exec(tagfs.dbh, statement2, drop_single_file, filename, &sqlerror) != SQLITE_OK) {
			dbg(LOG_ERR, "SQL error: %s @%s:%d", sqlerror, __FILE__, __LINE__);
				sqlite3_free(sqlerror);
			}

			element = element->next;
		}
		ptx = ptx->next;
	}

	free(statement1);
	free(statement2);
	destroy_querytree(pt);

	/*
	 * TODO check if file is no longer tagged in "tagged" table.
	 * if no occurrence appear, delete file also from /archive/ directory.
	 */
	statement1 = calloc(sizeof(char), strlen(HAS_TAGS) + strlen(filename) + 1);
	if (statement1 == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
	} else {
		sprintf(statement1, HAS_TAGS, filename);
		assert(strlen(HAS_TAGS) + strlen(filename) + 1 > strlen(statement1));
		dbg(LOG_INFO, "SQL statement: %s", statement1);

		char *sqlerror;
		int exists = 0;
		if (sqlite3_exec(tagfs.dbh, statement1, report_if_exists, &exists, &sqlerror) != SQLITE_OK) {
			dbg(LOG_ERR, "SQL error: %s @%s:%d", sqlerror, __FILE__, __LINE__);
			sqlite3_free(sqlerror);
		}
		free(statement1);

		if (!exists) {
			/* file is no longer tagged, so can be deleted from archive */
			char *filepath = get_file_path(filename);
			dbg(LOG_INFO, "Unlinking file %s: it's no longer tagged!", filepath);
			res = unlink(filepath);
			tagfs_errno = errno;
			free(filepath);
		}
	}

	free(filename);

	stop_labeled_time_profile("unlink");
	return (res == -1) ? -tagfs_errno : 0;
}

/* OK */
static int tagfs_rmdir(const char *path)
{
    int res = 0, tagfs_errno = 0;
	init_time_profile();
	start_time_profile();

	ptree_or_node_t *pt = build_querytree(path);
	ptree_or_node_t *ptx = pt;

	/* tag name is inserted 2 times in query, that's why '* 2' */
	char *statement = calloc(sizeof(char), strlen(DELETE_TAG) + MAX_TAG_LENGTH * 2);
	if (statement == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return 1;
	}

	while (ptx != NULL) {
		ptree_and_node_t *element = ptx->and_set;
		while (element != NULL) {
			sprintf(statement, DELETE_TAG, element->tag, element->tag);
			assert(strlen(DELETE_TAG) + MAX_TAG_LENGTH * 2 > strlen(statement));
			dbg(LOG_INFO, "RMDIR on %s", element->tag);
			char *sqlerror;
			if (sqlite3_exec(tagfs.dbh, statement, NULL, NULL, &sqlerror) != SQLITE_OK) {
				dbg(LOG_ERR, "SQL error: %s @%s:%d", sqlerror, __FILE__, __LINE__);
				sqlite3_free(sqlerror);
			}
			element = element->next;
		}
		ptx = ptx->next;
	}

	free(statement);
	destroy_querytree(pt);

	stop_labeled_time_profile("rmdir");
	return (res == -1) ? -tagfs_errno : 0;
}

/* OK */
static int tagfs_rename(const char *from, const char *to)
{
    int res = 0, tagfs_errno = 0;

	init_time_profile();
	start_time_profile();

	/* return if "to" path is complex, i.e. including logical operators */
	if ((strstr(to, "/AND") != NULL) || (strstr(to, "/OR") != NULL)) {
		dbg(LOG_ERR, "Logical operators not allowed in open path");
		return -ENOTDIR;
	}

	char *tagname = get_tag_name(from);

	if (rindex(from, '/') == from) {
		/* is a tag */
		const char *newtagname = rindex(to, '/');
		if (newtagname == NULL) newtagname = to;
		char *statement = calloc(sizeof(char), strlen(RENAME_TAG) + strlen(tagname) * 2 + strlen(newtagname) * 2);
		if (statement == NULL) {
			dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
			return 1;
		}
		sprintf(statement, RENAME_TAG, tagname, newtagname, tagname, newtagname);
		assert(strlen(RENAME_TAG) + strlen(tagname) * 2 + strlen(newtagname) * 2 > strlen(statement));
		char *sqlerror;
		if (sqlite3_exec(tagfs.dbh, statement, NULL, NULL, &sqlerror) != SQLITE_OK) {
			dbg(LOG_ERR, "SQL error: %s @%s:%d", sqlerror, __FILE__, __LINE__);
			sqlite3_free(sqlerror);
		}
		free(statement);
	} else {
		/* is a file */
		const char *newfilename = rindex(to, '/');
		if (newfilename == NULL) newfilename = to;
		char *statement = calloc(sizeof(char), strlen(RENAME_FILE) + strlen(tagname) + strlen(newfilename));
		if (statement == NULL) {
			dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
			return 1;
		}
		sprintf(statement, RENAME_FILE, tagname, newfilename);
		assert(strlen(RENAME_FILE) + strlen(tagname) + strlen(newfilename) > strlen(statement));
		char *sqlerror;
		if (sqlite3_exec(tagfs.dbh, statement, NULL, NULL, &sqlerror)) {
			dbg(LOG_ERR, "SQL error: %s @%s:%d", sqlerror, __FILE__, __LINE__);
			sqlite3_free(sqlerror);
		}
		free(statement);

		char *filepath = get_file_path(tagname);
		char *newfilepath = get_file_path(newfilename);
		res = rename(filepath, newfilepath);
		tagfs_errno = errno;
		free(filepath);
		free(newfilepath);
	}

	stop_labeled_time_profile("rename");
	return (res == -1) ? -tagfs_errno : 0;
}

/* OK */
static int tagfs_link(const char *from, const char *to)
{
    int res = 0, tagfs_errno = 0;

	init_time_profile();
	start_time_profile();

	char *filename = get_tag_name(to);
	char *topath = get_file_path(filename);

	char *path_dup = strdup(to);
	char *ri = rindex(path_dup, '/');
	*ri = '\0';
	ri = rindex(path_dup, '/');
	ri++;
	char *tagname = strdup(ri);
	free(path_dup);

	/* tag the link */
	dbg(LOG_INFO, "Tagging file inside tagfs_link() or tagfs_symlink()");
	tag_file(filename, tagname);

	struct stat stbuf;
	if ((lstat(topath, &stbuf) == -1) && (errno == ENOENT)) {
		dbg(LOG_INFO, "Linking %s as %s", from, topath);
		res = symlink(from, topath);
		tagfs_errno = errno;
	} else {
		dbg(LOG_INFO, "A file named %s is already in archive.", filename);
		res = -1;
		tagfs_errno = EEXIST;
	}

	free(tagname);
	free(filename);
	free(topath);

	stop_labeled_time_profile("link");
	return (res == -1) ? -tagfs_errno : 0;
}

/* OK */
static int tagfs_symlink(const char *from, const char *to)
{
	return tagfs_link(from, to);
}

/* OK */
static int tagfs_chmod(const char *path, mode_t mode)
{
    int res = 0, tagfs_errno = 0;

	init_time_profile();
	start_time_profile();

	char *tagname = get_tag_name(path);
	char *filepath = get_file_path(tagname);

	res = chmod(filepath, mode);
	tagfs_errno = errno;

	free(tagname);
	free(filepath);

	stop_labeled_time_profile("chmod");
	return (res == -1) ? -tagfs_errno : 0;
}

/* OK */
static int tagfs_chown(const char *path, uid_t uid, gid_t gid)
{
    int res = 0, tagfs_errno = 0;

	init_time_profile();
	start_time_profile();

	char *tagname = get_tag_name(path);
	char *filepath = get_file_path(tagname);

	res = chown(filepath, uid, gid);
	tagfs_errno = errno;

	free(tagname);
	free(filepath);

	stop_labeled_time_profile("chown");
	return (res == -1) ? -tagfs_errno : 0;
}

/* OK */
static int tagfs_truncate(const char *path, off_t size)
{
    int res = 0, tagfs_errno = 0;

	init_time_profile();
	start_time_profile();

	char *tagname = get_tag_name(path);
	char *filepath = get_file_path(tagname);

	res = truncate(filepath, size);
	tagfs_errno = errno;

	free(tagname);
	free(filepath);

	stop_labeled_time_profile("truncate");
	return (res == -1) ? -tagfs_errno : 0;
}

/* OK */
static int tagfs_utime(const char *path, struct utimbuf *buf)
{
    int res = 0, tagfs_errno = 0;
	(void) path;

	init_time_profile();
	start_time_profile();

	char *tagname = get_tag_name(path);
	char *filepath = get_file_path(tagname);

	res = utime(filepath, buf);
	tagfs_errno = errno;

	free(tagname);
	free(filepath);

	stop_labeled_time_profile("utime");
	return (res == -1) ? -errno : 0;
}

/* OK */
int internal_open(const char *path, int flags, int *_errno)
{
	char *filename = get_tag_name(path);
	char *filepath = get_file_path(filename);
	char *path_dup = strdup(path);
	char *ri = rindex(path_dup, '/');
	*ri = '\0';
	ri = rindex(path_dup, '/');
	ri++;
	char *tagname = strdup(ri);
	free(path_dup);

	dbg(LOG_INFO, "INTERNAL_OPEN: Opening file %s", filepath);

	if (flags&O_CREAT) dbg(LOG_INFO, "...O_CREAT");
	if (flags&O_WRONLY) dbg(LOG_INFO, "...O_WRONLY");
	if (flags&O_TRUNC) dbg(LOG_INFO, "...O_TRUNC");
	if (flags&O_LARGEFILE) dbg(LOG_INFO, "...O_LARGEFILE");

	if (flags&O_CREAT || flags&O_WRONLY || flags&O_TRUNC) {
		dbg(LOG_INFO, "Tagging file inside internal_open()");
		tag_file(filename, tagname);
	}

	int res = open(filepath, flags);
	*_errno = errno;

	free(tagname);
	free(filename);
	free(filepath);
	return res;
}

/* OK */
static int tagfs_open(const char *path, struct fuse_file_info *fi)
{
    int res = 0, tagfs_errno = 0;

	init_time_profile();
	start_time_profile();

	dbg(LOG_INFO, "OPEN: %s", path);

	res = internal_open(path, fi->flags|O_RDONLY|O_CREAT, &tagfs_errno);

	stop_labeled_time_profile("open");
	return (res == -1) ? -tagfs_errno : 0;
}

/* OK */
static int tagfs_read(const char *path, char *buf, size_t size, off_t offset,
                    struct fuse_file_info *fi)
{
    int res = 0, tagfs_errno = 0;

	init_time_profile();
	start_time_profile();

	int fd = internal_open(path, fi->flags|O_RDONLY, &tagfs_errno); 
	if (fd != -1) {
		res = pread(fd, buf, size, offset);
		tagfs_errno = errno;
		close(fd);
	}

	stop_labeled_time_profile("read");
	return (res == -1) ? -tagfs_errno : res;
}

/* OK */
static int tagfs_write(const char *path, const char *buf, size_t size,
	off_t offset, struct fuse_file_info *fi)
{
    int res = 0, tagfs_errno = 0;

	init_time_profile();
	start_time_profile();

	/* return if path is complex, i.e. including logical operators */
	if ((strstr(path, "/AND") != NULL) || (strstr(path, "/OR") != NULL)) {
		dbg(LOG_ERR, "Logical operators not allowed in open path");
		return -ENOTDIR;
	}

	int fd = internal_open(path, fi->flags|O_WRONLY, &tagfs_errno); 
	if (fd != -1) {
		dbg(LOG_INFO, "writing %d bytes to %s", size, path);
		res = pwrite(fd, buf, size, offset);
		tagfs_errno = errno;

		if (res == -1)
			dbg(LOG_INFO, "Error on fd.%d: %s", fd, strerror(tagfs_errno));

		close(fd);

	}

	if (res == -1)
		dbg(LOG_INFO, "WRITE: returning %d: %s", tagfs_errno, strerror(tagfs_errno));

	stop_labeled_time_profile("write");
	return (res == -1) ? -tagfs_errno : res;
}

#if FUSE_USE_VERSION == 25

/* OK */
static int tagfs_statvfs(const char *path, struct statvfs *stbuf)
{
    int res = 0, tagfs_errno = 0;
	(void) path;

	init_time_profile();
	start_time_profile();
	
	res = statvfs(tagfs.repository, stbuf);
	tagfs_errno = errno;

	stop_labeled_time_profile("statfs");
    return (res == -1) ? -tagfs_errno : 0;
}

#else

/* OK */
static int tagfs_statfs(const char *path, struct statfs *stbuf)
{
    int res = 0, tagfs_errno = 0;
	(void) path;

	init_time_profile();
	start_time_profile();
	
	res = statfs(tagfs.repository, stbuf);
	tagfs_errno = errno;

	stop_labeled_time_profile("statfs");
    return (res == -1) ? -tagfs_errno : 0;
}

#endif

static int tagfs_release(const char *path, struct fuse_file_info *fi)
{
    /* Just a stub.  This method is optional and can safely be left
       unimplemented */

    (void) path;
    (void) fi;

	return 0; /* REMOVE ME AFTER CODING */
}

static int tagfs_fsync(const char *path, int isdatasync,
                     struct fuse_file_info *fi)
{
    /* Just a stub.  This method is optional and can safely be left
       unimplemented */

    (void) path;
    (void) isdatasync;
    (void) fi;

	return 0; /* REMOVE ME AFTER CODING */
}

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int tagfs_setxattr(const char *path, const char *name, const char *value,
                        size_t size, int flags)
{
    int res;

	return 0; /* REMOVE ME AFTER CODING */
}

static int tagfs_getxattr(const char *path, const char *name, char *value,
                    size_t size)
{
    int res;
	return 0; /* REMOVE ME AFTER CODING */
}

static int tagfs_listxattr(const char *path, char *list, size_t size)
{
    int res;

	return 0; /* REMOVE ME AFTER CODING */
}

static int tagfs_removexattr(const char *path, const char *name)
{
    int res;

	return 0; /* REMOVE ME AFTER CODING */
}
#endif /* HAVE_SETXATTR */

static void *tagfs_init(void)
{
	return 0;
}

static struct fuse_operations tagfs_oper = {
    .getattr	= tagfs_getattr,
    .readlink	= tagfs_readlink,
    .readdir	= tagfs_readdir,
    .mknod		= tagfs_mknod,
    .mkdir		= tagfs_mkdir,
    .symlink	= tagfs_symlink,
    .unlink		= tagfs_unlink,
    .rmdir		= tagfs_rmdir,
    .rename		= tagfs_rename,
    .link		= tagfs_link,
    .chmod		= tagfs_chmod,
    .chown		= tagfs_chown,
    .truncate	= tagfs_truncate,
    .utime		= tagfs_utime,
    .open		= tagfs_open,
    .read		= tagfs_read,
    .write		= tagfs_write,
#if FUSE_USE_VERSION == 25
    .statfs		= tagfs_statvfs,
#else
    .statfs		= tagfs_statfs,
#endif
    .release	= tagfs_release,
    .fsync		= tagfs_fsync,
#ifdef HAVE_SETXATTR
    .setxattr	= tagfs_setxattr,
    .getxattr	= tagfs_getxattr,
    .listxattr	= tagfs_listxattr,
    .removexattr= tagfs_removexattr,
#endif
	.init		= tagfs_init,
};

enum {
    KEY_HELP,
    KEY_VERSION,
};

/* following code got from SSHfs sources */
#define TAGFS_OPT(t, p, v) { t, offsetof(struct tagfs, p), v }

static struct fuse_opt tagfs_opts[] = {
	TAGFS_OPT("-d",					debug,			1),
	TAGFS_OPT("--repository=%s",	repository,		0),
    TAGFS_OPT("-f",					foreground,		1),
    TAGFS_OPT("-s",					singlethread,	1),
    TAGFS_OPT("-r",					readonly,		1),

    FUSE_OPT_KEY("-V",          	KEY_VERSION),
    FUSE_OPT_KEY("--version",   	KEY_VERSION),
    FUSE_OPT_KEY("-h",          	KEY_HELP),
    FUSE_OPT_KEY("--help",      	KEY_HELP),
    FUSE_OPT_END
};

int usage_already_printed = 0;
void usage(char *progname)
{
	if (usage_already_printed++)
		return;

	fprintf(stderr, "\n"
		"TAGFS v.%s\n"
		"Semantic File System for Linux kernels\n"
		"(c) 2006-2007 Tx0 <tx0@autistici.org>\n"
		"FUSE_USE_VERSION: %d\n\n"
		"This program is free software; you can redistribute it and/or modify\n"
		"it under the terms of the GNU General Public License as published by\n"
		"the Free Software Foundation; either version 2 of the License, or\n"
		"(at your option) any later version.\n\n"

		"This program is distributed in the hope that it will be useful,\n"
		"but WITHOUT ANY WARRANTY; without even the implied warranty of\n"
		"MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the\n"
		"GNU General Public License for more details.\n\n"

		"You should have received a copy of the GNU General Public License\n"
		"along with this program; if not, write to the Free Software\n"
		"Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA\n"
		"\n"
		"Usage: %s [OPTIONS] --repository=<PATH> /mountpoint\n"
		"\n"
		"    -u  unmount a mounted filesystem\n"
		"    -q  be quiet\n"
		"    -r  mount readonly\n"
		"    -z  lazy unmount (can be dangerous!)\n"
		"\n" /*fuse options will follow... */
		, PACKAGE_VERSION, FUSE_USE_VERSION, progname
	);
}

static int tagfs_opt_proc(void *data, const char *arg, int key, struct fuse_args *outargs)
{
    (void) data;
	(void) arg;

    switch (key) {
		case FUSE_OPT_KEY_NONOPT:
			if (!tagfs.mountpoint) {
				tagfs.mountpoint = strdup(arg);
				return 1;
			}
			return 0;

	    case KEY_HELP:
	        usage(outargs->argv[0]);
	        fuse_opt_add_arg(outargs, "-ho");
	        fuse_main(outargs->argc, outargs->argv, &tagfs_oper);
	        exit(1);
	
	    case KEY_VERSION:
	        fprintf(stderr, "Tagfs for Linux 0.1 (prerelease %s)\n", VERSION);
#if FUSE_VERSION >= 25
	        fuse_opt_add_arg(outargs, "--version");
	        fuse_main(outargs->argc, outargs->argv, &tagfs_oper);
#endif
	        exit(0);
	
	    default:
	        fprintf(stderr, "Extra parameter provided\n");
	        usage(outargs->argv[0]);
    }

	return 0;
}

void cleanup(int s)
{
	dbg(LOG_ERR, "Got Signal %d in %s:%d", s, __FILE__, __LINE__);
	sqlite3_close(tagfs.dbh);
	exit(s);
}

int main(int argc, char *argv[])
{
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	int res;

	tagfs.progname = argv[0];

	if (fuse_opt_parse(&args, &tagfs, tagfs_opts, tagfs_opt_proc) == -1)
		exit(1);

	/* checking mountpoint */
	if (!tagfs.mountpoint) {
		usage(tagfs.progname);
		fprintf(stderr, "    *** No mountpoint provided *** \n\n");
		exit(2);
	}
	
	/* checking repository */
	if (!tagfs.repository || (strcmp(tagfs.repository, "") == 0)) {
		usage(tagfs.progname);
		fprintf(stderr, "    *** No repository provided with -r ***\n\n");
		exit(2);
	}

	/* removing last slash */
	int replength = strlen(tagfs.repository) - 1;
	if (tagfs.repository[replength] == '/') {
		tagfs.repository[replength] = '\0';
	}

	struct stat repstat;
	if (lstat(tagfs.repository, &repstat) == -1) {
		if(mkdir(tagfs.repository, 755) == -1) {
			fprintf(stderr, "    *** REPOSITORY: Can't mkdir(%s): %s ***\n\n", tagfs.repository, strerror(errno));
			exit(2);
		}
	}
	chmod(tagfs.repository, S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH);

	/* opening (or creating) SQL tags database */
	tagfs.tags = malloc(strlen(tagfs.repository) + strlen("/tags.sql") + 1);
	strcpy(tagfs.tags,tagfs.repository);
	strcat(tagfs.tags,"/tags.sql");

	/* check if db exists or has to be created */
	struct stat dbstat;
	int db_exists = lstat(tagfs.tags, &dbstat);

	/* open connection to sqlite database */
	int result = sqlite3_open(tagfs.tags, &(tagfs.dbh));

	/* check if open has been fullfilled */
	if (result != SQLITE_OK) {
		dbg(LOG_ERR, "Error [%d] opening database %s", result, tagfs.tags);
		dbg(LOG_ERR, "%s", sqlite3_errmsg(tagfs.dbh));
		exit(1);
	}

	/* if db does not existed, some SQL init is needed */
	if (db_exists != 0) {
		char *sqlerror;
		int sqlcode;
		
		sqlcode = sqlite3_exec(tagfs.dbh, CREATE_TAGS_TABLE, NULL, NULL, &sqlerror);
		if (sqlcode != SQLITE_OK) {
			dbg(LOG_ERR, "SQL error [%d] while creating tags table: %s", sqlcode, sqlerror);
			exit(1);
		}
		sqlcode = sqlite3_exec(tagfs.dbh, CREATE_TAGGED_TABLE, NULL, NULL, &sqlerror);
		if (sqlcode != SQLITE_OK) {
			dbg(LOG_ERR, "SQL error [%d] while creating tagged table: %s", sqlcode, sqlerror);
			exit(1);
		}
		sqlcode = sqlite3_exec(tagfs.dbh, CREATE_CACHE_TABLE, NULL, NULL, &sqlerror);
		if (sqlcode != SQLITE_OK) {
			dbg(LOG_ERR, "SQL error [%d] while creating cache main table: %s", sqlcode, sqlerror);
			exit(1);
		}
		sqlcode = sqlite3_exec(tagfs.dbh, CREATE_RESULT_TABLE, NULL, NULL, &sqlerror);
		if (sqlcode != SQLITE_OK) {
			dbg(LOG_ERR, "SQL error [%d] while creating cache results table: %s", sqlcode, sqlerror);
			exit(1);
		}
	}

	/* checking file archive directory */
	tagfs.archive = malloc(strlen(tagfs.repository) + strlen("/archive/") + 1);
	strcpy(tagfs.archive,tagfs.repository);
	strcat(tagfs.archive,"/archive/");

	if (lstat(tagfs.archive, &repstat) == -1) {
		if(mkdir(tagfs.archive, 755) == -1) {
			fprintf(stderr, "    *** ARCHIVE: Can't mkdir(%s): %s ***\n\n", tagfs.archive, strerror(errno));
			exit(2);
		}
	}
	chmod(tagfs.archive, S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH);

	fuse_opt_add_arg(&args, "-odefault_permissions,allow_other,fsname=tagfs");
	fuse_opt_add_arg(&args, "-ouse_ino,readdir_ino");
	if (tagfs.singlethread) {
		fprintf(stderr, " *** operating in single thread mode ***\n");
		fuse_opt_add_arg(&args, "-s");
	}
	if (tagfs.readonly) {
		fprintf(stderr, " *** mounting tagfs read-only ***\n");
		fuse_opt_add_arg(&args, "-r");
	}
	if (tagfs.foreground) {
		fprintf(stderr, " *** will run in foreground ***\n");
		fuse_opt_add_arg(&args, "-f");
	}

	fprintf(stderr, "\n");
	fprintf(stderr,
		" Tag based filesystem for Linux kernels\n"
		" (c) 2006-2007 Tx0 <tx0@autistici.org>\n"
		" For license informations, see %s -h\n"
		" FUSE_USE_VERSION: %d\n\n"
		, tagfs.progname, FUSE_USE_VERSION
	);

	if (tagfs.debug) debug = tagfs.debug;

	if (debug)
		dbg(LOG_INFO, "Debug is enabled");

	umask(0);

#ifdef _DEBUG_SYSLOG
	init_syslog();
#endif

#if REGISTER_CLEANUP
	signal(2,  cleanup); /* SIGINT */
	signal(11, cleanup); /* SIGSEGV */
	signal(15, cleanup); /* SIGTERM */
#endif

	dbg(LOG_INFO, "Mounting filesystem");

	dbg(LOG_INFO, "Fuse options:");
	int fargc = args.argc;
	while (fargc) {
		dbg(LOG_INFO, "%.2d: %s", fargc, args.argv[fargc]);
		fargc--;
	}

	res = fuse_main(args.argc, args.argv, &tagfs_oper);

    fuse_opt_free_args(&args);

	return res;
}

// vim:ts=4:autoindent:nocindent:syntax=c
