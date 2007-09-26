/*
   Tagsistant (tagfs) -- tagsistant.c
   Copyright (C) 2006-2007 Tx0 <tx0@strumentiresistenti.org>

   Tagsistant (tagfs) mount binary written using FUSE userspace library.

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
#include "tagsistant.h"

int debug = 0;
int log_enabled = 0;

/* defines command line options for tagsistant mount tool */
/* static */ struct tagsistant tagsistant;

/*
 * given a full path, returns the filename, the filepath (relative
 * to archive directory) and the tagname
 */
int get_filename_and_tagname(const char *path, char **filename, char **filepath, char **tagname)
{
	*filename = get_tag_name(path);
	*filepath = get_file_path(*filename);
	char *path_dup = strdup(path);
	char *ri = rindex(path_dup, '/');
	*ri = '\0';
	ri = rindex(path_dup, '/');
	ri++;
	*tagname = strdup(ri);
	free(path_dup);
	return 1;
}

#ifdef _DEBUG_SYSLOG
void init_syslog()
{
	if (log_enabled)
		return;

	openlog("tagsistant", LOG_PID, LOG_DAEMON);
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
	do_sql(NULL, statement, report_if_exists, &exists);
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

	int res = do_sql(NULL, sql, NULL, NULL);
	free(sql);

	return res;
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

	if (do_sql(&(tagsistant.dbh), sql, drop_single_query, tagname) != SQLITE_OK) {
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

	if (do_sql(&(tagsistant.dbh), sql, NULL, NULL) != SQLITE_OK) {
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
	do_sql(NULL, statement, NULL, NULL);
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
	char exists = 0;

	do_sql(NULL, statement, report_if_exists, &exists);
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
	do_sql(NULL, statement, NULL, NULL);
	free(statement);
	dbg(LOG_INFO, "Tag %s created", tagname);

	return 1;
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
	do_sql(NULL, sql, NULL, NULL);
	free(sql);
	
	return 0;
}

/**
 * remove tag tagname from file filename
 */
int untag_file(char *filename, char *tagname)
{
	char *statement = calloc(sizeof(char), strlen(UNTAG_FILE) + strlen(filename) + strlen(tagname));
	if (statement == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return 0;
	}
	sprintf(statement, UNTAG_FILE, tagname, filename);
	do_sql(NULL, statement, NULL, NULL);
	free(statement);

	statement = calloc(sizeof(char), strlen(GET_ID_OF_TAG) + strlen(tagname) * 2);
	if (statement == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return 0;
	}
	sprintf(statement, GET_ID_OF_TAG, tagname, tagname);
	do_sql(NULL, statement, drop_single_file, filename);

	return 1;
}

int is_cached(const char *path)
{
	char *mini;

	/* first clean cache table from old data */
	if (do_sql(&(tagsistant.dbh), CLEAN_CACHE, NULL, NULL) != SQLITE_OK) return 0;

	mini = calloc(sizeof(char), strlen(IS_CACHED) + strlen(path) + 1);
	if (mini == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return 0;
	}
	sprintf(mini, IS_CACHED, path);
	assert(strlen(IS_CACHED) + strlen(path) + 1 > strlen(mini));

	int exists = 0;
	do_sql(NULL, mini, report_if_exists, &exists);
	free(mini);
	return exists;
}

/**
 * lstat equivalent
 */
static int tagsistant_getattr(const char *path, struct stat *stbuf)
{
    int res = 0, tagsistant_errno = 0;

	if (strcmp(path, "/") == 0) {
		res = lstat(tagsistant.archive, stbuf);
		if ( res == -1 ) {
			tagsistant_errno = errno;
			dbg(LOG_INFO, "GETATTR on / using %s: %d %s", tagsistant.archive, errno, strerror(tagsistant_errno));
			return -errno;
		}
		dbg(LOG_INFO, "GETATTR on / OK!");
		return 0;
	}

	init_time_profile();
	start_time_profile();

	/* last is the last token in path */
	char *dup = strdup(path);
	char *last  = rindex(dup, '/');
	*last = '\0';
	last++;

	/*
	 * last2 is the token before last (can be null for 1 token paths, like "/photos",
	 * where last == "photos" and last2 == NULL)
	 */
	char *last2 = rindex(dup, '/');
	if (last2 != NULL) last2++;

	/* special case */
	if ((strcasecmp(last, "AND") == 0) || (strcasecmp(last, "OR") == 0)) {

#if VERBOSE_DEBUG
	 	dbg(LOG_INFO, "GETATTR on AND/OR logical operator!");
#endif

	 	lstat(tagsistant.archive, stbuf);	

		/* 
		 * using tagsistant.archive in lstat() returns its inode number
		 * for all AND/OR ops. that is a problem while traversing
		 * a dir tree with write approach, like in a rm -rf <dir>.
		 * to avoid such a problem we add 1 to inode number to
		 * differentiate it
		 */
		stbuf->st_ino++;

		/* AND and OR can't be the same inode */
		if (strcasecmp(last, "OR") == 0) stbuf->st_ino++;
		
		/* logical operators can't be written by anyone */
		stbuf->st_mode = S_IFDIR|S_IRUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH;

		/* not sure of that! */
		/* stbuf->st_size = 0; */

		stbuf->st_nlink = 1;

		free(dup);
		return 0;
	}

	/* last token in path is a file or a tag? */
	if ((last2 == NULL) || (strcmp(last2, "AND") == 0) || (strcmp(last2, "OR") == 0)) {
		/* is a dir-tag */
		/* last is the name of the tag */
#if VERBOSE_DEBUG
		dbg(LOG_INFO, "GETATTR on tag: '%s'", last);
#endif
		char *statement = calloc(sizeof(char), strlen(TAG_EXISTS) + strlen(last));
		if (statement == NULL) {
			dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
			return 0;
		}
		sprintf(statement, TAG_EXISTS, last);
		assert(strlen(TAG_EXISTS) + strlen(last) > strlen(statement));

		int exists = 0;
		do_sql(NULL, statement, report_if_exists, &exists);
		free(statement);

		if (exists) {
			res = lstat(tagsistant.archive, stbuf);
			tagsistant_errno = errno;
		} else {
			res = -1;
			tagsistant_errno = ENOENT;
		}
	} else {
		/* is a file */
		/* last is the filename */
		/* last2 is the last tag in path */

		/* check if file is tagged */

		/* build querytree */
		ptree_or_node_t *pt = build_querytree(path);
		if (pt == NULL) {
			dbg(LOG_ERR, "Error building querytree @%s:%d", __FILE__, __LINE__);
			return -ENOENT;
		}
		ptree_or_node_t *ptx = pt;

		/* check in all tags */
		while (ptx != NULL) {
			ptree_and_node_t *andpt = ptx->and_set;
			while (andpt != NULL) {
				if (is_tagged(last, andpt->tag)) {
					char *filepath = get_file_path(last);
					res = lstat(filepath, stbuf);
					tagsistant_errno = (res == -1) ? errno : 0;
#if VERBOSE_DEBUG
					dbg(LOG_INFO, "lstat('%s/%s'): %d (%s)", andpt->tag, last, res, strerror(tagsistant_errno));
#endif
					free(filepath);
					destroy_querytree(pt);
					goto GETATTR_EXIT;
				}
#if VERBOSE_DEBUG
				dbg(LOG_INFO, "%s is not tagged %s", last, andpt->tag);
#endif
				andpt = andpt->next;
			}
			ptx = ptx->next;
		}
		destroy_querytree(pt);
		res = -1;
		tagsistant_errno = ENOENT;
	}

GETATTR_EXIT:

	free(dup);
	/* last and last2 are not malloc()ated! free(last) and free(last2) are errors! */

	stop_labeled_time_profile("getattr");
	if ( res == -1 ) {
		dbg(LOG_ERR, "GETATTR on %s: %d %d: %s", path, res, tagsistant_errno, strerror(tagsistant_errno));
		return -tagsistant_errno;
	} else {
		dbg(LOG_INFO, "GETATTR on %s: OK", path);
		return 0;
	}
}

static int tagsistant_readlink(const char *path, char *buf, size_t size)
{
	char *filename = get_tag_name(path);
	char *filepath = get_file_path(filename);
	free(filename);

	dbg(LOG_INFO, "READLINK on %s", filepath);

	int res = readlink(filepath, buf, size);
	int tagsistant_errno = errno;

	free(filepath);
	return (res == -1) ? -tagsistant_errno : 0;
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

#if VERBOSE_DEBUG
	dbg(LOG_INFO, "add_entry_to_dir: + %s", argv[0]);
#endif

	/* add also to cache */
	if (ufs->add_to_cache) {
		char *mini = calloc(sizeof(char), strlen(ADD_RESULT_ENTRY) + strlen(argv[0]) + 14);
		if (mini == NULL) {
			dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		} else {
			sprintf(mini, ADD_RESULT_ENTRY, (int64_t) ufs->path_id, argv[0]);
			assert(strlen(ADD_RESULT_ENTRY) + strlen(argv[0]) + 14 > strlen(mini));
			do_sql(NULL, mini, add_entry_to_dir, ufs);
			free(mini);
		}
	}

	return ufs->filler(ufs->buf, argv[0], NULL, 0);
}

static int tagsistant_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
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
	
			do_sql(NULL, mini, add_entry_to_dir, ufs);
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
		do_sql(NULL, GET_ALL_TAGS, add_entry_to_dir, ufs);
		free(ufs);
	}
	free(tagname);

	return 0;
}

/* OK */
static int tagsistant_mknod(const char *path, mode_t mode, dev_t rdev)
{
	int res = 0, tagsistant_errno = 0;

	init_time_profile();
	start_time_profile();

	char *filename, *fullfilename, *tagname;
	get_filename_and_tagname(path, &filename, &fullfilename, &tagname);

	if (tagname == NULL) {
		/* can't create files outside tag/directories */
		res = -1;
		tagsistant_errno = ENOTDIR;
	} else if (is_tagged(filename, tagname)) {
		/* is already tagged so no mknod() is required */
		dbg(LOG_INFO, "%s is already tagged as %s", filename, tagname);
		res = -1;
		tagsistant_errno = EEXIST;
	} else {
		/* file is not already tagged, doing it */
		if (tag_file(filename, tagname)) {
			dbg(LOG_INFO, "Tagging file %s as %s inside tagsistant_mknod()", filename, tagname);
			res = mknod(fullfilename, mode, rdev);
			tagsistant_errno = errno;
			if (res == -1) {
				/*
				 * if real mknod() return -1 could be because file already exists
				 * in /archive/ directory. if that's the case, just fake correct
				 * operations.
				 */
				if (tagsistant_errno == EEXIST) {
					res = 0;
					tagsistant_errno = 0;
				} else {
					dbg(LOG_ERR, "Real mknod() failed: %s", strerror(tagsistant_errno));
					untag_file(filename, tagname);
				}
			}
		} else {
			/* tagging failed, we should fake a mknod() error */
			res = -1;
			tagsistant_errno = ENOSPC;
		}
	}

	dbg(LOG_INFO, "MKNOD on %s", path);

	free(filename);
	free(fullfilename);
	free(tagname);

	stop_labeled_time_profile("mknod");
	dbg(LOG_INFO, "mknod(%s): %d %s", filename, res, strerror(tagsistant_errno));
	return (res == -1) ? -tagsistant_errno: 0;
}

/* OK */
static int tagsistant_mkdir(const char *path, mode_t mode)
{
	(void) mode;
    int res = 0, tagsistant_errno = 0;
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
	do_sql(NULL, statement, NULL, NULL);

	free(statement);
	free(tagname);

	stop_labeled_time_profile("mkdir");
	return (res == -1) ? -tagsistant_errno : 0;
}

/* OK */
static int tagsistant_unlink(const char *path)
{
    int res = 0, tagsistant_errno = 0;

	init_time_profile();
	start_time_profile();

	char *filename = get_tag_name(path);

	dbg(LOG_INFO, "UNLINK on %s", filename);

	ptree_or_node_t *pt = build_querytree(path);
	ptree_or_node_t *ptx = pt;

	unsigned int size1 = strlen(UNTAG_FILE) + strlen(filename) + MAX_TAG_LENGTH;
	char *statement1 = calloc(sizeof(char), size1);
	if (statement1 == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return 1;
	}
	unsigned int size2 = strlen(GET_ID_OF_TAG) + MAX_TAG_LENGTH * 2;
	char *statement2 = calloc(sizeof(char), size2);
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
			memset(statement1, '\0', size1);
			sprintf(statement1, UNTAG_FILE, element->tag, filename);
			assert(size1 > strlen(statement1));
			do_sql(NULL, statement1, NULL, NULL);

			memset(statement2, '\0', size2);
			sprintf(statement2, GET_ID_OF_TAG, element->tag, element->tag);
			assert(size2 > strlen(statement2));
			do_sql(NULL, statement2, drop_single_file, filename);

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

		int exists = 0;
		do_sql(NULL, statement1, report_if_exists, &exists);
		free(statement1);

		if (!exists) {
			/* file is no longer tagged, so can be deleted from archive */
			char *filepath = get_file_path(filename);
			dbg(LOG_INFO, "Unlinking file %s: it's no longer tagged!", filepath);
			res = unlink(filepath);
			tagsistant_errno = errno;
			free(filepath);
		}
	}

	free(filename);

	stop_labeled_time_profile("unlink");
	return (res == -1) ? -tagsistant_errno : 0;
}

/* OK */
static int tagsistant_rmdir(const char *path)
{
    int res = 0, tagsistant_errno = 0;
	init_time_profile();
	start_time_profile();

	ptree_or_node_t *pt = build_querytree(path);
	ptree_or_node_t *ptx = pt;

	/* tag name is inserted 2 times in query, that's why '* 2' */
	char *statement = calloc(sizeof(char), strlen(DELETE_TAG) + MAX_TAG_LENGTH * 4);
	if (statement == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return 1;
	}

	while (ptx != NULL) {
		ptree_and_node_t *element = ptx->and_set;
		while (element != NULL) {
			sprintf(statement, DELETE_TAG, element->tag, element->tag, element->tag, element->tag);
			assert(strlen(DELETE_TAG) + MAX_TAG_LENGTH * 4 > strlen(statement));
			dbg(LOG_INFO, "RMDIR on %s", element->tag);
			do_sql(NULL, statement, NULL, NULL);
			element = element->next;
			memset(statement, '\0', strlen(statement));
		}
		ptx = ptx->next;
	}

	free(statement);
	destroy_querytree(pt);

	stop_labeled_time_profile("rmdir");
	return (res == -1) ? -tagsistant_errno : 0;
}

/* OK */
static int tagsistant_rename(const char *from, const char *to)
{
    int res = 0, tagsistant_errno = 0;

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
		do_sql(NULL, statement, NULL, NULL);
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
		do_sql(NULL, statement, NULL, NULL);
		free(statement);

		char *filepath = get_file_path(tagname);
		char *newfilepath = get_file_path(newfilename);
		res = rename(filepath, newfilepath);
		tagsistant_errno = errno;
		free(filepath);
		free(newfilepath);
	}

	stop_labeled_time_profile("rename");
	return (res == -1) ? -tagsistant_errno : 0;
}

/* OK */
static int tagsistant_link(const char *from, const char *to)
{
    int res = 0, tagsistant_errno = 0;

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
	dbg(LOG_INFO, "Tagging file inside tagsistant_link() or tagsistant_symlink()");
	tag_file(filename, tagname);

	struct stat stbuf;
	if ((lstat(topath, &stbuf) == -1) && (errno == ENOENT)) {
		dbg(LOG_INFO, "Linking %s as %s", from, topath);
		res = symlink(from, topath);
		tagsistant_errno = errno;
	} else {
		dbg(LOG_INFO, "A file named %s is already in archive.", filename);
		res = -1;
		tagsistant_errno = EEXIST;
	}

	free(tagname);
	free(filename);
	free(topath);

	stop_labeled_time_profile("link");
	return (res == -1) ? -tagsistant_errno : 0;
}

/* OK */
static int tagsistant_symlink(const char *from, const char *to)
{
	return tagsistant_link(from, to);
}

/* OK */
static int tagsistant_chmod(const char *path, mode_t mode)
{
    int res = 0, tagsistant_errno = 0;

	init_time_profile();
	start_time_profile();

	char *tagname = get_tag_name(path);
	char *filepath = get_file_path(tagname);

	res = chmod(filepath, mode);
	tagsistant_errno = errno;

	free(tagname);
	free(filepath);

	stop_labeled_time_profile("chmod");
	return (res == -1) ? -tagsistant_errno : 0;
}

/* OK */
static int tagsistant_chown(const char *path, uid_t uid, gid_t gid)
{
    int res = 0, tagsistant_errno = 0;

	init_time_profile();
	start_time_profile();

	char *tagname = get_tag_name(path);
	char *filepath = get_file_path(tagname);

	res = chown(filepath, uid, gid);
	tagsistant_errno = errno;

	free(tagname);
	free(filepath);

	stop_labeled_time_profile("chown");
	return (res == -1) ? -tagsistant_errno : 0;
}

/* OK */
static int tagsistant_truncate(const char *path, off_t size)
{
    int res = 0, tagsistant_errno = 0;

	init_time_profile();
	start_time_profile();

	char *tagname = get_tag_name(path);
	char *filepath = get_file_path(tagname);

	res = truncate(filepath, size);
	tagsistant_errno = errno;

	free(tagname);
	free(filepath);

	stop_labeled_time_profile("truncate");
	return (res == -1) ? -tagsistant_errno : 0;
}

/* OK */
static int tagsistant_utime(const char *path, struct utimbuf *buf)
{
    int res = 0, tagsistant_errno = 0;
	(void) path;

	init_time_profile();
	start_time_profile();

	char *tagname = get_tag_name(path);
	char *filepath = get_file_path(tagname);

	res = utime(filepath, buf);
	tagsistant_errno = errno;

	free(tagname);
	free(filepath);

	stop_labeled_time_profile("utime");
	return (res == -1) ? -errno : 0;
}

/* OK */
int internal_open(const char *filepath, int flags, int *_errno)
{
	int res = open(filepath, flags);
#if VERBOSE_DEBUG
	dbg(LOG_INFO, "internal_open(%s): %d", filepath, res);
	if (flags&O_CREAT) dbg(LOG_INFO, "...O_CREAT");
	if (flags&O_WRONLY) dbg(LOG_INFO, "...O_WRONLY");
	if (flags&O_TRUNC) dbg(LOG_INFO, "...O_TRUNC");
	if (flags&O_LARGEFILE) dbg(LOG_INFO, "...O_LARGEFILE");
#endif

	*_errno = errno;

	return res;
}

/* OK */
static int tagsistant_open(const char *path, struct fuse_file_info *fi)
{
    int res = -1, tagsistant_errno = ENOENT;
	char *filename, *filepath, *tagname;
	get_filename_and_tagname(path, &filename, &filepath, &tagname);

	init_time_profile();
	start_time_profile();

	dbg(LOG_INFO, "OPEN: %s", path);

	ptree_or_node_t *pt = build_querytree(path);
	ptree_or_node_t *ptx = pt;
	while (ptx != NULL && res == -1) {
		ptree_and_node_t *and = ptx->and_set;
		while (and != NULL && res == -1) {
			if (is_tagged(filename, and->tag)) {
				res = internal_open(filepath, fi->flags|O_RDONLY, &tagsistant_errno);
				if (res != -1) close(res);
			}
			and = and->next;
		}
		ptx = ptx->next;
	}

	free(filename);
	free(filepath);
	free(tagname);

	stop_labeled_time_profile("open");
	return (res == -1) ? -tagsistant_errno : 0;
}

/* OK */
static int tagsistant_read(const char *path, char *buf, size_t size, off_t offset,
                    struct fuse_file_info *fi)
{
    int res = 0, tagsistant_errno = 0;
	char *filename, *filepath, *tagname;
	get_filename_and_tagname(path, &filename, &filepath, &tagname);

	init_time_profile();
	start_time_profile();

	int fd = internal_open(filepath, fi->flags|O_RDONLY, &tagsistant_errno); 
	if (fd != -1) {
		res = pread(fd, buf, size, offset);
		tagsistant_errno = errno;
		close(fd);
	}

	free(filename);
	free(filepath);
	free(tagname);

	stop_labeled_time_profile("read");
	return (res == -1) ? -tagsistant_errno : res;
}

/* OK */
static int tagsistant_write(const char *path, const char *buf, size_t size,
	off_t offset, struct fuse_file_info *fi)
{
    int res = 0, tagsistant_errno = 0;
	char *filename, *filepath, *tagname;
	get_filename_and_tagname(path, &filename, &filepath, &tagname);

	init_time_profile();
	start_time_profile();

	/* return if path is complex, i.e. including logical operators */
	if ((strstr(path, "/AND") != NULL) || (strstr(path, "/OR") != NULL)) {
		dbg(LOG_ERR, "Logical operators not allowed in open path");
		return -ENOTDIR;
	}

	int fd = internal_open(filepath, fi->flags|O_WRONLY, &tagsistant_errno); 
	if (fd != -1) {
		dbg(LOG_INFO, "writing %d bytes to %s", size, path);
		res = pwrite(fd, buf, size, offset);
		tagsistant_errno = errno;

		if (res == -1)
			dbg(LOG_INFO, "Error on fd.%d: %s", fd, strerror(tagsistant_errno));

		close(fd);

	}

	if (res == -1)
		dbg(LOG_INFO, "WRITE: returning %d: %s", tagsistant_errno, strerror(tagsistant_errno));

	free(filename);
	free(filepath);
	free(tagname);

	stop_labeled_time_profile("write");
	return (res == -1) ? -tagsistant_errno : res;
}

#if FUSE_USE_VERSION == 25

/* OK */
static int tagsistant_statvfs(const char *path, struct statvfs *stbuf)
{
    int res = 0, tagsistant_errno = 0;
	(void) path;

	init_time_profile();
	start_time_profile();
	
	res = statvfs(tagsistant.repository, stbuf);
	tagsistant_errno = errno;

	stop_labeled_time_profile("statfs");
    return (res == -1) ? -tagsistant_errno : 0;
}

#else

/* OK */
static int tagsistant_statfs(const char *path, struct statfs *stbuf)
{
    int res = 0, tagsistant_errno = 0;
	(void) path;

	init_time_profile();
	start_time_profile();
	
	res = statfs(tagsistant.repository, stbuf);
	tagsistant_errno = errno;

	stop_labeled_time_profile("statfs");
    return (res == -1) ? -tagsistant_errno : 0;
}

#endif

static int tagsistant_release(const char *path, struct fuse_file_info *fi)
{
    /* Just a stub.  This method is optional and can safely be left
       unimplemented */

    (void) path;
    (void) fi;

	return 0; /* REMOVE ME AFTER CODING */
}

static int tagsistant_fsync(const char *path, int isdatasync,
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
static int tagsistant_setxattr(const char *path, const char *name, const char *value,
                        size_t size, int flags)
{
    int res;

	return 0; /* REMOVE ME AFTER CODING */
}

static int tagsistant_getxattr(const char *path, const char *name, char *value,
                    size_t size)
{
    int res;
	return 0; /* REMOVE ME AFTER CODING */
}

static int tagsistant_listxattr(const char *path, char *list, size_t size)
{
    int res;

	return 0; /* REMOVE ME AFTER CODING */
}

static int tagsistant_removexattr(const char *path, const char *name)
{
    int res;

	return 0; /* REMOVE ME AFTER CODING */
}
#endif /* HAVE_SETXATTR */

static void *tagsistant_init(void)
{
	return 0;
}

static struct fuse_operations tagsistant_oper = {
    .getattr	= tagsistant_getattr,
    .readlink	= tagsistant_readlink,
    .readdir	= tagsistant_readdir,
    .mknod		= tagsistant_mknod,
    .mkdir		= tagsistant_mkdir,
    .symlink	= tagsistant_symlink,
    .unlink		= tagsistant_unlink,
    .rmdir		= tagsistant_rmdir,
    .rename		= tagsistant_rename,
    .link		= tagsistant_link,
    .chmod		= tagsistant_chmod,
    .chown		= tagsistant_chown,
    .truncate	= tagsistant_truncate,
    .utime		= tagsistant_utime,
    .open		= tagsistant_open,
    .read		= tagsistant_read,
    .write		= tagsistant_write,
#if FUSE_USE_VERSION == 25
    .statfs		= tagsistant_statvfs,
#else
    .statfs		= tagsistant_statfs,
#endif
    .release	= tagsistant_release,
    .fsync		= tagsistant_fsync,
#ifdef HAVE_SETXATTR
    .setxattr	= tagsistant_setxattr,
    .getxattr	= tagsistant_getxattr,
    .listxattr	= tagsistant_listxattr,
    .removexattr= tagsistant_removexattr,
#endif
	.init		= tagsistant_init,
};

enum {
    KEY_HELP,
    KEY_VERSION,
};

/* following code got from SSHfs sources */
#define TAGSISTANT_OPT(t, p, v) { t, offsetof(struct tagsistant, p), v }

static struct fuse_opt tagsistant_opts[] = {
	TAGSISTANT_OPT("-d",					debug,			1),
	TAGSISTANT_OPT("--repository=%s",		repository,		0),
	TAGSISTANT_OPT("-f",					foreground,		1),
	TAGSISTANT_OPT("-s",					singlethread,	1),
	TAGSISTANT_OPT("-r",					readonly,		1),
	
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
		" Tagsistant (tagfs) v.%s\n"
		" Semantic File System for Linux kernels\n"
		" (c) 2006-2007 Tx0 <tx0@strumentiresistenti.org>\n"
		" FUSE_USE_VERSION: %d\n\n"
		" This program is free software; you can redistribute it and/or modify\n"
		" it under the terms of the GNU General Public License as published by\n"
		" the Free Software Foundation; either version 2 of the License, or\n"
		" (at your option) any later version.\n\n"

		" This program is distributed in the hope that it will be useful,\n"
		" but WITHOUT ANY WARRANTY; without even the implied warranty of\n"
		" MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the\n"
		" GNU General Public License for more details.\n\n"

		" You should have received a copy of the GNU General Public License\n"
		" along with this program; if not, write to the Free Software\n"
		" Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA\n"
		" \n"
		" Usage: %s [OPTIONS] [--repository=<PATH>] /mountpoint\n"
		"\n"
		"  -u  unmount a mounted filesystem\n"
		"  -q  be quiet\n"
		"  -r  mount readonly\n"
		"  -z  lazy unmount (can be dangerous!)\n"
		"\n" /*fuse options will follow... */
		, PACKAGE_VERSION, FUSE_USE_VERSION, progname
	);
}

static int tagsistant_opt_proc(void *data, const char *arg, int key, struct fuse_args *outargs)
{
    (void) data;
	(void) arg;

    switch (key) {
		case FUSE_OPT_KEY_NONOPT:
			if (!tagsistant.mountpoint) {
				tagsistant.mountpoint = strdup(arg);
				return 1;
			}
			return 0;

	    case KEY_HELP:
	        usage(outargs->argv[0]);
	        fuse_opt_add_arg(outargs, "-ho");
	        fuse_main(outargs->argc, outargs->argv, &tagsistant_oper);
	        exit(1);
	
	    case KEY_VERSION:
	        fprintf(stderr, "Tagfs for Linux 0.1 (prerelease %s)\n", VERSION);
#if FUSE_VERSION >= 25
	        fuse_opt_add_arg(outargs, "--version");
	        fuse_main(outargs->argc, outargs->argv, &tagsistant_oper);
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
	sqlite3_close(tagsistant.dbh);
	exit(s);
}

/**
 * the head of plugin list
 */
tagsistant_plugin_t *plugins = NULL;

/**
 * guess the MIME type of passed filename
 *
 * \param filename file to be processed (just the name, will be looked up in /archive)
 * \return the string rappresenting MIME type (like "audio/mpeg"); the string is dynamically
 *   allocated and need to be free()ed by outside code
 * \todo still to be coded
 */
char *get_file_mimetype(const char *filename)
{
	(void) filename;
	return strdup("audio/mpeg");
}

/**
 * process a file using plugin chain
 *
 * \param filename file to be processed (just the name, will be looked up in /archive)
 * \return zero on fault, one on success
 */
int process(const char *filename)
{
	int res = 0, process_res = 0;

	char *mime_type = get_file_mimetype(filename);

	/* apply plugins in order */
	tagsistant_plugin_t *plugin = plugins;
	while (plugin != NULL) {
		if ((strcmp(plugin->mime_type, mime_type) == 0) || (strcmp(plugin->mime_type, "*/*") == 0)) {
			process_res = (plugin->processor)(filename);

			/* report about processing */
			if (process_res == 0) {
				dbg(LOG_ERR, "Plugin %s was supposed to apply to %s, but failed!", plugin->filename, filename);
			} else if (process_res == 2) {
				dbg(LOG_INFO, "Plugin %s terminated plugin chain on file %s", plugin->filename, filename);
			}
		}
		plugin = plugin->next;
	}

	free(mime_type);
	return res;
}

int main(int argc, char *argv[])
{
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	int res;

	tagsistant.progname = argv[0];

	if (fuse_opt_parse(&args, &tagsistant, tagsistant_opts, tagsistant_opt_proc) == -1)
		exit(1);

	/*
	fuse_opt_add_arg(&args, "-odefault_permissions,allow_other,fsname=tagsistant");
	*/
	fuse_opt_add_arg(&args, "-odefault_permissions,fsname=tagsistant");
	fuse_opt_add_arg(&args, "-ouse_ino,readdir_ino");

	/*
	 * NOTE
	 *
	 * SQLite library is often called "out of sequence" like in
	 * performing a query before having connected to sql database.
	 * to temporary solve this problem we force here single threaded
	 * operations. should be really solved by better read_do_sql()
	 */
	tagsistant.singlethread = 1;

	if (tagsistant.singlethread) {
		fprintf(stderr, " *** operating in single thread mode ***\n");
		fuse_opt_add_arg(&args, "-s");
	}
	if (tagsistant.readonly) {
		fprintf(stderr, " *** mounting tagsistant read-only ***\n");
		fuse_opt_add_arg(&args, "-r");
	}
	if (tagsistant.foreground) {
		fprintf(stderr, " *** will run in foreground ***\n");
		fuse_opt_add_arg(&args, "-f");
	}

	/* checking mountpoint */
	if (!tagsistant.mountpoint) {
		usage(tagsistant.progname);
		fprintf(stderr, " *** No mountpoint provided *** \n\n");
		exit(2);
	}

	fprintf(stderr, "\n");
	fprintf(stderr,
		" Tag based filesystem for Linux kernels\n"
		" (c) 2006-2007 Tx0 <tx0@strumentiresistenti.org>\n"
		" For license informations, see %s -h\n"
		" FUSE_USE_VERSION: %d\n\n"
		, tagsistant.progname, FUSE_USE_VERSION
	);
	
	/* checking repository */
	if (!tagsistant.repository || (strcmp(tagsistant.repository, "") == 0)) {
		if (strlen(getenv("HOME"))) {
			int replength = strlen(getenv("HOME")) + strlen("/.tagsistant") + 1;
			free(tagsistant.repository);
			tagsistant.repository = calloc(replength, sizeof(char));
			strcat(tagsistant.repository, getenv("HOME"));
			strcat(tagsistant.repository, "/.tagsistant");
			fprintf(stderr, " Using default repository %s\n", tagsistant.repository);
		} else {
			usage(tagsistant.progname);
			fprintf(stderr, " *** No repository provided with -r ***\n\n");
			exit(2);
		}
	}

	/* removing last slash */
	int replength = strlen(tagsistant.repository) - 1;
	if (tagsistant.repository[replength] == '/') {
		tagsistant.repository[replength] = '\0';
	}

	/* checking if repository path begings with ~ */
	if (tagsistant.repository[0] == '~') {
		char *home_path = getenv("HOME");
		if (home_path != NULL) {
			char *relative_path = strdup(tagsistant.repository + 1);
			free(tagsistant.repository);
			tagsistant.repository = calloc(sizeof(char), strlen(relative_path) + strlen(home_path) + 1);
			strcpy(tagsistant.repository, home_path);
			strcat(tagsistant.repository, relative_path);
			free(relative_path);
			dbg(LOG_INFO, "Repository path is %s", tagsistant.repository);
		} else {
			dbg(LOG_ERR, "Repository path starts with '~', but $HOME was not available!");
		}
	} else 

	/* checking if repository is a relative path */
	if (tagsistant.repository[0] != '/') {
		dbg(LOG_ERR, "Repository path is relative [%s]", tagsistant.repository);
		char *cwd = getcwd(NULL, 0);
		if (cwd == NULL) {
			dbg(LOG_ERR, "Error getting working directory, will leave repository path as is");
		} else {
			char *absolute_repository = calloc(sizeof(char), strlen(tagsistant.repository) + strlen(cwd) + 2);
			if (absolute_repository == NULL) {
				dbg(LOG_ERR, "Error allocaing memory @%s:%d", __FILE__, __LINE__);
				dbg(LOG_ERR, "Repository path will be left as is");
			} else {
				strcpy(absolute_repository, cwd);
				strcat(absolute_repository, "/");
				strcat(absolute_repository, tagsistant.repository);
				free(tagsistant.repository);
				tagsistant.repository = absolute_repository;
				dbg(LOG_ERR, "Repository path is %s", tagsistant.repository);
			}
		}
	}

	struct stat repstat;
	if (lstat(tagsistant.repository, &repstat) == -1) {
		if(mkdir(tagsistant.repository, 755) == -1) {
			fprintf(stderr, " *** REPOSITORY: Can't mkdir(%s): %s ***\n\n", tagsistant.repository, strerror(errno));
			exit(2);
		}
	}
	chmod(tagsistant.repository, S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH);

	/* opening (or creating) SQL tags database */
	tagsistant.tags = malloc(strlen(tagsistant.repository) + strlen("/tags.sql") + 1);
	strcpy(tagsistant.tags,tagsistant.repository);
	strcat(tagsistant.tags,"/tags.sql");

	/* check if db exists or has to be created */
	struct stat dbstat;
	int db_exists = lstat(tagsistant.tags, &dbstat);

	/* open connection to sqlite database */
	int result = sqlite3_open(tagsistant.tags, &(tagsistant.dbh));

	/* check if open has been fullfilled */
	if (result != SQLITE_OK) {
		dbg(LOG_ERR, "Error [%d] opening database %s", result, tagsistant.tags);
		dbg(LOG_ERR, "%s", sqlite3_errmsg(tagsistant.dbh));
		exit(1);
	}

	/* if db does not existed, some SQL init is needed */
	if (db_exists != 0) {
		char *sqlerror;
		int sqlcode;
		
		sqlcode = sqlite3_exec(tagsistant.dbh, CREATE_TAGS_TABLE, NULL, NULL, &sqlerror);
		if (sqlcode != SQLITE_OK) {
			dbg(LOG_ERR, "SQL error [%d] while creating tags table: %s", sqlcode, sqlerror);
			exit(1);
		}
		sqlcode = sqlite3_exec(tagsistant.dbh, CREATE_TAGGED_TABLE, NULL, NULL, &sqlerror);
		if (sqlcode != SQLITE_OK) {
			dbg(LOG_ERR, "SQL error [%d] while creating tagged table: %s", sqlcode, sqlerror);
			exit(1);
		}
		sqlcode = sqlite3_exec(tagsistant.dbh, CREATE_CACHE_TABLE, NULL, NULL, &sqlerror);
		if (sqlcode != SQLITE_OK) {
			dbg(LOG_ERR, "SQL error [%d] while creating cache main table: %s", sqlcode, sqlerror);
			exit(1);
		}
		sqlcode = sqlite3_exec(tagsistant.dbh, CREATE_RESULT_TABLE, NULL, NULL, &sqlerror);
		if (sqlcode != SQLITE_OK) {
			dbg(LOG_ERR, "SQL error [%d] while creating cache results table: %s", sqlcode, sqlerror);
			exit(1);
		}
	}

	/* checking file archive directory */
	tagsistant.archive = malloc(strlen(tagsistant.repository) + strlen("/archive/") + 1);
	strcpy(tagsistant.archive,tagsistant.repository);
	strcat(tagsistant.archive,"/archive/");

	if (lstat(tagsistant.archive, &repstat) == -1) {
		if(mkdir(tagsistant.archive, 755) == -1) {
			fprintf(stderr, " *** ARCHIVE: Can't mkdir(%s): %s ***\n\n", tagsistant.archive, strerror(errno));
			exit(2);
		}
	}
	chmod(tagsistant.archive, S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH);

	if (tagsistant.debug) debug = tagsistant.debug;

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

	/*
	 * loading plugins
	 */
	char *tagsistant_plugins = NULL;
	if (getenv("TAGSISTANT_PLUGINS") != NULL) {
		tagsistant_plugins = strdup(getenv("TAGSISTANT_PLUGINS"));
		fprintf(stderr, " Using user defined plugin dir: %s\n", tagsistant_plugins);
	} else {
		tagsistant_plugins = strdup(PLUGINS_DIR);
		fprintf(stderr, " Using default plugin dir: %s\n", tagsistant_plugins);
	}

	struct stat st;
	if (lstat(tagsistant_plugins, &st) == -1) {
		fprintf(stderr, " *** error opening directory %s: %s ***\n", tagsistant_plugins, strerror(errno));
	} else if (!S_ISDIR(st.st_mode)) {
		fprintf(stderr, " *** error opening directory %s: not a directory ***\n", tagsistant_plugins);
	} else {
#if 0
		/* add this directory to LD_LIBRARY_PATH */
		int ld_library_path_length = strlen(getenv("LD_LIBRARY_PATH")) + 1 + strlen(tagsistant_plugins);
		char *NEW_LD_LIBRARY_PATH = calloc(ld_library_path_length, sizeof(char));
		strcat(NEW_LD_LIBRARY_PATH, getenv("LD_LIBRARY_PATH"));
		strcat(NEW_LD_LIBRARY_PATH, ":");
		strcat(NEW_LD_LIBRARY_PATH, tagsistant_plugins);
		setenv("LD_LIBRARY_PATH", NEW_LD_LIBRARY_PATH, 1);
		free(NEW_LD_LIBRARY_PATH);
		fprintf(stderr, " LD_LIBRARY_PATH = %s\n", getenv("LD_LIBRARY_PATH"));
#endif

		/* open directory and read contents */
		DIR *p = opendir(tagsistant_plugins);
		if (p == NULL) {
			fprintf(stderr, " *** error opening plugin directory %s ***\n", tagsistant_plugins);
		} else {
			struct dirent *de;
			while ((de = readdir(p)) != NULL) {
				/* checking if file begins with tagsistant plugin prefix */
				char *needle = strstr(de->d_name, TAGSISTANT_PLUGIN_PREFIX);
				if ((needle == NULL) || (needle != de->d_name)) continue;

				needle = strstr(de->d_name, ".so");
				if ((needle == NULL) || (needle != de->d_name + strlen(de->d_name) - 3)) continue;

				/* file is a tagsistant plugin (beginning by right prefix) and is processed */
				/* allocate new plugin object */
				tagsistant_plugin_t *plugin = NULL;
				while ((plugin = calloc(1, sizeof(tagsistant_plugin_t))) == NULL);

				char *pname = calloc(strlen(de->d_name) + strlen(tagsistant_plugins) + 2, sizeof(char));
				strcat(pname, tagsistant_plugins);
				strcat(pname, "/");
				strcat(pname, de->d_name);

				/* load the plugin */
				plugin->handle = dlopen(pname, RTLD_NOW);
				if (plugin->handle == NULL) {
					fprintf(stderr, " *** error dlopen()ing plugin %s: %s ***\n", de->d_name, dlerror());
					free(plugin);
				} else {
					/* search for init function and call it */
					int (*init_function)() = NULL;
					init_function = dlsym(plugin->handle, "plugin_init");
					if (init_function != NULL) {
						int init_res = init_function();
						if (!init_res) {
							/* if init failed, ignore this plugin */
							dbg(LOG_ERR, " *** error calling plugin_init() on %s ***\n", de->d_name);
							free(plugin);
							continue;
						}
					}

					/* search for MIME type string */
					plugin->mime_type = dlsym(plugin->handle, "mime_type");
					if (plugin->mime_type == NULL) {
						fprintf(stderr, " *** error finding %s processor function: %s ***\n", de->d_name, dlerror());
						free(plugin);
					} else {
						/* search for processor function */
						plugin->processor = dlsym(plugin->handle, "processor");	
						if (plugin->processor == NULL) {
							fprintf(stderr, " *** error finding %s processor function: %s ***\n", de->d_name, dlerror());
							free(plugin);
						} else {
							/* add this plugin on queue head */
							plugin->next = plugins;
							plugins = plugin;
							fprintf(stderr, " Loaded plugin: [%s]\n", de->d_name);
						}
					}
				}
				free(pname);
			}
			closedir(p);
		}
	}

	dbg(LOG_INFO, "Mounting filesystem");

	dbg(LOG_INFO, "Fuse options:");
	int fargc = args.argc;
	while (fargc) {
		dbg(LOG_INFO, "%.2d: %s", fargc, args.argv[fargc]);
		fargc--;
	}

	res = fuse_main(args.argc, args.argv, &tagsistant_oper);

    fuse_opt_free_args(&args);

	return res;
}

int real_do_sql(sqlite3 **dbh, char *statement, int (*callback)(void *, int, char **, char **), void *firstarg, char *file, unsigned int line)
{
	int result = SQLITE_OK;
	sqlite3 **intdbh = malloc(sizeof(sqlite3 *));

	if (statement == NULL) {
		dbg(LOG_ERR, "Null SQL statement");
		return 0;
	}

	/*
	 * check if:
	 * 1. no database handle location has been passed (means: use local dbh)
	 * 2. database handle location is empty (means: create new dbh and return it)
	 */
	if ((dbh == NULL) || (*dbh == NULL)) {
		result = sqlite3_open(tagsistant.tags, intdbh);
		if (result != SQLITE_OK) {
			dbg(LOG_ERR, "Error [%d] opening database %s", result, tagsistant.tags);
			dbg(LOG_ERR, "%s", sqlite3_errmsg(*intdbh));
			return 0;
		}
	} else {
		*intdbh = *dbh;
	}

	char *sqlerror;
	dbg(LOG_INFO, "SQL: \"%s\"", statement);
	result = sqlite3_exec(tagsistant.dbh, statement, callback, firstarg, &sqlerror);
	if (result != SQLITE_OK) {
		dbg(LOG_ERR, "SQL error: [%d] %s @%s:%u", result, sqlerror, file, line);
		sqlite3_free(sqlerror);
		return 0;
	}
	free(file);

	if (dbh == NULL) {
		sqlite3_close(*intdbh);
		free(intdbh);
	} else if (*dbh == NULL) {
		*dbh = *intdbh;
	}

	return result;
}

// vim:ts=4:autoindent:nocindent:syntax=c
