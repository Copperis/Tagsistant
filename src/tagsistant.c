/*
   Tagsistant (tagfs) -- tagsistant.c
   Copyright (C) 2006-2009 Tx0 <tx0@strumentiresistenti.org>

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

#ifndef MACOSX
#include <mcheck.h>
#endif

/* defines command line options for tagsistant mount tool */
/* static */ struct tagsistant tagsistant;

/**
 * lstat equivalent
 *
 * \param path the path to be lstat()ed
 * \param stbuf pointer to struct stat buffer holding data about file
 * \return 0 on success, -errno otherwise
 */
static int tagsistant_getattr(const char *path, struct stat *stbuf)
{
    int res = 0, tagsistant_errno = 0;
	gchar *lstat_path = NULL;

	init_time_profile();
	start_time_profile();

	// build querytree
	querytree_t *qtree = build_querytree(path, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
		goto GETATTR_EXIT;
	} else
	
	// -- object on disk --
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		lstat_path = qtree->full_archive_path;
	} else

	// -- tags --
	// -- archive --
	// -- root --
	// -- stats --
	// -- relations --
	{
		lstat_path = tagsistant.archive;
	}

	// do the real lstat()
	res = lstat(lstat_path, stbuf);
	tagsistant_errno = errno;

	// postprocess output
	if (QTREE_IS_TAGS(qtree)) {
		dbg(LOG_INFO, "getattr: last tag is %s", qtree->last_tag);
		if (g_strcmp0(qtree->last_tag, "+") == 0) {
			// path ends by '+'
			stbuf->st_ino += 1;
			stbuf->st_mode = S_IFDIR|S_IRUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH;
			stbuf->st_nlink = 1;
		} else if (g_strcmp0(qtree->last_tag, "=") == 0) {
			stbuf->st_ino += 2;
			stbuf->st_mode = S_IFDIR|S_IRUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH;
			stbuf->st_nlink = 1;
		} else if (NULL == qtree->last_tag) {
			// ok
		} else {
			if (sql_tag_exists(qtree->last_tag)) {
				// each directory holds 3 inodes: itself/, itself/+, itself/=
				stbuf->st_ino = get_exact_tag_id(qtree->last_tag) * 3;
			} else {
				tagsistant_errno = ENOENT;
				res = -1;
			}
		}
	} else if (QTREE_IS_STATS(qtree) || QTREE_IS_RELATIONS(qtree)) {
		// mangle inode for relations and stats
	}

GETATTR_EXIT:
	stop_labeled_time_profile("getattr");

	if ( res == -1 ) {
		dbg(LOG_ERR, "GETATTR on %s (%s) {%s}: %d %d: %s", path, lstat_path, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		dbg(LOG_INFO, "GETATTR on %s (%s): OK", path, query_type(qtree));
	}

	destroy_querytree(qtree);
	return (res == -1) ? -tagsistant_errno : 0;
}

/**
 * readlink equivalent
 *
 * \param path the path of the symlink to be read
 * \param buf the path the symlink is pointing to
 * \param size length of pointed path
 * \return 0 on success, -errno otherwise
 */
static int tagsistant_readlink(const char *path, char *buf, size_t size)
{
    int res = 0, tagsistant_errno = 0;
	gchar *readlink_path = NULL;

	init_time_profile();
	start_time_profile();

	// build querytree
	querytree_t *qtree = build_querytree(path, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
		goto READLINK_EXIT;
	}

	if ((QTREE_IS_TAGS(qtree) && QTREE_IS_COMPLETE(qtree)) || QTREE_IS_ARCHIVE(qtree)) {
		readlink_path = qtree->object_path;
	} else if (QTREE_IS_STATS(qtree) || QTREE_IS_RELATIONS(qtree)) {
		res = -1;
		tagsistant_errno = EINVAL;
		goto READLINK_EXIT;
	}

	// do real readlink()
	res = readlink(readlink_path, buf, size);
	tagsistant_errno = errno;
	if (res > 0) buf[res] = '\0';

READLINK_EXIT:
	if ( res == -1 ) {
		dbg(LOG_ERR, "READLINK on %s (%s) (%s): %d %d: %s", path, readlink_path, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		dbg(LOG_INFO, "REALINK on %s (%s): OK", path, query_type(qtree));
	}

	destroy_querytree(qtree);
	return (res == -1) ? -tagsistant_errno : 0;
}

/**
 * used by add_entry_to_dir() SQL callback to perform readdir() operations
 */
struct use_filler_struct {
	fuse_fill_dir_t filler;	/**< libfuse filler hook to return dir entries */
	void *buf;				/**< libfuse buffer to hold readdir results */
	const char *path;		/**< the path that generates the query */
	querytree_t *qtree;		/**< the querytree that originated the readdir() */
};

/**
 * SQL callback. Add dir entries to libfuse buffer.
 *
 * \param filler_ptr struct use_filler_struct pointer (cast to void*)
 * \param argc argv counter
 * \param argv array of SQL results
 * \param azColName array of column names
 * \return 0 (always, see SQLite policy)
 */
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

	/* check if this tag has been already listed inside the path */
	ptree_or_node_t *ptx = ufs->qtree->tree;
	while (NULL != ptx->next) ptx = ptx->next; // last OR section

	ptree_and_node_t *and_t = ptx->and_set;
	while (NULL != and_t) {
		if (g_strcmp0(and_t->tag, argv[0]) == 0) {
			return 0;
		}
		and_t = and_t->next;
	}

	/*
	char *path_duplicate = g_strdup(ufs->path);
	if (path_duplicate == NULL) {
		dbg(LOG_ERR, "Error duplicating path");
		return 0;
	}
	char *last_subquery = path_duplicate;
	while (strstr(last_subquery, "/+") != NULL) {
		last_subquery = strstr(last_subquery, "/+") + strlen("/+");
	}

	gchar *tag_to_check = g_strdup_printf("/%s/", argv[0]);

	if (strstr(last_subquery, tag_to_check) != NULL) {
		freenull(tag_to_check);
		freenull(path_duplicate);
		return 0;
	}

	freenull(tag_to_check);
	freenull(path_duplicate);
	*/

	return ufs->filler(ufs->buf, argv[0], NULL, 0);
}

/**
 * readdir equivalent (in FUSE paradigm)
 *
 * \param path the path of the directory to be read
 * \param buf buffer holding directory entries
 * \param filler libfuse fuse_fill_dir_t function to save entries in *buf
 * \param offset offset of next read
 * \param fi struct fuse_file_info passed by libfuse; unused.
 * \return 0 on success, -errno otherwise
 */
static int tagsistant_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
	int res = 0, tagsistant_errno = 0;
	struct dirent *de;

	(void) fi;
	(void) offset;

	// add . and ..
	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

	// dbg(LOG_INFO, "READDIR on %s", path);

	// build querytree
	querytree_t *qtree = build_querytree(path, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
		goto READDIR_EXIT;
	}

	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		dbg(LOG_INFO, "readdir on object %s", path);
		DIR *dp = opendir(qtree->object_path);
		if (dp == NULL) {
			tagsistant_errno = errno;
			goto READDIR_EXIT;
		}

		while ((de = readdir(dp)) != NULL) {
			struct stat st;
			memset(&st, 0, sizeof(st));
			st.st_ino = de->d_ino;
			st.st_mode = de->d_type << 12;
			if (filler(buf, de->d_name, &st, 0))
				break;
		}

		closedir(dp);
	} else if (QTREE_IS_ROOT(qtree)) {
		dbg(LOG_INFO, "readdir on root %s", path);
		/*
		 * insert pseudo directories: tags/ archive/ relations/ and stats/
		 */
		filler(buf, "archive", NULL, 0);
		filler(buf, "relations", NULL, 0);
		filler(buf, "stats", NULL, 0);
		filler(buf, "tags", NULL, 0);
	} else if (QTREE_IS_ARCHIVE(qtree)) {
		/*
		 * already served by QTREE_POINTS_TO_OBJECT()?
		 */
		DIR *dp = opendir(tagsistant.archive);
		if (dp == NULL) {
			tagsistant_errno = errno;
			goto READDIR_EXIT;
		}

		while ((de = readdir(dp)) != NULL) {
			struct stat st;
			memset(&st, 0, sizeof(st));
			st.st_ino = de->d_ino;
			st.st_mode = de->d_type << 12;
			if (filler(buf, de->d_name, &st, 0))
				break;
		}
	} else if (QTREE_IS_TAGS(qtree)) {
		if (qtree->complete) {
			// build the filetree
			file_handle_t *fh = build_filetree(qtree->tree, path);

			// check filetree is not null
			if (NULL == fh) {
				tagsistant_errno = EBADF;
				goto READDIR_EXIT;
			}
	
			// save filetree reference to later destroy it
			file_handle_t *fh_save = fh;
		
			// add each filetree node to directory
			do {
				if ( (fh->name != NULL) && strlen(fh->name)) {
					dbg(LOG_INFO, "Adding %s to directory", fh->name);
					if (filler(buf, fh->name, NULL, offset))
						break;
				}
				fh = fh->next;
			} while ( fh != NULL && fh->name != NULL );
		
			// destroy the file tree
			destroy_filetree(fh_save);
		} else {
			/*
		 	* if path does not terminate by =,
		 	* directory should be filled with tagsdir registered tags
		 	*/
			filler(buf, "+", NULL, 0);
			filler(buf, "=", NULL, 0);

			struct use_filler_struct *ufs = g_new0(struct use_filler_struct, 1);
			if (ufs == NULL) {
				dbg(LOG_ERR, "Error allocating memory");
				goto READDIR_EXIT;
			}
	
			ufs->filler = filler;
			ufs->buf = buf;
			ufs->path = path;
			ufs->qtree = qtree;
	
			/* parse tagsdir list */
			tagsistant_query("select tagname from tags;", add_entry_to_dir, ufs);
			freenull(ufs);
		}
	} else if (QTREE_IS_STATS(qtree)) {
		// fill with available statistics
	} else if (QTREE_IS_RELATIONS(qtree)) {
		if (qtree->second_tag) {
			// nothin'
		} else if (qtree->relation) {
			// list all tags related to first_tag with this relation
		} else if (qtree->first_tag) {
			// list all relations
		} else {
			// list all tags
		}
	}

#if 0
	char *tagname = get_tag_name(path);
	if (
		strlen(tagname) > 1 &&
		(strcmp(tagname,"AND") != 0) &&
		(strcmp(tagname,"OR") != 0)
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
		
		querytree_t *qtree = build_querytree(path, TRUE);
		if (qtree == NULL) {
			freenull(tagname);
			return -EBADF;
		}
	
		file_handle_t *fh = build_filetree(qtree->tree, path);
		if (fh == NULL) {
			freenull(tagname);
			destroy_querytree(qtree);
			return -EBADF;
		}

		file_handle_t *fh_save = fh;
	
		do {
			if ( (fh->name != NULL) && strlen(fh->name)) {
				dbg(LOG_INFO, "Adding %s to directory", fh->name);
				if (filler(buf, fh->name, NULL, offset))
					break;
			}
			fh = fh->next;
		} while ( fh != NULL && fh->name != NULL );
	
		destroy_querytree(qtree);
		destroy_filetree(fh_save);
		freenull(tagname);

	} else {

		dbg(LOG_INFO, "%s terminate with and operator or is root", path);

		/*
		 * if path does terminate with a logical operator
		 * directory should be filled with tagsdir registered tags
		 */
		struct use_filler_struct *ufs = g_new0(struct use_filler_struct, 1);
		if (ufs == NULL) {
			dbg(LOG_ERR, "Error allocating memory");
			freenull(tagname);
			return 0;
		}

		ufs->filler = filler;
		ufs->buf = buf;
		ufs->path = path;

		/* parse tagsdir list */
		tagsistant_query("select tagname from tags;", add_entry_to_dir, ufs);
		freenull(ufs);
	}
#endif

READDIR_EXIT:
	if ( res == -1 ) {
		dbg(LOG_ERR, "READDIR on %s (%s): %d %d: %s", path, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		dbg(LOG_INFO, "READDIR on %s (%s): OK", path, query_type(qtree));
	}

	destroy_querytree(qtree);
	return (res == -1) ? -tagsistant_errno : 0;
}

/**
 * mknod equivalent (used to create even regular files)
 *
 * \param path the path of the file (block, char, fifo) to be created
 * \param mode file type and permissions
 * \param rdev major and minor numbers, if applies
 * \return 0 on success, -errno otherwise
 */
static int tagsistant_mknod(const char *path, mode_t mode, dev_t rdev)
{
	int res = 0, tagsistant_errno = 0;
	const char *mknod_path = path;

	init_time_profile();
	start_time_profile();

	// build querytree
	querytree_t *qtree = build_querytree(path, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		res = -1;
		tagsistant_errno = EFAULT;
		goto MKNOD_EXIT;
	} else

	// -- tags --
	// -- archive --
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		if (QTREE_IS_TAGGABLE(qtree)) {
			// 1. create the object on db
			tagsistant_id ID;
			tagsistant_query("insert into objects (objectname, path) values (\"%s\", \"%s\")", NULL, NULL, qtree->object_path, qtree->full_archive_path);
			tagsistant_query("select last_insert_rowid()", return_integer, &ID);

			// 2. tag it using qtree for all the tags
			traverse_querytree(qtree, sql_tag_object, ID);
		}

		res = mknod(qtree->full_archive_path, mode, rdev);
		tagsistant_errno = errno;
	} else
	
	// -- stats --
	// -- relations --
	{
		res = -1;
		tagsistant_errno = EROFS;
	}

#if 0
	/* the old code used to tag an object */
		dbg(LOG_INFO, "Tagging file %s as %s inside tagsistant_mknod()", filename, tagname);

		// 1. inserting file into "files" table with a temporary path
		tagsistant_query("insert into objects(basename, path) values(\"%s\")", NULL, NULL, filename, "-to-be-changed-");

		// 2. fetching the index of the last insert query
		int file_id;
		tagsistant_query("select last_insert_rowid()", return_integer, &file_id);

		// 3. creating the full file path as "<tagsistant.archive>/<id>.<filename>"
		fullfilename = g_strdup_printf("%s%d.%s", tagsistant.archive, file_id, filename);

		// 4. executing real mknod (and simulating a rollback in case of error)
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
				dbg(LOG_ERR, "mknod(%s, %u, %u) failed: %s", fullfilename, mode, (unsigned int) rdev, strerror(tagsistant_errno));
			}

			// elimino la entry nella tabella file
			tagsistant_query("delete from objects where id = %d", NULL, NULL, file_id);
		}

		// 5. completo la entry dentro "files" con il path completo
		tagsistant_query("update objects set path = \"%s\" where id = %d", NULL, NULL, fullfilename, file_id);

		// 6. taggo il file
		tagsistant_query("insert into tagging (file_id, tagname) values (%d, \"%s\")", NULL, NULL, file_id, tagname);

		dbg(LOG_INFO, "mknod(%s): %d %s", path, res, strerror(tagsistant_errno));

		// 7. dealloco la memoria
		freenull(fullfilename);

		process(file_id);
	}

	freenull(filename);
	freenull(tagname);
#endif

MKNOD_EXIT:
	stop_labeled_time_profile("mknod");

	if ( res == -1 ) {
		dbg(LOG_ERR, "MKNOD on %s (%s) (%s): %d %d: %s", path, mknod_path, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		dbg(LOG_INFO, "MKNOD on %s (%s): OK", path, query_type(qtree));
	}

	destroy_querytree(qtree);
	return (res == -1) ? -tagsistant_errno : 0;
}

/**
 * mkdir equivalent
 *
 * \param path the path of the directory to be created
 * \param mode directory permissions (unused, since directories are tags saved in SQL backend)
 * \return 0 on success, -errno otherwise
 */
static int tagsistant_mkdir(const char *path, mode_t mode)
{
	(void) mode;
    int res = 0, tagsistant_errno = 0;

	init_time_profile();
	start_time_profile();

	// build querytree
	querytree_t *qtree = build_querytree(path, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
	} else

	// -- tags --
	// -- archive
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		if (QTREE_IS_TAGGABLE(qtree)) {
			// create a new directory inside tagsistant.archive directory
			// and tag it with all the tags in the qtree
			tagsistant_id ID;
			tagsistant_query("insert into objects (objectname, path) values (\"%s\", \"%s\")", NULL, NULL, qtree->object_path, qtree->full_archive_path);
			tagsistant_query("select last_insert_rowid()", return_integer, &ID);
			traverse_querytree(qtree, sql_tag_object, ID);
		} else {
			// do a real mkdir
			res = mkdir(qtree->full_archive_path, mode);
			tagsistant_errno = errno;
		}
	} else

	// -- tags but incomplete (means: create a new tag) --
	if (QTREE_IS_TAGS(qtree)) {
		sql_create_tag(qtree->last_tag);	
	} else

	// -- relations --
	if (QTREE_IS_RELATIONS(qtree)) {
		// mkdir can be used only on third level
		// since first level is all available tags
		// and second level is all available relations
		if (qtree->second_tag) {
			// create a new relation between two tags	
			sql_create_tag(qtree->second_tag);
			int tag1_id = get_exact_tag_id(qtree->first_tag);
			int tag2_id = get_exact_tag_id(qtree->second_tag);
			if (tag1_id && tag2_id && IS_VALID_RELATION(qtree->relation)) {
				tagsistant_query(
					"insert into relations (tag1_id, tag2_id, relation) values (%d, %d, \"%s\")",
					NULL, NULL, tag1_id, tag2_id, qtree->relation);
			} else {
				res = -1;
				tagsistant_errno = EFAULT;
			}
		} else {
			res = -1;
			tagsistant_errno = EROFS;
		}
	} else

	// -- stats
	if (QTREE_IS_STATS(qtree)) {
		res = -1;
		tagsistant_errno = EROFS;
	}
	
	stop_labeled_time_profile("mkdir");

	if ( res == -1 ) {
		dbg(LOG_ERR, "MKDIR on %s (%s): %d %d: %s", path, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		dbg(LOG_INFO, "MKDIR on %s (%s): OK", path, query_type(qtree));
	}

	destroy_querytree(qtree);
	return (res == -1) ? -tagsistant_errno : 0;
}

/**
 * unlink equivalent
 *
 * \param path the path to be unlinked (deleted)
 * \return 0 on success, -errno otherwise
 */
static int tagsistant_unlink(const char *path)
{
    int res = 0, tagsistant_errno = 0;

	init_time_profile();
	start_time_profile();

	// build querytree
	querytree_t *qtree = build_querytree(path, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
	} else

	// -- objects on disk --
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		if (QTREE_IS_TAGGABLE(qtree)) {
			traverse_querytree(qtree, sql_untag_object, qtree->object_id);
			tagsistant_query("delete from objects where object_id = %d", NULL, NULL, qtree->object_id);
		}

		res = unlink(qtree->full_archive_path);
		tagsistant_errno = errno;
	} else

	// -- tags --
	// -- stats --
	// -- relations --
	{
		res = -1;
		tagsistant_errno = EROFS;
	}

	stop_labeled_time_profile("unlink");

	if ( res == -1 ) {
		dbg(LOG_ERR, "UNLINK on %s (%s) (%s): %d %d: %s", path, qtree->full_archive_path, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		dbg(LOG_INFO, "UNLINK on %s (%s): OK", path, query_type(qtree));
	}

	destroy_querytree(qtree);
	return (res == -1) ? -tagsistant_errno : 0;
}

/**
 * rmdir equivalent
 *
 * \param path the tag (directory) to be removed
 * \return 0 on success, -errno otherwise
 */
static int tagsistant_rmdir(const char *path)
{
    int res = 0, tagsistant_errno = 0;
	init_time_profile();
	start_time_profile();

	querytree_t *qtree = build_querytree(path, FALSE);

#if 0
	ptree_or_node_t *ptx = qtree->tree;

	/* tag name is inserted 2 times in query, that's why '* 2' */
	while (ptx != NULL) {
		ptree_and_node_t *element = ptx->and_set;
		while (element != NULL) {
			dbg(LOG_INFO, "RMDIR on %s", element->tag);
			sql_delete_tag(element->tag);
			element = element->next;
		}
		ptx = ptx->next;
	}
#endif

	stop_labeled_time_profile("rmdir");

	if ( res == -1 ) {
		dbg(LOG_ERR, "RMDIR on %s (%s): %d %d: %s", path, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		dbg(LOG_INFO, "RMDIR on %s (%s): OK", path, query_type(qtree));
	}

	destroy_querytree(qtree);
	return (res == -1) ? -tagsistant_errno : 0;
}

/**
 * rename equivalent
 *
 * \param from old file name
 * \param to new file name
 * \return 0 on success, -errno otherwise
 */
static int tagsistant_rename(const char *from, const char *to)
{
    int res = 0, tagsistant_errno = 0;

	init_time_profile();
	start_time_profile();

	querytree_t *from_tree = build_querytree(from, FALSE);
	if (from_tree == NULL) {
		tagsistant_errno = ENOMEM;
		goto RETURN;
	}

	querytree_t *to_tree = build_querytree(to, FALSE);
	if (to_tree == NULL) {
		tagsistant_errno = ENOMEM;
		goto RETURN;
	}

	// 1. rename "from" file into "to" file, if the case
	if (g_strcmp0(from_tree->object_path, to_tree->object_path)) {
		res = rename(from_tree->object_path, to_tree->object_path);
		if (res == -1) {
			tagsistant_errno = errno;
			dbg(LOG_ERR, "Error renaming %s to %s", from_tree->object_path, to_tree->object_path);
			goto RETURN;
		}
	}

	if (g_strstr_len(from_tree->object_path, -1, "/") == NULL) {
		// get the object id
		/* ..... */

		// 2. deletes all the tagging between "from" file and all AND nodes in "from" path
		traverse_querytree(from_tree, sql_untag_object, from_tree->object_id);

		// 3. adds all the tags from "to" path
		traverse_querytree(to_tree, sql_tag_object, from_tree->object_id);
	}

RETURN:
	stop_labeled_time_profile("rename");

	if ( res == -1 ) {
		dbg(LOG_ERR, "RENAME %s (%s) to %s (%s): %d %d: %s", from, query_type(from_tree), to, query_type(to_tree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		dbg(LOG_INFO, "RENAME %s (%s) to %s (%s): OK", from, query_type(from_tree), to, query_type(to_tree));
	}

	destroy_querytree(from_tree);
	destroy_querytree(to_tree);
	return (res == -1) ? -tagsistant_errno : 0;
}

/**
 * symlink equivalent
 *
 * \param from existing file name
 * \param to new file name
 * \return 0 on success, -errno otherwise
 */
static int tagsistant_symlink(const char *from, const char *to)
{
    int tagsistant_errno = 0, res = 0;

	// remove last slash and following part from to
	gchar *dir_to = g_strdup(to);
	gchar *last_slash = g_strrstr(dir_to, "/");
	if (NULL != last_slash) {
		*last_slash = '\0';
	}

	init_time_profile();
	start_time_profile();

	querytree_t *to_qtree = build_querytree(dir_to, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(to_qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
	} else

	// -- object on disk --
	if (QTREE_POINTS_TO_OBJECT(to_qtree) || (QTREE_IS_TAGS(to_qtree) && QTREE_IS_COMPLETE(to_qtree))) {

		// if object_path is null, borrow it from original path
		if (strlen(to_qtree->object_path) == 0) {
			qtree_set_object_path(to_qtree, g_strdup(g_path_get_basename(from)));
		}

		// if qtree is taggable, do it
		if (QTREE_IS_TAGGABLE(to_qtree)) {
			tagsistant_id ID = sql_create_object(to_qtree->object_path, to_qtree->archive_path);
			traverse_querytree(to_qtree, sql_tag_object, ID);
		}

		// do the real symlink on disk
		res = symlink(from, to_qtree->full_archive_path);
		tagsistant_errno = errno;
	} else

	// -- tags (uncomplete) --
	// -- stats --
	// -- relations --
	{
		// nothin'?
		dbg(LOG_INFO, "%s non punta a un oggetto e non è una tags query completa", dir_to);
	}

	stop_labeled_time_profile("symlink");

	if ( res == -1 ) {
		dbg(LOG_ERR, "SYMLINK from %s to %s (%s) (%s): %d %d: %s", from, to, to_qtree->full_archive_path, query_type(to_qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		dbg(LOG_INFO, "SYMLINK from %s to %s (%s): OK", from, to, query_type(to_qtree));
	}

	destroy_querytree(to_qtree);
	g_free(dir_to);
	return (res == -1) ? -tagsistant_errno : 0;
}

/**
 * link equivalent
 *
 * \param from existing file name
 * \param to new file name
 * \return 0 on success, -errno otherwise
 * \todo why tagsistant_link() calls tagsistant_symlink()? We should be able to perform proper linking!
 */
static int tagsistant_link(const char *from, const char *to)
{
	return tagsistant_symlink(from, to);
}

/**
 * chmod equivalent
 *
 * \param path the path to be chmod()ed
 * \param mode new mode for path
 * \return 0 on success, -errno otherwise
 */
static int tagsistant_chmod(const char *path, mode_t mode)
{
    int res = 0, tagsistant_errno = 0;

	init_time_profile();
	start_time_profile();

	querytree_t *qtree = build_querytree(path, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
	} else

	// -- object on disk --
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		res = chmod(qtree->full_archive_path, mode);
		tagsistant_errno = errno;
	} else

	// -- tags --
	// -- stats --
	// -- relations --
	{
		res = -1;
		tagsistant_errno = EROFS;
	}

	stop_labeled_time_profile("chmod");

	if ( res == -1 ) {
		dbg(LOG_ERR, "CHMOD %s (%s) as %d: %d %d: %s", qtree->full_archive_path, query_type(qtree), mode, res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		dbg(LOG_INFO, "CHMOD %s (%s), %d: OK", path, query_type(qtree), mode);
	}

	destroy_querytree(qtree);
	return (res == -1) ? -tagsistant_errno : 0;
}

/**
 * chown equivalent
 *
 * \param path the path to be chown()ed
 * \param uid new UID for path
 * \param gid new GID for path
 * \return 0 on success, -errno otherwise
 */
static int tagsistant_chown(const char *path, uid_t uid, gid_t gid)
{
    int res = 0, tagsistant_errno = 0;

	init_time_profile();
	start_time_profile();

	querytree_t *qtree = build_querytree(path, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
	} else

	// -- object on disk --
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		res = chown(qtree->full_archive_path, uid, gid);
		tagsistant_errno = errno;
	} else

	// -- tags --
	// -- stats --
	// -- relations --
	{
		res = -1;
		tagsistant_errno = EROFS;
	}

	stop_labeled_time_profile("chown");

	if ( res == -1 ) {
		dbg(LOG_ERR, "CHMOD %s to %d,%d (%s): %d %d: %s", qtree->full_archive_path, uid, gid, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		dbg(LOG_INFO, "CHMOD %s, %d, %d (%s): OK", path, uid, gid, query_type(qtree));
	}

	destroy_querytree(qtree);
	return (res == -1) ? -tagsistant_errno : 0;
}

/**
 * truncate equivalent
 *
 * \param path the path to be truncate()ed
 * \param size truncation offset
 * \return 0 on success, -errno otherwise
 */
static int tagsistant_truncate(const char *path, off_t size)
{
    int res = 0, tagsistant_errno = 0;

	init_time_profile();
	start_time_profile();

	querytree_t *qtree = build_querytree(path, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
	} else

	// -- object on disk --
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		res = truncate(qtree->full_archive_path, size);
		tagsistant_errno = errno;
	} else

	// -- tags --
	// -- stats --
	// -- relations --
	{
		res = -1;
		tagsistant_errno = EROFS;
	}

	stop_labeled_time_profile("truncate");

	if ( res == -1 ) {
		dbg(LOG_ERR, "TRUNCATE %s at %llu (%s): %d %d: %s", qtree->full_archive_path, (unsigned long long) size, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		dbg(LOG_INFO, "TRUNCATE %s, %llu (%s): OK", path, (unsigned long long) size, query_type(qtree));
	}

	destroy_querytree(qtree);
	return (res == -1) ? -tagsistant_errno : 0;
}

/**
 * utime equivalent
 *
 * \param path the path to utime()ed
 * \param buf struct utimbuf pointer holding new access and modification times
 * \return 0 on success, -errno otherwise
 */
static int tagsistant_utime(const char *path, struct utimbuf *buf)
{
    int res = 0, tagsistant_errno = 0;
	char *utime_path = NULL;

	init_time_profile();
	start_time_profile();

	querytree_t *qtree = build_querytree(path, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
	} else

	// -- object on disk --
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		utime_path = qtree->full_archive_path;
	} else

	// -- tags --
	// -- stats --
	// -- relations --
	{
		utime_path = tagsistant.archive;
	}

	// do the real utime()
	res = utime(utime_path, buf);
	tagsistant_errno = errno;

	stop_labeled_time_profile("utime");

	if ( res == -1 ) {
		dbg(LOG_ERR, "UTIME %s (%s): %d %d: %s", utime_path, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		dbg(LOG_INFO, "UTIME %s (%s): OK", path, query_type(qtree));
	}

	destroy_querytree(qtree);
	return (res == -1) ? -tagsistant_errno : 0;
}

/**
 * performs real open() on a file. Used by tagsistant_open(),
 * tagsistant_read() and tagsistant_write().
 *
 * \param filepath the path to be open()ed
 * \param flags how to open file (see open(2) for more informations)
 * \param _errno returns open() errno
 * \return open() return value
 * \todo Should it perform permissions checking???
 */
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

/**
 * open() equivalent
 *
 * \param path the path to be open()ed
 * \param fi struct fuse_file_info holding open() flags
 * \return 0 on success, -errno otherwise
 */
static int tagsistant_open(const char *path, struct fuse_file_info *fi)
{
    int res = -1, tagsistant_errno = ENOENT;

	init_time_profile();
	start_time_profile();

	gchar *open_path = NULL;

	// build querytree
	querytree_t *qtree = build_querytree(path, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
	} else

	// -- object --
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		open_path = qtree->full_archive_path;
		res = internal_open(qtree->full_archive_path, fi->flags|O_RDONLY, &tagsistant_errno);
		if (-1 != res) close(res);
	} else
	
	// -- stats -- 
	if (QTREE_IS_STATS(qtree)) {
		open_path = qtree->stats_path;
		// do proper action
	} else

	// -- tags --
	// -- relations --
	{
		res = -1;
		tagsistant_errno = EROFS;
	}

	stop_labeled_time_profile("open");

	if ( res == -1 ) {
		dbg(LOG_ERR, "OPEN on %s (%s) (%s): %d %d: %s", path, open_path, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		dbg(LOG_INFO, "OPEN on %s (%s): OK", path, query_type(qtree));
	}

	destroy_querytree(qtree);
	return (res == -1) ? -tagsistant_errno : 0;
}

/**
 * read() equivalent
 *
 * \param path the path of the file to be read
 * \param buf buffer holding read() result
 * \param size how many bytes should/can be read
 * \param offset starting of the read
 * \param fi struct fuse_file_info used for open() flags
 * \return 0 on success, -errno otherwise
 */
static int tagsistant_read(const char *path, char *buf, size_t size, off_t offset,
                    struct fuse_file_info *fi)
{
    int res = 0, tagsistant_errno = 0;

	init_time_profile();
	start_time_profile();

	querytree_t *qtree = build_querytree(path, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
	} else

	// -- object on disk --
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		int fd = internal_open(qtree->full_archive_path, fi->flags|O_RDONLY, &tagsistant_errno); 
		if (fd != -1) {
			res = pread(fd, buf, size, offset);
			tagsistant_errno = errno;
			close(fd);
		} else {
			res = -1;
			tagsistant_errno = errno;
		}
	} else

	// -- stats --
	if (QTREE_IS_STATS(qtree)) {
		// do what is needed
	} else

	// -- tags --
	// -- relations --
	{
		res = -1;
		tagsistant_errno = EROFS;
	}

	stop_labeled_time_profile("read");

	if ( res == -1 ) {
		dbg(LOG_ERR, "READ %s (%s) (%s): %d %d: %s", path, qtree->full_archive_path, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		dbg(LOG_INFO, "READ %s (%s): OK", path, query_type(qtree));
	}

	destroy_querytree(qtree);
	return (res == -1) ? -tagsistant_errno : res;
}

/**
 * write() equivalent
 *
 * \param path the path of the file to be written
 * \param buf buffer holding write() data
 * \param size how many bytes should be written (size of *buf)
 * \param offset starting of the write
 * \param fi struct fuse_file_info used for open() flags
 * \return 0 on success, -errno otherwise
 */
static int tagsistant_write(const char *path, const char *buf, size_t size,
	off_t offset, struct fuse_file_info *fi)
{
    int res = 0, tagsistant_errno = 0;

	init_time_profile();
	start_time_profile();

	querytree_t *qtree = build_querytree(path, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
	} else

	// -- object on disk --
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		int fd = internal_open(qtree->full_archive_path, fi->flags|O_WRONLY, &tagsistant_errno); 
		if (fd != -1) {
			res = pwrite(fd, buf, size, offset);
			tagsistant_errno = errno;
			close(fd);
		} else {
			res = -1;
			tagsistant_errno = errno;
		}
	} else

	// -- tags --
	// -- stats --
	// -- relations --
	{
		res = -1;
		tagsistant_errno = EROFS;
	}

	stop_labeled_time_profile("write");

	if ( res == -1 ) {
		dbg(LOG_ERR, "WRITE %s (%s) (%s): %d %d: %s", path, qtree->full_archive_path, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		dbg(LOG_INFO, "WRITE %s (%s): OK", path, query_type(qtree));
	}

	destroy_querytree(qtree);
	return (res == -1) ? -tagsistant_errno : res;
}

#if FUSE_USE_VERSION >= 25

/**
 * statvfs equivalent (used on fuse >= 25)
 *
 * \param path the path to be statvfs()ed
 * \param stbuf pointer to struct statvfs holding filesystem informations
 * \return 0 on success, -errno otherwise
 */
static int tagsistant_statvfs(const char *path, struct statvfs *stbuf)
{
    int res = 0, tagsistant_errno = 0;
	(void) path;

	init_time_profile();
	start_time_profile();
	
	res = statvfs(tagsistant.repository, stbuf);
	tagsistant_errno = errno;

	stop_labeled_time_profile("statvfs");

	if ( res == -1 ) {
		dbg(LOG_ERR, "STATVFS on %s: %d %d: %s", path, res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		dbg(LOG_INFO, "STATVFS on %s: OK", path);
	}

	return (res == -1) ? -tagsistant_errno : 0;
}

#else

/**
 * statfs equivalent (used on fuse < 25)
 *
 * \param path the path to be statfs()ed
 * \param stbuf pointer to struct statfs holding filesystem informations
 * \return 0 on success, -errno otherwise
 */
static int tagsistant_statfs(const char *path, struct statfs *stbuf)
{
    int res = 0, tagsistant_errno = 0;
	(void) path;

	init_time_profile();
	start_time_profile();
	
	res = statfs(tagsistant.repository, stbuf);
	tagsistant_errno = errno;

	stop_labeled_time_profile("statfs");

	if ( res == -1 ) {
		dbg(LOG_ERR, "STATFS on %s: %d %d: %s", path, res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		dbg(LOG_INFO, "STATFS on %s: OK", path);
	}

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

/**
 * access() equivalent.
 *
 * \param path the path of the filename to be access()ed
 * \int mode the mode which is F_OK|R_OK|W_OK|X_OK
 * \return 0 on success, -errno on error
 */
static int tagsistant_access(const char *path, int mode)
{
	(void) mode;

	/*
	if (mode & X_OK) {
		dbg(LOG_ERR, "ACCESS on %s: -1 %d: %s", path, EACCES, strerror(EACCES));
		return -EACCES;
	}
	*/

	struct stat st;
	int res = tagsistant_getattr(path, &st);
	if (res == 0) {
		dbg(LOG_INFO, "ACCESS on %s: OK", path);
		return 0;
	}

	dbg(LOG_ERR, "ACCESS on %s: -1 %d: %s", path, EACCES, strerror(EACCES));
	return -EACCES;
}

#if FUSE_VERSION >= 26

static void *tagsistant_init(struct fuse_conn_info *conn)
{
	(void) conn;
	return NULL;
}

#else

static void *tagsistant_init(void)
{
	return NULL;
}

#endif

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
#if FUSE_USE_VERSION >= 25
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
	.access		= tagsistant_access,
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
	TAGSISTANT_OPT("-v",					verbose,		1),
	TAGSISTANT_OPT("-q",					quiet,			1),
	
	FUSE_OPT_KEY("-V",          	KEY_VERSION),
	FUSE_OPT_KEY("--version",   	KEY_VERSION),
	FUSE_OPT_KEY("-h",          	KEY_HELP),
	FUSE_OPT_KEY("--help",      	KEY_HELP),
	FUSE_OPT_END
};

int usage_already_printed = 0;

/**
 * print usage message on STDOUT
 *
 * \param progname the name tagsistant was invoked as
 */
void usage(char *progname)
{
	if (usage_already_printed++)
		return;

	fprintf(stderr, "\n"
		" Tagsistant (tagfs) v.%s FUSE_USE_VERSION: %d\n"
		" Semantic File System for Linux kernels\n"
		" (c) 2006-2009 Tx0 <tx0@strumentiresistenti.org>\n"
		" \n"
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
		"    -q  be quiet\n"
		"    -r  mount readonly\n"
		"    -v  verbose syslogging\n"
		"\n" /*fuse options will follow... */
		, PACKAGE_VERSION, FUSE_USE_VERSION, progname
	);
}

/**
 * process command line options
 * 
 * \param data pointer (unused)
 * \param arg argument pointer (if key has one)
 * \param key command line option to be processed
 * \param outargs structure holding libfuse options
 * \return 1 on success, 0 otherwise
 */
static int tagsistant_opt_proc(void *data, const char *arg, int key, struct fuse_args *outargs)
{
    (void) data;

    switch (key) {
		case FUSE_OPT_KEY_NONOPT:
			if (!tagsistant.mountpoint) {
				tagsistant.mountpoint = g_strdup(arg);
				return 1;
			}
			return 0;

	    case KEY_HELP:
	        usage(outargs->argv[0]);
	        fuse_opt_add_arg(outargs, "-ho");
#if FUSE_VERSION <= 25
	        fuse_main(outargs->argc, outargs->argv, &tagsistant_oper);
#else
	        fuse_main(outargs->argc, outargs->argv, &tagsistant_oper, NULL);
#endif
	        exit(1);
	
	    case KEY_VERSION:
	        fprintf(stderr, "Tagsistant for Linux 0.1 (prerelease %s)\n", VERSION);
#if FUSE_VERSION >= 25
	        fuse_opt_add_arg(outargs, "--version");
#endif
#if FUSE_VERSION == 25
	        fuse_main(outargs->argc, outargs->argv, &tagsistant_oper);
#else
			fuse_main(outargs->argc, outargs->argv, &tagsistant_oper, NULL);
#endif
	        exit(0);
	
	    default:
	        fprintf(stderr, "Extra parameter provided\n");
	        usage(outargs->argv[0]);
    }

	return 0;
}

/**
 * cleanup hook used by signal()
 *
 * \param s the signal number passed by signal()
 */
void cleanup(int s)
{
	dbg(LOG_ERR, "Got Signal %d", s);
	/* sqlite3_close(tagsistant.dbh); */
	exit(s);
}

int main(int argc, char *argv[])
{
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	int res;

#ifndef MACOSX
	char *destfile = getenv("MALLOC_TRACE");
	if (destfile != NULL && strlen(destfile)) {
		fprintf(stderr, " *** logging g_malloc() calls to %s ***\n", destfile);
		mtrace();
	}
#endif

#ifdef DEBUG_TO_LOGFILE
	open_debug_file();	
#endif

	tagsistant.progname = argv[0];

	if (fuse_opt_parse(&args, &tagsistant, tagsistant_opts, tagsistant_opt_proc) == -1)
		exit(1);

	fuse_opt_add_arg(&args, "-ofsname=tagsistant");
	fuse_opt_add_arg(&args, "-ouse_ino,readdir_ino");
	fuse_opt_add_arg(&args, "-oallow_other");

#ifdef MACOSX
	fuse_opt_add_arg(&args, "-odefer_permissions");
	gchar *volname = g_strdup_printf("-ovolname=%s", tagsistant.mountpoint);
	fuse_opt_add_arg(&args, volname);
	freenull(volname);
#else
	/* fuse_opt_add_arg(&args, "-odefault_permissions"); */
#endif

	/***
	 * TODO
	 * NOTE
	 *
	 * SQLite library is often called "out of sequence" like in
	 * performing a query before having connected to sql database.
	 * to temporary solve this problem we force here single threaded
	 * operations. should be really solved by better real_do_sql()
	 */
	if (!tagsistant.quiet)
		fprintf(stderr, " *** forcing single thread mode until our SQLite interface is broken! ***\n");
	tagsistant.singlethread = 1;

	if (tagsistant.singlethread) {
		if (!tagsistant.quiet)
			fprintf(stderr, " *** operating in single thread mode ***\n");
		fuse_opt_add_arg(&args, "-s");
	}
	if (tagsistant.readonly) {
		if (!tagsistant.quiet)
			fprintf(stderr, " *** mounting tagsistant read-only ***\n");
		fuse_opt_add_arg(&args, "-r");
	}
	if (tagsistant.foreground) {
		if (!tagsistant.quiet)
			fprintf(stderr, " *** will run in foreground ***\n");
		fuse_opt_add_arg(&args, "-f");
	}
	if (tagsistant.verbose) {
		if (!tagsistant.quiet)
			fprintf(stderr, " *** will log verbosely ***\n");
		fuse_opt_add_arg(&args, "-d");
	}

	/* checking mountpoint */
	if (!tagsistant.mountpoint) {
		usage(tagsistant.progname);
		fprintf(stderr, " *** No mountpoint provided *** \n\n");
		exit(2);
	}

	/* checking if mount point exists or can be created */
	struct stat mst;
	if ((lstat(tagsistant.mountpoint, &mst) == -1) && (errno == ENOENT)) {
		if (mkdir(tagsistant.mountpoint, S_IRWXU|S_IRGRP|S_IXGRP) != 0) {
			usage(tagsistant.progname);
			if (!tagsistant.quiet)
				fprintf(stderr, "\n    Mountpoint %s does not exists and can't be created!\n\n", tagsistant.mountpoint);
			exit(1);
		}
	}

	if (!tagsistant.quiet)
		fprintf(stderr, "\n");
	if (!tagsistant.quiet)
		fprintf(stderr,
		" Tagsistant (tagfs) v.%s FUSE_USE_VERSION: %d\n"
		" (c) 2006-2009 Tx0 <tx0@strumentiresistenti.org>\n"
		" For license informations, see %s -h\n\n"
		, PACKAGE_VERSION, FUSE_USE_VERSION, tagsistant.progname
	);
	
	/* checking repository */
	if (!tagsistant.repository || (strcmp(tagsistant.repository, "") == 0)) {
		if (strlen(getenv("HOME"))) {
			freenull(tagsistant.repository);
			tagsistant.repository = g_strdup_printf("%s/.tagsistant", getenv("HOME"));
			if (!tagsistant.quiet)
				fprintf(stderr, " Using default repository %s\n", tagsistant.repository);
		} else {
			usage(tagsistant.progname);
			if (!tagsistant.quiet)
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
			char *relative_path = g_strdup(tagsistant.repository + 1);
			freenull(tagsistant.repository);
			tagsistant.repository = g_strdup_printf("%s%s", home_path, relative_path);
			freenull(relative_path);
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
			gchar *absolute_repository = g_strdup_printf("%s/%s", cwd, tagsistant.repository);
			freenull(tagsistant.repository);
			tagsistant.repository = absolute_repository;
			dbg(LOG_ERR, "Repository path is %s", tagsistant.repository);
		}
	}

	struct stat repstat;
	if (lstat(tagsistant.repository, &repstat) == -1) {
		if(mkdir(tagsistant.repository, 755) == -1) {
			if (!tagsistant.quiet)
				fprintf(stderr, " *** REPOSITORY: Can't mkdir(%s): %s ***\n\n", tagsistant.repository, strerror(errno));
			exit(2);
		}
	}
	chmod(tagsistant.repository, S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH);

	/* opening (or creating) SQL tags database */
	tagsistant.tags = g_strdup_printf("%s/tags.sql", tagsistant.repository);

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
		tagsistant_init_database();
	}

	/* tagsistant.dbh is no longer used as a permanent connection */
	sqlite3_close(tagsistant.dbh);

	/* checking file archive directory */
	tagsistant.archive = g_strdup_printf("%s/archive/", tagsistant.repository);

	if (lstat(tagsistant.archive, &repstat) == -1) {
		if(mkdir(tagsistant.archive, 755) == -1) {
			if (!tagsistant.quiet)
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
	plugin_loader();

	dbg(LOG_INFO, "Mounting filesystem");

	dbg(LOG_INFO, "Fuse options:");
	int fargc = args.argc;
	while (fargc) {
		dbg(LOG_INFO, "%.2d: %s", fargc, args.argv[fargc]);
		fargc--;
	}

#if FUSE_VERSION <= 25
	res = fuse_main(args.argc, args.argv, &tagsistant_oper);
#else
	res = fuse_main(args.argc, args.argv, &tagsistant_oper, NULL);
#endif

	fuse_opt_free_args(&args);

	/*
	 * unloading plugins
	 */
	plugin_unloader();

	/* free memory to better perfom memory leak profiling */
	freenull(tagsistant.repository);
	freenull(tagsistant.archive);
	freenull(tagsistant.tags);

	return res;
}

// vim:ts=4:autoindent:nocindent:syntax=c
