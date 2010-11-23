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

/*
 * since paths are issued without the tagsistant_id trailing
 * the filename (as in path/path/filename), while after being
 * created inside tagsistant, filenames get the additional ID
 * (as in path/path/3273.filename), an hash table to point
 * original paths to actual paths is required.
 *
 * the aliases hash table gets instantiated inside main() and
 * must be used with get_alias(), set_alias() and delete_alias().
 *
 */
void set_alias(const char *alias, const char *aliased) {
	dbg(LOG_INFO, "Setting alias %s for %s", aliased, alias);
	tagsistant_query("insert into aliases (alias, aliased) values (\"%s\", \"%s\")", NULL, NULL, alias, aliased);
}

gchar *get_alias(const char *alias) {
	gchar *aliased = NULL;
	tagsistant_query("select aliased from aliases where alias = \"%s\"", return_string, &aliased, alias);
	dbg(LOG_INFO, "Looking for an alias for %s, found %s", alias, aliased);
	return aliased;
}

void delete_alias(const char *alias) {
	dbg(LOG_INFO, "Deleting alias for %s", alias);
	tagsistant_query("delete from aliases where alias = \"%s\"", NULL, NULL, alias);
}

int __create_and_tag_object(querytree_t *qtree, int *tagsistant_errno, int force_create)
{
	tagsistant_id ID = 0;

	// 1. create the object on db or get its ID if exists
	//    if force_create is true, create a new object and fetch its ID
	//    if force_create is false, try to find an object with name and path matching
	//    and use its ID, otherwise create a new one
	if (!force_create) {
		tagsistant_query(
			"select object_id from objects where objectname = \"%s\" and path = \"%s\" limit 1",
			return_integer, &ID,
			qtree->object_path, qtree->archive_path);
	}

	if (force_create || (0 == ID)) {
		tagsistant_query(
			"insert into objects (objectname, path) values (\"%s\", \"-\")",
			NULL, NULL,
			qtree->object_path);
		tagsistant_query(
			"select max(object_id) from objects where objectname = \"%s\" and path = \"-\"",
			return_integer, &ID,
			qtree->object_path);

		// ID = tagsistant_last_insert_id(); // don't know why it does not work on MySQL
	}

	if (0 == ID) {
		dbg(LOG_ERR, "Object %s recorded as ID 0!", qtree->object_path);
		*tagsistant_errno = EIO;
		return -1;
	}

	// 2. adjust archive_path and full_archive_path with leading object_id
	g_free(qtree->archive_path);
	g_free(qtree->full_archive_path);

	qtree->archive_path = g_strdup_printf("%d.%s", ID, qtree->object_path);
	qtree->full_archive_path = g_strdup_printf("%s%s%d.%s", tagsistant.archive, G_DIR_SEPARATOR_S, ID, qtree->object_path);

	// 2.bis adjust object_path inside DB
	tagsistant_query(
		"update objects set path = \"%s\" where object_id = %d",
		NULL, NULL,
		qtree->archive_path, ID);

	// 3. set an alias for getattr
	set_alias(qtree->full_path, qtree->full_archive_path);

	// 5. tag the object
	traverse_querytree(qtree, sql_tag_object, ID);

	return ID;
}

#define create_and_tag_object(qtree, errno) __create_and_tag_object(qtree, errno, 0); dbg(LOG_INFO, "Tried creation of object %s", qtree->full_path)
#define force_create_and_tag_object(qtree, errno) __create_and_tag_object(qtree, errno, 1); dbg(LOG_INFO, "Forced creation of object %s", qtree->full_path)

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

	TAGSISTANT_START("/ GETATTR on %s", path);

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

#if 0
		// TODO :: we should check if path points to an object which is
		// tagged at least as one of the contained tags
		int exists = 0;
		ptree_or_node_t *or_ptr = qtree->tree;

// CHECK_EXISTANCE: while (NULL != or_ptr) {
		while (NULL != or_ptr) {
			ptree_and_node_t *and_ptr = or_ptr->and_set;

			while (NULL != and_ptr) {
				tagsistant_id tag_id = tagsistant_get_tag_id(and_ptr->tag);
				if (tag_id && tagsistant_object_is_tagged_as(qtree->object_id, tag_id)) {
					exists = 1;
					// break CHECK_EXISTANCE;
				}
				and_ptr = and_ptr->next;
			}

			or_ptr = or_ptr->next;
		}

		if (!exists) {
			res = -1;
			tagsistant_errno = ENOENT;
			goto GETATTR_EXIT;
		}
#endif

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

	//
	// a getattr may fail if issued on a path that
	// was just created by mknod or mkdir. so getattr
	// can query the aliases hash table to guess
	// if a path changed since its creation
	//
	// i.e.: 'cp filename tags/t1/=' will create a
	// file called N.filename in the archive/ directory
	// where N is the tagsistant_id assigned to the
	// file. its path will be:
	//
	//   archive/N.filename
	//   tags/t1/=/N.filename
	//
	if (-1 == res) {
		lstat_path = get_alias(path);
		if (NULL != lstat_path) {
			res = lstat(lstat_path, stbuf);
			tagsistant_errno = errno;
		}
	}

	// postprocess output
	if (QTREE_IS_TAGS(qtree)) {
		// dbg(LOG_INFO, "getattr: last tag is %s", qtree->last_tag);
		if (NULL == qtree->last_tag) {
			// ok
		} else if (g_strcmp0(qtree->last_tag, "+") == 0) {
			// path ends by '+'
			stbuf->st_ino += 1;
			stbuf->st_mode = S_IFDIR|S_IRUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH;
			stbuf->st_nlink = 1;
		} else if (g_strcmp0(qtree->last_tag, "=") == 0) {
			// path ends by '='
			stbuf->st_ino += 2;
			stbuf->st_mode = S_IFDIR|S_IRUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH;
			stbuf->st_nlink = 1;
		} else {
			tagsistant_id tag_id = get_exact_tag_id(qtree->last_tag);
			if (tag_id) {
				// each directory holds 3 inodes: itself/, itself/+, itself/=
				stbuf->st_ino = tag_id * 3;
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
		TAGSISTANT_STOP_ERROR("\\ GETATTR on %s (%s) {%s}: %d %d: %s", path, lstat_path, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ GETATTR on %s (%s): OK", path, query_type(qtree));
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

	TAGSISTANT_START("/ READLINK on %s", path);

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
		readlink_path = qtree->full_archive_path;
	} else if (QTREE_IS_STATS(qtree) || QTREE_IS_RELATIONS(qtree)) {
		res = -1;
		tagsistant_errno = EINVAL; /* symlinks exist in archive/ and tags/ only */
		goto READLINK_EXIT;
	}

	// do real readlink()
	res = readlink(readlink_path, buf, size);
	tagsistant_errno = errno;

	if (-1 == res) {
		readlink_path = get_alias(qtree->full_path);
		if (NULL != readlink_path) {
			res = readlink(readlink_path, buf, size);
			tagsistant_errno = errno;
		}
	}

	if (res > 0) buf[res] = '\0';

READLINK_EXIT:
	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("\\ READLINK on %s (%s) (%s): %d %d: %s", path, readlink_path, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ REALINK on %s (%s): OK", path, query_type(qtree));
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

#if TAGSISTANT_SQL_BACKEND == TAGSISTANT_SQLITE_BACKEND

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

	return ufs->filler(ufs->buf, argv[0], NULL, 0);
}

#else // DBI-DRIVEN-backend

/**
 * SQL callback. Add dir entries to libfuse buffer.
 *
 * \param filler_ptr struct use_filler_struct pointer (cast to void*)
 * \param result dbi_result pointer
 * \return 0 (always, see SQLite policy, may change in the future)
 */
static int add_entry_to_dir(void *filler_ptr, dbi_result result)
{
	struct use_filler_struct *ufs = (struct use_filler_struct *) filler_ptr;
	const char *dir = dbi_result_get_string_idx(result, 1);

	if (dir == NULL || strlen(dir) == 0)
		return 0;

	/* check if this tag has been already listed inside the path */
	ptree_or_node_t *ptx = ufs->qtree->tree;
	while (NULL != ptx->next) ptx = ptx->next; // last OR section

	ptree_and_node_t *and_t = ptx->and_set;
	while (NULL != and_t) {
		if (g_strcmp0(and_t->tag, dir) == 0) {
			return 0;
		}
		and_t = and_t->next;
	}

	return ufs->filler(ufs->buf, dir, NULL, 0);
}

#endif

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
	gchar *readdir_path = NULL;

	(void) fi;
	(void) offset;

	TAGSISTANT_START("/ READDIR on %s", path);

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
		DIR *dp = opendir(qtree->full_archive_path);
		if (NULL == dp) {

			readdir_path = get_alias(path);
			if (NULL != readdir_path) {
				dp = opendir(readdir_path);
				tagsistant_errno = errno;

				if (NULL == dp) {
					tagsistant_errno = errno;
					goto READDIR_EXIT;
				}
			}
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
		filler(buf, ".", NULL, 0);
		filler(buf, "..", NULL, 0);
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
		filler(buf, ".", NULL, 0);
		filler(buf, "..", NULL, 0);
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
			// add operators if path is not "/tags", to avoid
			// "/tags/+" and "/tags/="
			if (g_strcmp0(path, "/tags") != 0) {
				filler(buf, "+", NULL, 0);
				filler(buf, "=", NULL, 0);
			}

			/*
		 	* if path does not terminate by =,
		 	* directory should be filled with tagsdir registered tags
		 	*/
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
		filler(buf, ".", NULL, 0);
		filler(buf, "..", NULL, 0);
		// fill with available statistics
	} else if (QTREE_IS_RELATIONS(qtree)) {
		filler(buf, ".", NULL, 0);
		filler(buf, "..", NULL, 0);
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

READDIR_EXIT:
	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("\\ READDIR on %s (%s): %d %d: %s", path, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ READDIR on %s (%s): OK", path, query_type(qtree));
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

	TAGSISTANT_START("/ MKNOD on %s [mode: %u rdev: %u]", path, mode, (unsigned int) rdev);

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
			res = force_create_and_tag_object(qtree, &tagsistant_errno);
			if (-1 == res) goto MKNOD_EXIT;
		}

		dbg(LOG_INFO, "NEW object on disk: mknod(%s)", qtree->full_archive_path);
		res = mknod(qtree->full_archive_path, mode, rdev);
		tagsistant_errno = errno;
	} else
	
	// -- stats --
	// -- relations --
	{
		res = -1;
		tagsistant_errno = EROFS;
	}

MKNOD_EXIT:
	stop_labeled_time_profile("mknod");

	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("\\ MKNOD on %s (%s) (%s): %d %d: %s", path, qtree->full_archive_path, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ MKNOD on %s (%s): OK", path, query_type(qtree));
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

	TAGSISTANT_START("/ MKDIR on %s [mode: %d]", path, mode);

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
			res = create_and_tag_object(qtree, &tagsistant_errno);

			if (-1 == res) goto MKDIR_EXIT;
		}

		// do a real mkdir
		res = mkdir(qtree->full_archive_path, mode);
		tagsistant_errno = errno;
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
	
MKDIR_EXIT:
	stop_labeled_time_profile("mkdir");

	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("\\ MKDIR on %s (%s): %d %d: %s", path, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ MKDIR on %s (%s): OK", path, query_type(qtree));
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
	gchar *unlink_path = NULL;

	TAGSISTANT_START("/ UNLINK on %s", path);

	// build querytree
	querytree_t *qtree = build_querytree(path, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
	} else

	// -- objects on disk --
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		if (QTREE_IS_TAGGABLE(qtree) && QTREE_IS_TAGS(qtree)) {

			// if object is pointed by a tags/ query, then untag it
			// from the tags included in the query path...
			traverse_querytree(qtree, sql_untag_object, qtree->object_id);

			// ...then check if it's tagged elsewhere...
			// ...if still tagged, then avoid real unlink(): the object must survive!
			if (tagsistant_object_is_tagged(qtree->object_id))
				goto UNLINK_EXIT;

			// otherwise just delete if from the objects table and go on.
		} else if (QTREE_IS_ARCHIVE(qtree)) {
			// if the query path points to archive/, it's clear that the
			// object is required to disappear from the filesystem, so
			// must be erased from the tagging table.
			tagsistant_full_untag_object(qtree->object_id);
		}

		// wipe the object from objects table...
		tagsistant_query("delete from objects where object_id = %d", NULL, NULL, qtree->object_id);

		// ... and do the real unlink()
		unlink_path = qtree->full_archive_path;
		res = unlink(unlink_path);
		tagsistant_errno = errno;

		if (-1 == res) {
			unlink_path = get_alias(qtree->full_path);
			if (NULL != unlink_path) {
				res = unlink(unlink_path);
				tagsistant_errno = errno;
			}
		}
	} else

	// -- tags --
	// -- stats --
	// -- relations --
	{
		res = -1;
		tagsistant_errno = EROFS;
	}

UNLINK_EXIT:
	stop_labeled_time_profile("unlink");

	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("\\ UNLINK on %s (%s) (%s): %d %d: %s", path, unlink_path, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ UNLINK on %s (%s): OK", path, query_type(qtree));
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
	gchar *rmdir_path = NULL;

	TAGSISTANT_START("/ RMDIR on %s", path);

	querytree_t *qtree = build_querytree(path, FALSE);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
	} else

	// -- tags --
	// -- archive
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		if (QTREE_IS_TAGGABLE(qtree)) {
			// remove all the tags associated to the object
			traverse_querytree(qtree, sql_untag_object, qtree->object_id);
		} else {

			// do a real mkdir
			rmdir_path = qtree->full_archive_path;
			res = rmdir(rmdir_path);
			tagsistant_errno = errno;
			if (-1 == res) {
				rmdir_path = get_alias(path);
				if (NULL != rmdir_path) {
					res = rmdir(rmdir_path);
					tagsistant_errno = errno;
				}
			}
		}
	} else

	// -- tags but incomplete (means: delete a tag) --
	if (QTREE_IS_TAGS(qtree)) {
		sql_delete_tag(qtree->last_tag);	
	} else

	// -- relations --
	if (QTREE_IS_RELATIONS(qtree)) {
		// rmdir can be used only on third level
		// since first level is all available tags
		// and second level is all available relations
		if (qtree->second_tag) {
			// create a new relation between two tags	
			sql_create_tag(qtree->second_tag);
			int tag1_id = get_exact_tag_id(qtree->first_tag);
			int tag2_id = get_exact_tag_id(qtree->second_tag);
			if (tag1_id && tag2_id && IS_VALID_RELATION(qtree->relation)) {
				tagsistant_query(
					"delete from relations where tag1_id = \"%d\" and tag2_id = \"%d\" and relation = \"%s\"",
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

	stop_labeled_time_profile("rmdir");

	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("\\ RMDIR on %s (%s): %d %d: %s", path, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ RMDIR on %s (%s): OK", path, query_type(qtree));
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
	gchar *rename_path = NULL;
	querytree_t *from_qtree = NULL, *to_qtree = NULL;

	TAGSISTANT_START("/ RENAME %s as %s", from, to);

	from_qtree = build_querytree(from, FALSE);
	if (NULL == from_qtree) {
		tagsistant_errno = ENOMEM;
		goto RENAME_EXIT;
	}

	to_qtree = build_querytree(to, FALSE);
	if (NULL == to_qtree) {
		tagsistant_errno = ENOMEM;
		goto RENAME_EXIT;
	}

	// -- malformed --
	if (QTREE_IS_MALFORMED(from_qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
		goto RENAME_EXIT;
	}

	if (QTREE_IS_MALFORMED(to_qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
		goto RENAME_EXIT;
	}

	// -- can't rename objects of different type or not both complete
	if (!QTREES_ARE_SIMILAR(from_qtree, to_qtree)) {
		res = -1;
		tagsistant_errno = EINVAL;
		goto RENAME_EXIT;
	}

	// -- can't rename anything from or into /stats or /relations
	if (QTREE_IS_STATS(to_qtree) || QTREE_IS_STATS(from_qtree) || QTREE_IS_RELATIONS(to_qtree) || QTREE_IS_RELATIONS(from_qtree)) {
		res = -1;
		tagsistant_errno = EINVAL;
		goto RENAME_EXIT;
	}

	// -- object on disk (/archive and complete /tags) --
	if (QTREE_POINTS_TO_OBJECT(from_qtree)) {
		if (QTREE_IS_TAGGABLE(from_qtree)) {
			// 0. strip trailing number (i.e. 283.filename -> filename)
			tagsistant_qtree_renumber(to_qtree, from_qtree->object_id);

			// 1. rename the object
			tagsistant_query("update objects set objectname = \"%s\" where object_id = %d", NULL, NULL, to_qtree->object_path, from_qtree->object_id);
			tagsistant_query("update objects set path = \"%s\" where object_id = %d", NULL, NULL, to_qtree->full_archive_path, from_qtree->object_id);

			// 2. deletes all the tagging between "from" file and all AND nodes in "from" path
			traverse_querytree(from_qtree, sql_untag_object, from_qtree->object_id);

			// 3. adds all the tags from "to" path
			traverse_querytree(to_qtree, sql_tag_object, from_qtree->object_id);
		}

		rename_path = from_qtree->full_archive_path;
		res = rename(rename_path, to_qtree->full_archive_path);
		tagsistant_errno = errno;

		if (-1 == res) {
			rename_path = get_alias(rename_path);
			if (NULL != rename_path) {
				res = rename(rename_path, to_qtree->full_archive_path);
				tagsistant_errno = errno;

				if (-1 == res) goto RENAME_EXIT;
			}
		}

	} else if (QTREE_IS_ROOT(from_qtree)) {
		res = -1;
		tagsistant_errno = EPERM;
	} else if (QTREE_IS_TAGS(from_qtree)) {
		if (QTREE_IS_COMPLETE(from_qtree)) {
			res = -1;
			tagsistant_errno = EPERM;
			goto RENAME_EXIT;
		}

		tagsistant_query("update tags set tagname = \"%s\" where tagname = \"%s\"", NULL, NULL, to_qtree->last_tag, from_qtree->last_tag);
	}

RENAME_EXIT:
	stop_labeled_time_profile("rename");

	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("\\ RENAME %s (%s) to %s (%s): %d %d: %s", from, query_type(from_qtree), to, query_type(to_qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ RENAME %s (%s) to %s (%s): OK", from, query_type(from_qtree), to, query_type(to_qtree));
	}

	destroy_querytree(from_qtree);
	destroy_querytree(to_qtree);
	return (res == -1) ? -tagsistant_errno : 0;
}

/**
 * symlink equivalent
 *
 * \param from existing file name
 * \param to new file name
 * \return 0 on success, -errno otherwise
 *
 * TODO :: Huston, we have a problem with Nautilus:
 *
 * TS> / SYMLINK /home/tx0/tags/tags/t1/=/1.clutter_renamed to /tags/t1/=/Link to 1.clutter_renamed (@tagsistant.c:973)
 * TS> | SQL: [start transaction] @sql.c:240 (@sql.c:302)
 * TS> | Building querytree for /tags/t1/=/1.clutter_renamed (@path_resolution.c:254)
 * TS> | Building querytree for /tags/t1/=/Link to 1.clutter_renamed (@path_resolution.c:254)
 * TS> | Retagging /home/tx0/tags/tags/t1/=/1.clutter_renamed as internal to /home/tx0/tags (@tagsistant.c:1011)
 * TS> | Traversing querytree... (@tagsistant.c:1012)
 * TS> | SQL: [insert into tags(tagname) values("t1");] @sql.c:456 (@sql.c:302)
 * TS> | SQL Error: 1062: Duplicate entry 't1' for key 'tagname'. (@sql.c:326)
 * TS> | SQL: [select tag_id from tags where tagname = "t1" limit 1] @sql.c:440 (@sql.c:302)
 * TS> | Returning integer: 1 (@sql.c:388)
 * TS> | Tagging object 1 as t1 (1) (@sql.c:460)
 * TS> | SQL: [insert into tagging(tag_id, object_id) values("1", "1");] @sql.c:462 (@sql.c:302)
 * TS> | SQL Error: 1062: Duplicate entry '1-1' for key 'Tagging_key'. (@sql.c:326)
 * TS> | Applying sql_tag_object(t1,...) (@tagsistant.c:1012)
 * TS> | SQL: [commit] @sql.c:248 (@sql.c:302)
 * TS> \ SYMLINK from /home/tx0/tags/tags/t1/=/1.clutter_renamed to /tags/t1/=/Link to 1.clutter_renamed (QTYPE_TAGS): OK (@tagsistant.c:1048)
 *
 * May be we should reconsider the idea of retagging internal
 * paths while symlinking...
 */
static int tagsistant_symlink(const char *from, const char *to)
{
	int tagsistant_errno = 0, res = 0;

	TAGSISTANT_START("/ SYMLINK %s to %s", from, to);

	/*
	 * guess if query points to an external or internal object
	 */
	char *_from = (char *) from;
	if (!TAGSISTANT_PATH_IS_EXTERNAL(from)) {
		_from = (char * ) from + strlen(tagsistant.mountpoint);
		// dbg(LOG_INFO, "%s is internal to %s, trimmed to %s", from, tagsistant.mountpoint, _from);
	}

	querytree_t *from_qtree = build_querytree(_from, 0);
	querytree_t *to_qtree = build_querytree(to, 0);

	from_qtree->is_external = (from == _from) ? 1 : 0;

	if (from_qtree->object_path) qtree_copy_object_path(from_qtree, to_qtree);

	// -- malformed --
	if (QTREE_IS_MALFORMED(to_qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
	} else

	// -- object on disk --
	if (QTREE_POINTS_TO_OBJECT(to_qtree) || (QTREE_IS_TAGS(to_qtree) && QTREE_IS_COMPLETE(to_qtree))) {

		// if object_path is null, borrow it from original path
		if (strlen(to_qtree->object_path) == 0) {
			dbg(LOG_INFO, "Getting object path from %s", from);
			qtree_set_object_path(to_qtree, g_strdup(g_path_get_basename(from)));
		}

		// if qtree is internal, just re-tag it, taking the tags from to_qtree but
		// the ID from from_qtree
		if (QTREE_IS_INTERNAL(from_qtree) && from_qtree->object_id) {
			dbg(LOG_INFO, "Retagging %s as internal to %s", from, tagsistant.mountpoint);
			traverse_querytree(to_qtree, sql_tag_object, from_qtree->object_id);
			goto SYMLINK_EXIT;
		} else

		// if qtree is taggable, do it
		if (QTREE_IS_TAGGABLE(to_qtree)) {
			dbg(LOG_INFO, "SYMLINK : Creating %s", to_qtree->object_path);
			res = force_create_and_tag_object(to_qtree, &tagsistant_errno);
			if (-1 == res) goto SYMLINK_EXIT;
		} else

		// nothing to do about tags
		{
			dbg(LOG_INFO, "%s is not taggable!", to_qtree->full_path); // ??? why ??? should be taggable!!
		}

		// do the real symlink on disk
		dbg(LOG_INFO, "Symlinking %s to %s", from, to_qtree->object_path);
		res = symlink(from, to_qtree->full_archive_path);
		tagsistant_errno = errno;
	} else

	// -- tags (uncomplete) --
	// -- stats --
	// -- relations --
	{
		res = -1;
		tagsistant_errno = EINVAL; /* can't symlink outside of tags/ and archive/ */
	}

SYMLINK_EXIT:
	stop_labeled_time_profile("symlink");

	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("\\ SYMLINK from %s to %s (%s) (%s): %d %d: %s", from, to, to_qtree->full_archive_path, query_type(to_qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ SYMLINK from %s to %s (%s): OK", from, to, query_type(to_qtree));
	}

	destroy_querytree(from_qtree);
	destroy_querytree(to_qtree);

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

	TAGSISTANT_START("/ CHMOD on %s [mode: %d]", path, mode);

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
		TAGSISTANT_STOP_ERROR("\\ CHMOD %s (%s) as %d: %d %d: %s", qtree->full_archive_path, query_type(qtree), mode, res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ CHMOD %s (%s), %d: OK", path, query_type(qtree), mode);
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

	TAGSISTANT_START("/ CHOWN on %s [uid: %d gid: %d]", path, uid, gid);

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
		TAGSISTANT_STOP_ERROR("\\ CHMOD %s to %d,%d (%s): %d %d: %s", qtree->full_archive_path, uid, gid, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ CHMOD %s, %d, %d (%s): OK", path, uid, gid, query_type(qtree));
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

	TAGSISTANT_START("/ TRUNCATE on %s [size: %lu]", path, (long unsigned int) size);

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
		TAGSISTANT_STOP_ERROR("\\ TRUNCATE %s at %llu (%s): %d %d: %s", qtree->full_archive_path, (unsigned long long) size, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ TRUNCATE %s, %llu (%s): OK", path, (unsigned long long) size, query_type(qtree));
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

	TAGSISTANT_START("/ UTIME on %s", path);

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
		TAGSISTANT_STOP_ERROR("\\ UTIME %s (%s): %d %d: %s", utime_path, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ UTIME %s (%s): OK", path, query_type(qtree));
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
int internal_open(querytree_t *qtree, int flags, int *_errno)
{
	// first check on plain path
	int res = open(qtree->full_archive_path, flags);

	// later look for an alias and try it
	if (-1 == res) {
		gchar *alias = get_alias(qtree->full_path);
		if (NULL != alias) {
			res = open(alias, flags);
		}
	}

	*_errno = errno;

#if VERBOSE_DEBUG
	dbg(LOG_INFO, "internal_open(%s): %d", filepath, res);
	if (flags&O_CREAT) dbg(LOG_INFO, "...O_CREAT");
	if (flags&O_WRONLY) dbg(LOG_INFO, "...O_WRONLY");
	if (flags&O_TRUNC) dbg(LOG_INFO, "...O_TRUNC");
	if (flags&O_LARGEFILE) dbg(LOG_INFO, "...O_LARGEFILE");
#endif

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

	TAGSISTANT_START("/ OPEN on %s", path);

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
		res = internal_open(qtree, fi->flags|O_RDONLY, &tagsistant_errno);
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
		TAGSISTANT_STOP_ERROR("\\ OPEN on %s (%s) (%s): %d %d: %s", path, open_path, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ OPEN on %s (%s): OK", path, query_type(qtree));
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

	TAGSISTANT_START("/ READ on %s [size: %lu offset: %lu]", path, (long unsigned int) size, (long unsigned int) offset);

	querytree_t *qtree = build_querytree(path, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
	} else

	// -- object on disk --
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		int fd = internal_open(qtree, fi->flags|O_RDONLY, &tagsistant_errno); 
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
		TAGSISTANT_STOP_ERROR("\\ READ %s (%s) (%s): %d %d: %s", path, qtree->full_archive_path, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ READ %s (%s): OK", path, query_type(qtree));
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

	TAGSISTANT_START("/ WRITE on %s [size: %d offset: %lu]", path, size, (long unsigned int) offset);

	querytree_t *qtree = build_querytree(path, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
	} else

	// -- object on disk --
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		int fd = internal_open(qtree, fi->flags|O_WRONLY, &tagsistant_errno); 
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
		TAGSISTANT_STOP_ERROR("\\ WRITE %s (%s) (%s): %d %d: %s", path, qtree->full_archive_path, query_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ WRITE %s (%s): OK", path, query_type(qtree));
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

	TAGSISTANT_START("/ STATVFS on %s", path);
	
	res = statvfs(tagsistant.repository, stbuf);
	tagsistant_errno = errno;

	stop_labeled_time_profile("statvfs");

	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("\\ STATVFS on %s: %d %d: %s", path, res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ STATVFS on %s: OK", path);
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

	TAGSISTANT_START("/ STATVFS on %s", path);

	res = statfs(tagsistant.repository, stbuf);
	tagsistant_errno = errno;

	stop_labeled_time_profile("statfs");

	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("\\ STATFS on %s: %d %d: %s", path, res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ STATFS on %s: OK", path);
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

	TAGSISTANT_START("/ ACCESS on %s [mode: %u]", path, mode);
	struct stat st;
	int res = tagsistant_getattr(path, &st);

	if (res == 0) {
		TAGSISTANT_STOP_OK("\\ ACCESS on %s: OK", path);
		return 0;
	}

	TAGSISTANT_STOP_ERROR("\\ ACCESS on %s: -1 %d: %s", path, EACCES, strerror(EACCES));
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
    .mknod	= tagsistant_mknod,
    .mkdir	= tagsistant_mkdir,
    .symlink	= tagsistant_symlink,
    .unlink	= tagsistant_unlink,
    .rmdir	= tagsistant_rmdir,
    .rename	= tagsistant_rename,
    .link	= tagsistant_link,
    .chmod	= tagsistant_chmod,
    .chown	= tagsistant_chown,
    .truncate	= tagsistant_truncate,
    .utime	= tagsistant_utime,
    .open	= tagsistant_open,
    .read	= tagsistant_read,
    .write	= tagsistant_write,
#if FUSE_USE_VERSION >= 25
    .statfs	= tagsistant_statvfs,
#else
    .statfs	= tagsistant_statfs,
#endif
    .release	= tagsistant_release,
    .fsync	= tagsistant_fsync,
#ifdef HAVE_SETXATTR
    .setxattr	= tagsistant_setxattr,
    .getxattr	= tagsistant_getxattr,
    .listxattr	= tagsistant_listxattr,
    .removexattr= tagsistant_removexattr,
#endif
    .access	= tagsistant_access,
    .init	= tagsistant_init,
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
	TAGSISTANT_OPT("--db=%s",				dboptions,		0),
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
		" Usage: \n"
		" \n"
		"  %s [OPTIONS] [--repository=<PATH>] [--db=<OPTIONS>] /mountpoint\n"
		"\n"
		"  -q  be quiet\n"
		"  -r  mount readonly\n"
		"  -v  verbose syslogging\n"
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
	// fuse_opt_add_arg(&args, "-oallow_other");

#ifdef MACOSX
	fuse_opt_add_arg(&args, "-odefer_permissions");
	gchar *volname = g_strdup_printf("-ovolname=%s", tagsistant.mountpoint);
	fuse_opt_add_arg(&args, volname);
	freenull(volname);
#else
	/* fuse_opt_add_arg(&args, "-odefault_permissions"); */
#endif

#if 0

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
#endif

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
	if (tagsistant.dboptions) {
		if (!tagsistant.quiet)
			fprintf(stderr, " *** connecting to %s\n", tagsistant.dboptions);
	}
	if (tagsistant.repository) {
		if (!tagsistant.quiet)
			fprintf(stderr, " *** saving repository in %s\n", tagsistant.repository);
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

	/* initialize db connection */
	tagsistant_db_connection();

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
	freenull(tagsistant.dboptions);
	freenull(tagsistant.repository);
	freenull(tagsistant.archive);
	freenull(tagsistant.tags);

	return res;
}

// vim:ts=4:autoindent:nocindent:syntax=c
