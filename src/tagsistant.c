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

#ifndef MACOSX
#include <mcheck.h>
#endif

FILE *debugfd = NULL;

#ifdef DEBUG_TO_LOGFILE
void open_debug_file()
{
	char debug_file[1024];
	sprintf(debug_file, "/tmp/tagsistant.debug.%d", getpid());
	debugfd = fopen(debug_file, "w");
	if (debugfd != NULL) {
		dbg(LOG_INFO, "Logfile %s open!", debug_file);
	} else {
		dbg(LOG_ERR, "Can't open logfile %s: %s!", debug_file, strerror(errno));
	}
}
#endif

#ifdef DEBUG_STRDUP
char *real_strdup(const char *orig, char *file, int line)
{
	if (orig == NULL) return NULL;
	/* dbg(LOG_INFO, "strdup(%s) @%s:%d", orig, file, line); */
	char *res = calloc(sizeof(char), strlen(orig) + 1);
	memcpy(res, orig, strlen(orig));
	if (debugfd != NULL) fprintf(debugfd, "0x%.8x: strdup(%s) @%s:%d\n", (unsigned int) res, orig, file, line);
	return res;
}
#endif

int debug = 0;
int log_enabled = 0;

/* defines command line options for tagsistant mount tool */
/* static */ struct tagsistant tagsistant;

/**
 * given a full path, returns the filename, the filepath (relative
 * to archive directory) and the tagname
 *
 * \param path original path to analize
 * \param filename pointer to path filename
 * \param filepath pointer to path other part which is not filename
 * \param tagname pointer to path last tag
 * \return 1 if successfull, 0 otherwise
 * \todo return value is not conditional, this function always returns 1!
 */
int get_filename_and_tagname(const char *path, char **filename, char **filepath, char **tagname)
{
	*filename = get_tag_name(path);
	*filepath = get_file_path(*filename);
	char *path_dup = strdup(path);
	char *ri = rindex(path_dup, '/');
	*ri = '\0';
	ri = rindex(path_dup, '/');
	if (ri) {
		ri++;
		*tagname = strdup(ri);
	}
	freenull(path_dup);
	return 1;
}

#ifdef _DEBUG_SYSLOG
/**
 * initialize syslog stream
 */
void init_syslog()
{
	if (log_enabled)
		return;

	openlog("tagsistant", LOG_PID, LOG_DAEMON);
	log_enabled = 1;
}
#endif


/**
 * Check if a file is associated with a given tag.
 *
 * \param filename the filename to be checked (no path)
 * \param tagname the name of the tag to be searched on filename
 * \return 1 if file filename is tagged with tag tagname, 0 otherwise.
 */
int is_tagged(char *filename, char *tagname)
{
	int exists = 0;
	tagsistant_query(
		"select filename from tagged where filename = \"%s\" and tagname = \"%s\";",
		report_if_exists, &exists, filename, tagname);
	return exists;
}

/**
 * Add tag tagname to file.
 *
 * \param filename the file to be tagged (no path)
 * \param tagname the tag to be added.
 * \return 1 if successful, 0 otherwise
 * \todo check return values of this function
 */
int tag_file(const char *filename, char *tagname)
{
	/* get pure filename */
	char *purefile = get_tag_name(filename);

	/* check if file is already tagged */
	if (is_tagged(purefile, tagname)) {
		freenull(purefile);
		return 1;
	}

	/* add tag to file */
	sql_tag_file(tagname, purefile);

	/* check if tag is already in db */
	char exists = sql_tag_exists(tagname);
	
	if (exists) {
		dbg(LOG_INFO, "Tag %s already exists", tagname);
		freenull(purefile);
		return 1;
	}

	/* add tag to taglist */
	sql_create_tag(tagname);
	dbg(LOG_INFO, "Tag %s created", tagname);

	freenull(purefile);
	return 1;
}

/**
 * remove tag tagname from file filename
 */
int untag_file(char *filename, char *tagname)
{
	sql_untag_file(tagname, filename);
	
	return 1;
}

#ifdef MACOSX
ssize_t getline(char **lineptr, size_t *n, FILE *stream)
{
	if (*lineptr == NULL)
		*lineptr = calloc(sizeof(char), *n + 1);

	if (*lineptr == NULL)
		return 0;

	if (fgets(*lineptr, *n, stream) == NULL)
		*n = 0;
	else
		*n = strlen(*lineptr);

	return *n;
}
#endif

/******************\
 * PLUGIN SUPPORT *
\******************/

/**
 * the head of plugin list
 */
tagsistant_plugin_t *plugins = NULL;

/**
 * guess the MIME type of passed filename
 *
 * \param filename file to be processed (just the name, will be looked up in /archive)
 * \return the string rappresenting MIME type (like "audio/mpeg"); the string is dynamically
 *   allocated and need to be freenull()ed by outside code
 */
char *get_file_mimetype(const char *filename)
{
	char *type = NULL;

	/* get file extension */
	char *ext = rindex(filename, '.');
	if (ext == NULL) {
		return NULL;
	}
	ext++;
	char *ext_space = malloc(strlen(ext)+2);
	if (ext_space == NULL) {
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return NULL;
	}

	strcpy(ext_space, ext);
	strcat(ext_space, " "); /* trailing space is used later in matching */

	/* open /etc/mime.types */
	FILE *f = fopen("/etc/mime.types", "r");
	if (f == NULL) {
		dbg(LOG_ERR, "Can't open /etc/mime.types");
		return type;
	}

	/* parse /etc/mime.types */
	char *line = NULL;
	size_t len = 0;
	while (getline(&line, &len, f) != -1) {
		/* remove line break */
		if (index(line, '\n') != NULL)
			*(index(line, '\n')) = '\0';

		/* get the mimetype and the extention list */
		char *ext_list = index(line, '\t');
		if (ext_list != NULL) {
			while (*ext_list == '\t') {
				*ext_list = '\0';
				ext_list++;
			}

			while (*ext_list != '\0') {
				if ((strstr(ext_list, ext) == ext_list) || (strstr(ext_list, ext_space) == ext_list)) {
					type = strdup(line);
					dbg(LOG_INFO, "File %s is %s", filename, type);
					freenull(line);
					goto BREAK_MIME_SEARCH;
				}

				/* advance to next extension */
				while ((*ext_list != ' ') && (*ext_list != '\0')) ext_list++;
				if (*ext_list == ' ') ext_list++;
			}
		}

		if (line != NULL) freenull(line);
		line = NULL;
	}

BREAK_MIME_SEARCH:
	freenull(ext_space);
	fclose(f);
	return type;
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

	dbg(LOG_INFO, "Processing file %s", filename);

	char *mime_type = get_file_mimetype(filename);

	if (mime_type == NULL) {
		dbg(LOG_ERR, "process() wasn't able to guess mime type for %s", filename);
		return 0;
	}

	char *mime_generic = strdup(mime_type);
	char *slash = index(mime_generic, '/');
	slash++;
	*slash = '*';
	slash++;
	*slash = '\0';

	/* apply plugins in order */
	tagsistant_plugin_t *plugin = plugins;
	while (plugin != NULL) {
		if (
			(strcmp(plugin->mime_type, mime_type) == 0) ||
			(strcmp(plugin->mime_type, mime_generic) == 0) ||
			(strcmp(plugin->mime_type, "*/*") == 0)
		) {
			/* call plugin processor */
			process_res = (plugin->processor)(filename);

			/* report about processing */
			switch (process_res) {
				case TP_ERROR:
					dbg(LOG_ERR, "Plugin %s was supposed to apply to %s, but failed!", plugin->filename, filename);
					break;
				case TP_OK:
					dbg(LOG_INFO, "Plugin %s tagged %s", plugin->filename, filename);
					break;
				case TP_STOP:
					dbg(LOG_INFO, "Plugin %s stopped chain on %s", plugin->filename, filename);
					goto STOP_CHAIN_TAGGING;
					break;
				case TP_NULL:
					dbg(LOG_INFO, "Plugin %s did not tagged %s", plugin->filename, filename);
					break;
				default:
					dbg(LOG_ERR, "Plugin %s returned unknown result %d", plugin->filename, process_res);
					break;
			}
		}
		plugin = plugin->next;
	}

STOP_CHAIN_TAGGING:

	freenull(mime_type);
	freenull(mime_generic);

	dbg(LOG_INFO, "Processing of %s ended.", filename);
	return res;
}


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
	if ((strcmp(last, "AND") == 0) || (strcmp(last, "OR") == 0)) {

#if VERBOSE_DEBUG
	 	dbg(LOG_INFO, "GETATTR on AND/OR logical operator!");
#endif

	 	lstat(tagsistant.archive, stbuf);	

		/* getting directory inode from filesystem */
		ino_t inode = 0;

		if (last2 != NULL)
			inode = get_exact_tag_id(last2);

		stbuf->st_ino = inode * 3; /* each directory holds 3 inodes: itself/, itself/AND/, itself/OR/ */

		/* 
		 * using tagsistant.archive in lstat() returns its inode number
		 * for all AND/OR ops. that is a problem while traversing
		 * a dir tree with write approach, like in a rm -rf <dir>.
		 * to avoid such a problem we add 1 to inode number to
		 * differentiate it
		 */
		stbuf->st_ino++;

		/* AND and OR can't be the same inode */
		if (strcmp(last, "OR") == 0) stbuf->st_ino++;
		
		/* logical operators can't be written by anyone */
		stbuf->st_mode = S_IFDIR|S_IRUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH;

		/* not sure of that! */
		/* stbuf->st_size = 0; */

		stbuf->st_nlink = 1;

		freenull(dup);
		return 0;
	}

	/* last token in path is a file or a tag? */
	if ((last2 == NULL) || (strcmp(last2, "AND") == 0) || (strcmp(last2, "OR") == 0)) {
		/* is a dir-tag */
		/* last is the name of the tag */
#if VERBOSE_DEBUG
		dbg(LOG_INFO, "GETATTR on tag: '%s'", last);
#endif

		int exists = sql_tag_exists(last);

		if (exists) {
			res = lstat(tagsistant.archive, stbuf);
			tagsistant_errno = errno;

			/* getting directory inode from filesystem */
			ino_t inode = 0;
			tagsistant_query("select id from tags where tagname = \"%s\";", return_integer, &inode, last);
			stbuf->st_ino = inode * 3; /* each directory holds 3 inodes: itself/, itself/AND/, itself/OR/ */
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
			freenull(dup);
			return -ENOENT;
		}
		ptree_or_node_t *ptx = pt;

		/* check in all tags */
		while (ptx != NULL) {
			ptree_and_node_t *andpt = ptx->and_set;
			while (andpt != NULL) {
				ptree_and_node_t *related = andpt;
				while (related != NULL) {
					if (is_tagged(last, related->tag)) {
						char *filepath = get_file_path(last);
						res = lstat(filepath, stbuf);
						tagsistant_errno = (res == -1) ? errno : 0;
#if VERBOSE_DEBUG
						dbg(LOG_INFO, "lstat('%s/%s'): %d (%s)", related->tag, last, res, strerror(tagsistant_errno));
#endif
						freenull(filepath);
						destroy_querytree(pt);
						goto GETATTR_EXIT;
					}
					related = related->related;
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

	freenull(dup);
	/* last and last2 are not malloc()ated! freenull(last) and freenull(last2) are errors! */

	stop_labeled_time_profile("getattr");
	if ( res == -1 ) {
		dbg(LOG_ERR, "GETATTR on %s: %d %d: %s", path, res, tagsistant_errno, strerror(tagsistant_errno));
		return -tagsistant_errno;
	} else {
		dbg(LOG_INFO, "GETATTR on %s: OK", path);
		return 0;
	}
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
	char *filename = get_tag_name(path);
	char *filepath = get_file_path(filename);
	freenull(filename);

	dbg(LOG_INFO, "READLINK on %s", filepath);

	int res = readlink(filepath, buf, size);
	int tagsistant_errno = errno;

	freenull(filepath);
	return (res == -1) ? -tagsistant_errno : 0;
}

/**
 * used by add_entry_to_dir() SQL callback to perform readdir() ops.
 */
struct use_filler_struct {
	fuse_fill_dir_t filler;	/**< libfuse filler hook to return dir entries */
	void *buf;				/**< libfuse buffer to hold readdir results */
	const char *path;		/**< the path that generates the query */
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
	char *path_duplicate = strdup(ufs->path);
	if (path_duplicate == NULL) {
		dbg(LOG_ERR, "Error duplicating path @%s:%d", __FILE__, __LINE__);
		return 0;
	}
	char *last_subquery = path_duplicate;
	while (strstr(last_subquery, "/OR") != NULL) {
		last_subquery = strstr(last_subquery, "/OR") + strlen("/OR");
	}
	char *tag_to_check = calloc(sizeof(char), strlen(argv[0]) + 3);
	if (tag_to_check == NULL) {
		freenull(path_duplicate);
		dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
		return 0;
	}
	strcat(tag_to_check, "/");
	strcat(tag_to_check, argv[0]);
	strcat(tag_to_check, "/");
	if (strstr(last_subquery, tag_to_check) != NULL) {
		freenull(tag_to_check);
		freenull(path_duplicate);
		return 0;
	}
	freenull(tag_to_check);
	freenull(path_duplicate);

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
	(void) fi;
	(void) offset;

	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

	dbg(LOG_INFO, "READDIR on %s", path);

	char *tagname = get_tag_name(path);
	if (
		strlen(tagname) &&
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
		
		char *pathcopy = strdup(path);
	
		ptree_or_node_t *pt = build_querytree(pathcopy);
		freenull(pathcopy);
		if (pt == NULL)
			return -EBADF;
	
		file_handle_t *fh = build_filetree(pt, path);
		if (fh == NULL) {
			destroy_querytree(pt);
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
	
		destroy_querytree(pt);
		destroy_filetree(fh_save);
		freenull(tagname);

	} else {

		dbg(LOG_INFO, "%s terminate with and operator or is root", path);

		/*
		 * if path does terminate with a logical operator
		 * directory should be filled with tagsdir registered tags
		 */
		struct use_filler_struct *ufs = calloc(sizeof(struct use_filler_struct), 1);
		if (ufs == NULL) {
			dbg(LOG_ERR, "Error allocating memory @%s:%d", __FILE__, __LINE__);
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
	freenull(tagname);

	return 0;
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

	init_time_profile();
	start_time_profile();

	char *filename = NULL, *fullfilename = NULL, *tagname = NULL;
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

			/* do stacked plugin operations */
			process(filename);

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

	stop_labeled_time_profile("mknod");
	dbg(LOG_INFO, "mknod(%s): %d %s", fullfilename, res, strerror(tagsistant_errno));

	freenull(filename);
	freenull(fullfilename);
	freenull(tagname);

	return (res == -1) ? -tagsistant_errno: 0;
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

	char *tagname = get_tag_name(path);

	dbg(LOG_INFO, "MKDIR on %s", tagname);

	sql_create_tag(tagname);

	freenull(tagname);

	stop_labeled_time_profile("mkdir");
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

	char *filename = get_tag_name(path);

	dbg(LOG_INFO, "UNLINK on %s", filename);

	ptree_or_node_t *pt = build_querytree(path);
	ptree_or_node_t *ptx = pt;

	while (ptx != NULL) {
		ptree_and_node_t *element = ptx->and_set;
		while (element != NULL) {
			dbg(LOG_INFO, "removing tag %s from %s", element->tag, filename);

			sql_untag_file(element->tag, filename);

			element = element->next;
		}
		ptx = ptx->next;
	}

	destroy_querytree(pt);

	/* checking if file has more tags or is untagged */
	int exists = 0;
	tagsistant_query("select tagname from tagged where filename = \"%s\";", report_if_exists, &exists, filename);
	if (!exists) {
		/* file is no longer tagged, so can be deleted from archive */
		char *filepath = get_file_path(filename);
		dbg(LOG_INFO, "Unlinking file %s: it's no longer tagged!", filepath);
		res = unlink(filepath);
		tagsistant_errno = errno;
		freenull(filepath);
	}

	freenull(filename);

	stop_labeled_time_profile("unlink");
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

	ptree_or_node_t *pt = build_querytree(path);
	ptree_or_node_t *ptx = pt;

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

	destroy_querytree(pt);

	stop_labeled_time_profile("rmdir");
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

	/* return if "to" path is complex, i.e. including logical operators */
	if ((strstr(to, "/AND") != NULL) || (strstr(to, "/OR") != NULL)) {
		dbg(LOG_ERR, "Logical operators not allowed in destination path");
		return -ENOTDIR;
	}

	char *tagname = get_tag_name(from);

	if (rindex(from, '/') == from) {
		/* is a tag */
		const char *newtagname = rindex(to, '/');
		if (newtagname == NULL) {
			newtagname = to;
		} else {
			newtagname++; /* skip the slash */
		}
		
		sql_rename_tag(newtagname, tagname);
	} else {
		/* is a file */
		const char *newfilename = rindex(to, '/');
		if (newfilename == NULL) {
			newfilename = to;
		} else {
			newfilename++; /* skip the slash */
		}

		sql_rename_file(newfilename, tagname);

		char *filepath = get_file_path(tagname);
		char *newfilepath = get_file_path(newfilename);
		res = rename(filepath, newfilepath);
		tagsistant_errno = errno;
		freenull(filepath);
		freenull(newfilepath);
	}

	stop_labeled_time_profile("rename");
	freenull(tagname);
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
	freenull(path_dup);

	/* tag the link */
	dbg(LOG_INFO, "Tagging file inside tagsistant_symlink() or tagsistant_link()");
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

	freenull(tagname);
	freenull(filename);
	freenull(topath);

	stop_labeled_time_profile("symlink");
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

	char *tagname = get_tag_name(path);
	char *filepath = get_file_path(tagname);

	res = chmod(filepath, mode);
	tagsistant_errno = errno;

	freenull(tagname);
	freenull(filepath);

	stop_labeled_time_profile("chmod");
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

	char *tagname = get_tag_name(path);
	char *filepath = get_file_path(tagname);

	res = chown(filepath, uid, gid);
	tagsistant_errno = errno;

	freenull(tagname);
	freenull(filepath);

	stop_labeled_time_profile("chown");
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

	char *tagname = get_tag_name(path);
	char *filepath = get_file_path(tagname);

	res = truncate(filepath, size);
	tagsistant_errno = errno;

	freenull(tagname);
	freenull(filepath);

	stop_labeled_time_profile("truncate");
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
	(void) path;

	init_time_profile();
	start_time_profile();

	char *tagname = get_tag_name(path);
	char *filepath = get_file_path(tagname);

	res = utime(filepath, buf);
	tagsistant_errno = errno;

	freenull(tagname);
	freenull(filepath);

	stop_labeled_time_profile("utime");
	return (res == -1) ? -errno : 0;
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
	char *filename = NULL, *filepath = NULL, *tagname = NULL;
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

	freenull(filename);
	freenull(filepath);
	freenull(tagname);
	destroy_querytree(pt);

	stop_labeled_time_profile("open");
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
	char *filename = NULL, *filepath = NULL, *tagname = NULL;
	get_filename_and_tagname(path, &filename, &filepath, &tagname);

	init_time_profile();
	start_time_profile();

	int fd = internal_open(filepath, fi->flags|O_RDONLY, &tagsistant_errno); 
	if (fd != -1) {
		res = pread(fd, buf, size, offset);
		tagsistant_errno = errno;
		close(fd);
	}

	freenull(filename);
	freenull(filepath);
	freenull(tagname);

	stop_labeled_time_profile("read");
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
	char *filename = NULL, *filepath = NULL, *tagname = NULL;
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
#if VERBOSE_DEBUG
		dbg(LOG_INFO, "writing %d bytes to %s", size, path);
#endif
		res = pwrite(fd, buf, size, offset);
		tagsistant_errno = errno;

		if (res == -1)
			dbg(LOG_INFO, "Error on fd.%d: %s", fd, strerror(tagsistant_errno));

		close(fd);

	}

	if (res == -1)
		dbg(LOG_INFO, "WRITE: returning %d: %s", tagsistant_errno, strerror(tagsistant_errno));

	freenull(filename);
	freenull(filepath);
	freenull(tagname);

	stop_labeled_time_profile("write");
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

	stop_labeled_time_profile("statfs");
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
		" (c) 2006-2007 Tx0 <tx0@strumentiresistenti.org>\n"
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
				tagsistant.mountpoint = strdup(arg);
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
	dbg(LOG_ERR, "Got Signal %d in %s:%d", s, __FILE__, __LINE__);
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
		fprintf(stderr, " *** logging malloc() calls to %s ***\n", destfile);
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
	char *volname = calloc(sizeof(char), strlen("-ovolname=") + strlen(tagsistant.mountpoint) + 1);
	if (volname != NULL) {
		strcpy(volname, "-ovolname=");
		strcat(volname, tagsistant.mountpoint);
		fuse_opt_add_arg(&args, volname);
		freenull(volname);
	}
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
		" (c) 2006-2007 Tx0 <tx0@strumentiresistenti.org>\n"
		" For license informations, see %s -h\n\n"
		, PACKAGE_VERSION, FUSE_USE_VERSION, tagsistant.progname
	);
	
	/* checking repository */
	if (!tagsistant.repository || (strcmp(tagsistant.repository, "") == 0)) {
		if (strlen(getenv("HOME"))) {
			int replength = strlen(getenv("HOME")) + strlen("/.tagsistant") + 1;
			freenull(tagsistant.repository);
			tagsistant.repository = calloc(replength, sizeof(char));
			strcat(tagsistant.repository, getenv("HOME"));
			strcat(tagsistant.repository, "/.tagsistant");
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
			char *relative_path = strdup(tagsistant.repository + 1);
			freenull(tagsistant.repository);
			tagsistant.repository = calloc(sizeof(char), strlen(relative_path) + strlen(home_path) + 1);
			strcpy(tagsistant.repository, home_path);
			strcat(tagsistant.repository, relative_path);
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
			char *absolute_repository = calloc(sizeof(char), strlen(tagsistant.repository) + strlen(cwd) + 2);
			if (absolute_repository == NULL) {
				dbg(LOG_ERR, "Error allocaing memory @%s:%d", __FILE__, __LINE__);
				dbg(LOG_ERR, "Repository path will be left as is");
			} else {
				strcpy(absolute_repository, cwd);
				strcat(absolute_repository, "/");
				strcat(absolute_repository, tagsistant.repository);
				freenull(tagsistant.repository);
				tagsistant.repository = absolute_repository;
				dbg(LOG_ERR, "Repository path is %s", tagsistant.repository);
			}
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
		tagsistant_init_database();
	}

	/* tagsistant.dbh is no longer used as a permanent connection */
	sqlite3_close(tagsistant.dbh);

	/* checking file archive directory */
	tagsistant.archive = malloc(strlen(tagsistant.repository) + strlen("/archive/") + 1);
	strcpy(tagsistant.archive,tagsistant.repository);
	strcat(tagsistant.archive,"/archive/");

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
	char *tagsistant_plugins = NULL;
	if (getenv("TAGSISTANT_PLUGINS") != NULL) {
		tagsistant_plugins = strdup(getenv("TAGSISTANT_PLUGINS"));
		if (!tagsistant.quiet)
			fprintf(stderr, " Using user defined plugin dir: %s\n", tagsistant_plugins);
	} else {
		tagsistant_plugins = strdup(PLUGINS_DIR);
		if (!tagsistant.quiet)
			fprintf(stderr, " Using default plugin dir: %s\n", tagsistant_plugins);
	}

	struct stat st;
	if (lstat(tagsistant_plugins, &st) == -1) {
		if (!tagsistant.quiet)
			fprintf(stderr, " *** error opening directory %s: %s ***\n", tagsistant_plugins, strerror(errno));
	} else if (!S_ISDIR(st.st_mode)) {
		if (!tagsistant.quiet)
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
		freenull(NEW_LD_LIBRARY_PATH);
		if (!tagsistant.quiet)
			fprintf(stderr, " LD_LIBRARY_PATH = %s\n", getenv("LD_LIBRARY_PATH"));
#endif

		/* open directory and read contents */
		DIR *p = opendir(tagsistant_plugins);
		if (p == NULL) {
			if (!tagsistant.quiet)
				fprintf(stderr, " *** error opening plugin directory %s ***\n", tagsistant_plugins);
		} else {
			struct dirent *de = NULL;
			while ((de = readdir(p)) != NULL) {
				/* checking if file begins with tagsistant plugin prefix */
				char *needle = strstr(de->d_name, TAGSISTANT_PLUGIN_PREFIX);
				if ((needle == NULL) || (needle != de->d_name)) continue;

#				ifdef MACOSX
#					define PLUGIN_EXT ".dylib"
#				else
#					define PLUGIN_EXT ".so"
#				endif

				needle = strstr(de->d_name, PLUGIN_EXT);
				if ((needle == NULL) || (needle != de->d_name + strlen(de->d_name) - strlen(PLUGIN_EXT)))
					continue;

				/* file is a tagsistant plugin (beginning by right prefix) and is processed */
				/* allocate new plugin object */
				tagsistant_plugin_t *plugin = NULL;
				while ((plugin = calloc(1, sizeof(tagsistant_plugin_t))) == NULL);

				char *pname = calloc(strlen(de->d_name) + strlen(tagsistant_plugins) + 2, sizeof(char));
				strcat(pname, tagsistant_plugins);
				strcat(pname, "/");
				strcat(pname, de->d_name);

				/* load the plugin */
				plugin->handle = dlopen(pname, RTLD_NOW|RTLD_GLOBAL);
				if (plugin->handle == NULL) {
					if (!tagsistant.quiet)
						fprintf(stderr, " *** error dlopen()ing plugin %s: %s ***\n", de->d_name, dlerror());
					freenull(plugin);
				} else {
					/* search for init function and call it */
					int (*init_function)() = NULL;
					init_function = dlsym(plugin->handle, "plugin_init");
					if (init_function != NULL) {
						int init_res = init_function();
						if (!init_res) {
							/* if init failed, ignore this plugin */
							dbg(LOG_ERR, " *** error calling plugin_init() on %s ***\n", de->d_name);
							freenull(plugin);
							continue;
						}
					}

					/* search for MIME type string */
					plugin->mime_type = dlsym(plugin->handle, "mime_type");
					if (plugin->mime_type == NULL) {
						if (!tagsistant.quiet)
							fprintf(stderr, " *** error finding %s processor function: %s ***\n", de->d_name, dlerror());
						freenull(plugin);
					} else {
						/* search for processor function */
						plugin->processor = dlsym(plugin->handle, "processor");	
						if (plugin->processor == NULL) {
							if (!tagsistant.quiet)
								fprintf(stderr, " *** error finding %s processor function: %s ***\n", de->d_name, dlerror());
							freenull(plugin);
						} else {
							plugin->free = dlsym(plugin->handle, "plugin_free");
							if (plugin->free == NULL) {
								if (!tagsistant.quiet)
									fprintf(stderr, " *** error finding %s free function: %s (still registering the plugin) ***", de->d_name, dlerror());
							}

							/* add this plugin on queue head */
							plugin->filename = strdup(de->d_name);
							plugin->next = plugins;
							plugins = plugin;
							if (!tagsistant.quiet)
								fprintf(stderr, " Loaded plugin %s \t-> \"%s\"\n", de->d_name, plugin->mime_type);
						}
					}
				}
				freenull(pname);
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

#if FUSE_VERSION <= 25
	res = fuse_main(args.argc, args.argv, &tagsistant_oper);
#else
	res = fuse_main(args.argc, args.argv, &tagsistant_oper, NULL);
#endif

	fuse_opt_free_args(&args);

	/* unregistering plugins */
	tagsistant_plugin_t *pp = plugins;
	tagsistant_plugin_t *ppnext = pp;
	while (pp != NULL) {
		/* call plugin free method to let it free allocated resources */
		if (pp->free != NULL) {
			(pp->free)();
		}
		freenull(pp->filename);	/* free plugin filename */
		dlclose(pp->handle);	/* unload the plugin */
		ppnext = pp->next;		/* save next plugin in tagsistant chain */
		freenull(pp);			/* free this plugin entry in tagsistant chain */
		pp = ppnext;			/* point to next plugin in tagsistant chain */
	}

	/* free memory to better perfom memory leak profiling */
	free(tagsistant_plugins);
	free(tagsistant.repository);
	free(tagsistant.archive);
	free(tagsistant.tags);

	return res;
}

// vim:ts=4:autoindent:nocindent:syntax=c
