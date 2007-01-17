/*
   TAGFS -- mount.tagfs.c
   Copyright (C) 2006 Tx0 <tx0@autistici.org>

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

/**
 * lstat equivalent
 */
static int tagfs_getattr(const char *path, struct stat *stbuf)
{
    int res = 0, tagfs_errno = 0;

	init_time_profile();
	start_time_profile();

	char *tagname = get_tag_name(path);

	/* special case */
	if ((strcasecmp(tagname, "AND") == 0)
	 || (strcasecmp(tagname, "OR") == 0)) {
	 	lstat(tagfs.tagsdir, stbuf);	
		free(tagname);
		return 0;
	}

	char *tagpath = get_tag_path(tagname);

	dbg(LOG_INFO, "GETATTR %s -> %s", path, tagpath);
	res = lstat(tagpath, stbuf);
	tagfs_errno = errno;

	if ((res == -1) && (tagfs_errno == ENOENT)) {
		/* must be a file */
		dbg(LOG_INFO, "%s is not a tag, must be a file", tagname);

		path_tree_t *pt = build_pathtree(path);
		path_tree_t *ptx = pt;
		while (ptx != NULL) {

			char *filepath = malloc(strlen(tagfs.tagsdir) + strlen(ptx->name) + strlen(tagname) + 3);
			if (filepath != NULL) {
				strcpy(filepath, tagfs.tagsdir);	
				strcat(filepath, "/");
				strcat(filepath, ptx->name);
				strcat(filepath, "/");
				strcat(filepath, tagname);
				res = stat(filepath, stbuf);
				tagfs_errno = errno;
				free(filepath);
				if (res != -1)
					break;
			}
			
			if (ptx->AND != NULL)
				ptx = ptx->AND;
			else
				ptx = ptx->OR;
		}
		destroy_path_tree(pt);

	}

	free(tagname);
	free(tagpath);

	stop_labeled_time_profile("getattr");
	if ( res == -1 ) {
		dbg(LOG_INFO, "GETATTR exited: %d %d: %s", res, tagfs_errno, strerror(tagfs_errno));
	} else {
		dbg(LOG_INFO, "GETATTR exited: %d", res);
	}

    return (res == -1) ? -tagfs_errno : 0;
}

static int tagfs_readlink(const char *path, char *buf, size_t size)
{
	strncpy(buf, path, size);
	return 0;
}

static int tagfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
	(void) fi;
	(void) offset;

	filler(buf, ".", NULL, 0);
	if (strcmp(path, "/") != 0)
		filler(buf, "..", NULL, 0);

	char *tagname = get_tag_name(path);
	if (
		strlen(tagname) &&
		(strcasecmp(tagname,"AND") != 0) &&
		(strcasecmp(tagname,"OR") != 0)
	) {

		/*
		 * if path does not terminates with a logical operator,
		 * directory should be filled with logical operators and
		 * with files from the filetree build on path
		 */

		/* add logical operators */
		filler(buf, "AND", NULL, 0);
		filler(buf, "OR", NULL, 0);

		char *pathcopy = strdup(path);
	
		path_tree_t *pt = build_pathtree(pathcopy);
		if (pt == NULL)
			return -EBADF;
	
		file_handle_t *fh = build_filetree(pt);
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
	
		destroy_path_tree(pt);
		destroy_file_tree(fh);

	} else {

		/*
		 * if path does terminate with a logical operator
		 * directory should be filled with tagsdir registered tags
		 */

		/* parse tagsdir list */
		DIR *dp = opendir(tagfs.tagsdir);
		struct dirent *de;
		while ((de = readdir(dp))) {
			if ((strcmp(de->d_name,".")!=0) && (strcmp(de->d_name,"..")!=1)) {
				if (filler(buf, de->d_name, NULL, 0))
					break;
			}
		}
		closedir(dp);

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

	char *tagname = get_tag_name(path);
	char *filename = get_file_path(tagname);

	res = mknod(filename, mode, rdev);
	tagfs_errno = errno;

	char *destpath = malloc(strlen(tagfs.tagsdir) + strlen(path));
	if (destpath != NULL) {
		strcpy(destpath, tagfs.tagsdir);
		strcat(destpath, path);
		dbg(LOG_INFO, "linking %s to %s", filename, destpath);
		int err = symlink(filename, destpath);
		if (err == -1) {
			dbg(LOG_ERR, "Error linking: %s", strerror(errno));
		}
		free(destpath);
	}

	free(tagname);
	free(filename);

	stop_labeled_time_profile("mknod");
	return (res == -1) ? -tagfs_errno: 0;
}

/* OK */
static int tagfs_mkdir(const char *path, mode_t mode)
{
    int res = 0, tagfs_errno = 0;
	init_time_profile();
	start_time_profile();

	char *tagname = get_tag_name(path);
	if (tagname != NULL) {
		char *relocated = get_tag_path(tagname);
		if (mkdir(relocated,mode) == -1) {
			tagfs_errno = errno;
			dbg(LOG_ERR, "Can't create tagdir %s: %s", relocated, strerror(errno));
			free(relocated);
			goto MKDIRABORT;
		}
		chmod(relocated, mode);
		/*
		chmod(relocated, S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH);
		*/
		free(relocated);
	}

MKDIRABORT:
	free(tagname);

	stop_labeled_time_profile("mkdir");
	return (res == -1) ? -tagfs_errno : 0;
}

/* OK */
static int tagfs_unlink(const char *path)
{
    int res = 0, tagfs_errno = 0;

	init_time_profile();
	start_time_profile();

	char *tagname = get_tag_name(path);
	char *filepath = get_file_path(tagname);

	path_tree_t *pt = build_pathtree(path);
	path_tree_t *element = pt;

	/* is a file */
	while (element != NULL) {
		char *delpath = malloc(
			strlen(tagfs.tagsdir) +
			strlen(element->name) +
			strlen(tagname) +
			2
		);
		if (delpath != NULL) {
			strcpy(delpath, tagfs.tagsdir);
			strcat(delpath, element->name);
			strcat(delpath, "/");
			strcat(delpath, tagname);

			dbg(LOG_INFO, "unlink(%s)", delpath);
			unlink(delpath);
			free(delpath);
		}
		if (element->AND != NULL)
			element = element->AND;
		else
			element = element->OR;
	}

	/*
	stat(filepath, &filest);
	if (filest.st_nlink == 1) {
		dbg(LOG_INFO, "unlinking original copy too...");
		unlink(filepath);
	}
	*/

	free(tagname);
	free(filepath);
	destroy_path_tree(pt);

	stop_labeled_time_profile("unlink");
	return (res == -1) ? -tagfs_errno : 0;
}

/* OK */
static int tagfs_rmdir(const char *path)
{
    int res = 0, tagfs_errno = 0;
	init_time_profile();
	start_time_profile();

	path_tree_t *pt = build_pathtree(path);

	path_tree_t *element = pt;
	while (element != NULL) {
		char *delpath = malloc(strlen(tagfs.tagsdir) + strlen(element->name) + 1);
		if (delpath != NULL) {
			strcpy(delpath, tagfs.tagsdir);
			strcat(delpath, element->name);

			dbg(LOG_INFO, "unlink(%s)", delpath);
			res = rmdir(delpath);
			tagfs_errno = errno;
			free(delpath);
			if (res == -1)
				break;
		}
		if (element->AND != NULL)
			element = element->AND;
		else
			element = element->OR;
	}

	destroy_path_tree(pt);

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
	char *tagpath = get_tag_path(tagname);
	struct stat stbuf;
	dbg(LOG_INFO, "RENAME: stat(%s)", tagpath);
	res = lstat(tagpath, &stbuf);
	if (res == -1) {
		char *filepath = get_file_path(tagname);
		dbg(LOG_INFO, "RENAME: stat(%s)", filepath);
		res = lstat(filepath, &stbuf);
		if (res == -1) {
			dbg(LOG_ERR, "RENAME: error -1- %s: %s", filepath, strerror(errno));
			free(filepath);
			free(tagname);
			free(tagpath);
			return -errno;
		}
		free(filepath);
	}

	if (S_ISDIR(stbuf.st_mode)) {
		/* return if "from" path is complex, i.e. including logical operators */
		if ((strstr(from, "/AND") != NULL) || (strstr(from, "/OR") != NULL)) {
			dbg(LOG_ERR, "Logical operators not allowed in open path");
			free(tagname);
			free(tagpath);
			return -ENOTDIR;
		}

		/* origin is a direcory */
		char *toname = get_tag_name(to);
		char *topath = get_tag_path(toname);
		dbg(LOG_INFO, "Renaming directory %s to %s", from, to);
		res = rename(tagpath, topath);
		tagfs_errno = errno;
		free(toname);
		free(topath);

	} else {

		/* origin is a file */
		path_tree_t *from_pt = build_pathtree(from);

		char *orig = malloc(
			strlen(tagfs.tagsdir) +
			strlen(from_pt->name) +
			strlen(tagname) +
			3
		);
		if (orig == NULL) {
			dbg(LOG_ERR, "RENAME: -2- Error allocating memory");
			destroy_path_tree(from_pt);
			free(tagname);
			free(tagpath);
			return -ENOMEM;
		}
		strcpy(orig, tagfs.tagsdir);
		strcat(orig, "/");
		strcat(orig, from_pt->name);
		strcat(orig, "/");
		strcat(orig, tagname);

		char *dest = malloc(
			strlen(tagfs.tagsdir) +
			strlen(to) +
			2
		);
		if (dest == NULL) {
			dbg(LOG_ERR, "RENAME: -3- Error allocating memory");
			free(orig);
			destroy_path_tree(from_pt);
			free(tagname);
			free(tagpath);
			return -ENOMEM;
		}
		strcpy(dest, tagfs.tagsdir);
		strcat(dest, "/");
		strcat(dest, to);

		dbg(LOG_INFO, "Renaming file %s to %s", orig, dest);
		res = rename(orig, dest);
		tagfs_errno = errno;

		free(orig);
		free(dest);

		path_tree_t *element = (from_pt->AND != NULL) ? from_pt->AND : from_pt->OR;
		while (element != NULL) {
			char *unlinkpath = malloc(
				strlen(tagfs.tagsdir) +
				strlen(element->name) +
				strlen(tagname) +
				3
			);
			if (unlinkpath != NULL) {
				strcpy(unlinkpath, tagfs.tagsdir);
				strcat(unlinkpath, "/");
				strcat(unlinkpath, element->name);
				strcat(unlinkpath, "/");
				strcat(unlinkpath, tagname);
				unlink(unlinkpath);
				free(unlinkpath);
			}
			element = (from_pt->AND != NULL) ? from_pt->AND : from_pt->OR;
		}

		destroy_path_tree(from_pt);
	}

	free(tagname);
	free(tagpath);

	stop_labeled_time_profile("rename");
	return (res == -1) ? -tagfs_errno : 0;
}

/* OK */
static int tagfs_link(const char *from, const char *to)
{
    int res = 0, tagfs_errno = 0;

	init_time_profile();
	start_time_profile();

	char *tagname = get_tag_name(from);
	char *frompath = get_file_path(tagname);

	tagname = get_tag_name(to);
	char *topath = get_file_path(tagname);

	dbg(LOG_INFO, "Linking %s as %s", frompath, topath);
	res = symlink(frompath, topath);
	tagfs_errno = errno;

	free(tagname);
	free(frompath);
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
	char *tagname = get_tag_name(path);
	char *filepath = get_file_path(tagname);

	dbg(LOG_INFO, "INTERNAL_OPEN: Opening file %s", filepath);

	if (flags&O_CREAT) dbg(LOG_INFO, "...O_CREAT");
	if (flags&O_WRONLY) dbg(LOG_INFO, "...O_WRONLY");
	if (flags&O_TRUNC) dbg(LOG_INFO, "...O_TRUNC");
	if (flags&O_LARGEFILE) dbg(LOG_INFO, "...O_LARGEFILE");

	int res = open(filepath, flags);
	*_errno = errno;

	free(tagname);
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
	TAGFS_OPT("-d",					debug,		1),
	TAGFS_OPT("--repository=%s",	repository,	0),

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
		" TAGFS v.%s\n"
		" Semantic File System for Linux kernels\n"
		" (c) 2006-2007 Tx0 <tx0@autistici.org>\n"
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
		"\n"
		" Usage: %s [OPTIONS] /mountpoint\n"
		"\n"
		" --repository=<PATH> Path of repository\n"
		" -d                  Enables debug\n"
		" -u                  unmount a mounted filesystem\n"
		" -q                  be quiet\n"
		" -z                  lazy unmount (can be dangerous!)\n"
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

	/* checking tagged directory structure */
	tagfs.tagsdir = malloc(strlen(tagfs.repository) + strlen("/tagsdir/") + 1);
	strcpy(tagfs.tagsdir,tagfs.repository);
	strcat(tagfs.tagsdir,"/tagsdir/");

	if (lstat(tagfs.tagsdir, &repstat) == -1) {
		if(mkdir(tagfs.tagsdir, 755) == -1) {
			fprintf(stderr, "    *** TAGSDIR: Can't mkdir(%s): %s ***\n\n", tagfs.tagsdir, strerror(errno));
			exit(2);
		}
	}
	chmod(tagfs.tagsdir, S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH);

#ifdef OBSOLETE_CODE
	/* checking tmp archive directory structure */
	tagfs.tmparchive = malloc(strlen(tagfs.repository) + strlen("/tmparchive/") + 1);
	strcpy(tagfs.tmparchive,tagfs.repository);
	strcat(tagfs.tmparchive,"/tmparchive/");

	if (lstat(tagfs.tmparchive, &repstat) == -1) {
		if(mkdir(tagfs.tmparchive, 755) == -1) {
			fprintf(stderr, "    *** TMPARCHIVE: Can't mkdir(%s): %s ***\n\n", tagfs.tmparchive, strerror(errno));
			exit(2);
		}
	}
	chmod(tagfs.tmparchive, S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH);
#endif /* OBSOLETE_CODE */

	// fuse_opt_add_arg(&args, "-odefault_permissions,allow_other,fsname=tagfs");
	fuse_opt_add_arg(&args, "-ouse_ino,readdir_ino");

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
		printf("\n    Debug is enabled");

	umask(0);

#ifdef _DEBUG_SYSLOG
	init_syslog();
#endif

	signal(2,  cleanup); /* SIGINT */
	signal(11, cleanup); /* SIGSEGV */
	signal(15, cleanup); /* SIGTERM */

	dbg(LOG_INFO, "Mounting filesystem");
	res = fuse_main(args.argc, args.argv, &tagfs_oper);

    fuse_opt_free_args(&args);

	return res;
}

// vim:ts=4:autoindent:nocindent:syntax=c
