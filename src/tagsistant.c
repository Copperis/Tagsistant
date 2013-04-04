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
#include "../config.h" // this is just to suppress eclipse warnings
#include "buildnumber.h"

#ifndef MACOSX
#include <mcheck.h>
#endif

#define BUILD_DATE "/"

/* defines command line options for tagsistant mount tool */
struct tagsistant tagsistant;

static int tagsistant_fsync(const char *path, int isdatasync,
                     struct fuse_file_info *fi)
{
    /* Just a stub.  This method is optional and can safely be left
       unimplemented */

    (void) path;
    (void) isdatasync;
    (void) fi;

	return(0); /* REMOVE ME AFTER CODING */
}

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int tagsistant_setxattr(const char *path, const char *name, const char *value,
                        size_t size, int flags)
{
    int res;

	return(0); /* REMOVE ME AFTER CODING */
}

static int tagsistant_getxattr(const char *path, const char *name, char *value,
                    size_t size)
{
    int res;
	return(0); /* REMOVE ME AFTER CODING */
}

static int tagsistant_listxattr(const char *path, char *list, size_t size)
{
    int res;

	return(0); /* REMOVE ME AFTER CODING */
}

static int tagsistant_removexattr(const char *path, const char *name)
{
    int res;

	return(0); /* REMOVE ME AFTER CODING */
}
#endif /* HAVE_SETXATTR */

#if FUSE_VERSION >= 26

static void *tagsistant_init(struct fuse_conn_info *conn)
{
	(void) conn;
	return(NULL);
}

#else

static void *tagsistant_init(void)
{
	return(NULL);
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
    .flush		= tagsistant_flush,
//    .release	= tagsistant_release,
#if FUSE_USE_VERSION >= 25
    .statfs		= tagsistant_statvfs,
#else
    .statfs		= tagsistant_statfs,
#endif
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
	TAGSISTANT_OPT("-d",				debug,			1),
	TAGSISTANT_OPT("--repository=%s",	repository,		0),
	TAGSISTANT_OPT("-f",				foreground,		1),
	TAGSISTANT_OPT("-s",				singlethread,	1),
	TAGSISTANT_OPT("--db=%s",			dboptions,		0),
	TAGSISTANT_OPT("-r",				readonly,		1),
	TAGSISTANT_OPT("-v",				verbose,		1),
	TAGSISTANT_OPT("-q",				quiet,			1),
	TAGSISTANT_OPT("-p",				show_config,	1),
	
	FUSE_OPT_KEY("-V",          		KEY_VERSION),
	FUSE_OPT_KEY("--version",   		KEY_VERSION),
	FUSE_OPT_KEY("-h",          		KEY_HELP),
	FUSE_OPT_KEY("--help",      		KEY_HELP),
	FUSE_OPT_END
};

int usage_already_printed = 0;

/**
 * print usage message on STDOUT
 *
 * @param progname the name tagsistant was invoked as
 */
void tagsistant_usage(char *progname)
{
	if (usage_already_printed++)
		return;

	fprintf(stderr, "\n"
		" Tagsistant (tagfs) v.%s Build: %s FUSE_USE_VERSION: %d\n"
		" Semantic File System for Linux kernels\n"
		" (c) 2006-2013 Tx0 <tx0@strumentiresistenti.org>\n"
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
		"    -q                     be quiet\n"
		"    -r                     mount readonly\n"
		"    -v                     verbose syslogging\n   "
		"\n" /*fuse options will follow... */
		, PACKAGE_VERSION, TAGSISTANT_BUILDNUMBER, FUSE_USE_VERSION, progname
	);
}

/**
 * process command line options
 * 
 * @param data pointer (unused)
 * @param arg argument pointer (if key has one)
 * @param key command line option to be processed
 * @param outargs structure holding libfuse options
 * @return 1 on success, 0 otherwise
 */
static int tagsistant_opt_proc(void *data, const char *arg, int key, struct fuse_args *outargs)
{
    (void) data;
    (void) data;

    switch (key) {
		case FUSE_OPT_KEY_NONOPT:
			if (!tagsistant.mountpoint) {
				tagsistant.mountpoint = g_strdup(arg);
				return(1);
			}
			return(0);

	    case KEY_HELP:
	        tagsistant_usage(outargs->argv[0]);
	        fuse_opt_add_arg(outargs, "-ho");
#if FUSE_VERSION <= 25
	        fuse_main(outargs->argc, outargs->argv, &tagsistant_oper);
#else
	        fuse_main(outargs->argc, outargs->argv, &tagsistant_oper, NULL);
#endif
	        exit(1);
	
	    case KEY_VERSION:
	        fprintf(stderr, "Tagsistant for Linux 0.4 (prerelease %s)\n", VERSION);
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
	        tagsistant_usage(outargs->argv[0]);
	        break;
    }

	return(0);
}

/**
 * cleanup hook used by signal()
 *
 * @param s the signal number passed by signal()
 */
void cleanup(int s)
{
	dbg(LOG_ERR, "Got Signal %d", s);
	exit(s);
}

extern void tagsistant_show_config();

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

	if (fuse_opt_parse(&args, &tagsistant, tagsistant_opts, tagsistant_opt_proc) == -1) {
		exit(1);
	}

	/* do some tuning on FUSE options */
	fuse_opt_add_arg(&args, "-s");
	fuse_opt_add_arg(&args, "-odirect_io");
	fuse_opt_add_arg(&args, "-obig_writes");
	fuse_opt_add_arg(&args, "-omax_write=131072");
//	fuse_opt_add_arg(&args, "-ofstype=tagsistant");
	fuse_opt_add_arg(&args, "-ofsname=tagsistant");
//	fuse_opt_add_arg(&args, "-ouse_ino,readdir_ino");
	// fuse_opt_add_arg(&args, "-oallow_other");

#ifdef MACOSX
	fuse_opt_add_arg(&args, "-odefer_permissions");
	gchar *volname = g_strdup_printf("-ovolname=%s", tagsistant.mountpoint);
	fuse_opt_add_arg(&args, volname);
	freenull(volname);
#else
	/* fuse_opt_add_arg(&args, "-odefault_permissions"); */
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
	if (!(tagsistant.mountpoint||tagsistant.show_config)) {
		tagsistant_usage(tagsistant.progname);
		fprintf(stderr, " *** No mountpoint provided *** \n\n");
		exit(2);
	}

	/* checking if mount point exists or can be created */
	struct stat mst;
	if ((lstat(tagsistant.mountpoint, &mst) == -1) && (errno == ENOENT)) {
		if (mkdir(tagsistant.mountpoint, S_IRWXU|S_IRGRP|S_IXGRP) != 0) {
			tagsistant_usage(tagsistant.progname);
			if (!tagsistant.quiet)
				fprintf(stderr, "\n    Mountpoint %s does not exists and can't be created!\n\n", tagsistant.mountpoint);
			if (!tagsistant.show_config)
				exit(1);
		}
	}

	if (!tagsistant.quiet)
		fprintf(stderr, "\n");
	if (!tagsistant.quiet)
		fprintf(stderr,
		" Tagsistant (tagfs) v.%s Build: %s FUSE_USE_VERSION: %d\n"
		" (c) 2006-2009 Tx0 <tx0@strumentiresistenti.org>\n"
		" For license informations, see %s -h\n\n"
		, PACKAGE_VERSION, TAGSISTANT_BUILDNUMBER, FUSE_USE_VERSION, tagsistant.progname
	);

	/* checking repository */
	if (!tagsistant.repository || (strcmp(tagsistant.repository, "") == 0)) {
		if (strlen(getenv("HOME"))) {
			freenull(tagsistant.repository);
			tagsistant.repository = g_strdup_printf("%s/.tagsistant", getenv("HOME"));
			if (!tagsistant.quiet)
				fprintf(stderr, " Using default repository %s\n", tagsistant.repository);
		} else {
			tagsistant_usage(tagsistant.progname);
			if (!tagsistant.show_config) {
				if (!tagsistant.quiet)
					fprintf(stderr, " *** No repository provided with -r ***\n\n");
				exit(2);
			}
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

	if (tagsistant.debug)
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
	g_thread_init(NULL);
	tagsistant_plugin_loader();

	dbg(LOG_INFO, "Mounting filesystem");

	dbg(LOG_INFO, "Fuse options:");
	int fargc = args.argc;
	while (fargc) {
		dbg(LOG_INFO, "%.2d: %s", fargc, args.argv[fargc]);
		fargc--;
	}

	/*
	 * print configuration if requested
	 */
	if (tagsistant.show_config) tagsistant_show_config();

	/*
	 * initialize db connection, SQL schema,
	 * path_resolution.c and utils.c structures
	 */
	tagsistant_db_init();
	tagsistant_create_schema();
	tagsistant_path_resolution_init();
	tagsistant_utils_init();

#if FUSE_VERSION <= 25
	res = fuse_main(args.argc, args.argv, &tagsistant_oper);
#else
	res = fuse_main(args.argc, args.argv, &tagsistant_oper, NULL);
#endif

	fuse_opt_free_args(&args);

	/*
	 * unloading plugins
	 */
	tagsistant_plugin_unloader();

	/* free memory to better perfom memory leak profiling */
	freenull(tagsistant.dboptions);
	freenull(tagsistant.repository);
	freenull(tagsistant.archive);
	freenull(tagsistant.tags);

	return(res);
}

// vim:ts=4:autoindent:nocindent:syntax=c
