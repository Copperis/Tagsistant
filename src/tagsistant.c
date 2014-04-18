/*
   Tagsistant (tagfs) -- tagsistant.c
   Copyright (C) 2006-2013 Tx0 <tx0@strumentiresistenti.org>

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

/* defines command line options for tagsistant mount tool */
struct tagsistant tagsistant;

static int tagsistant_fsync(const char *path, int isdatasync, struct fuse_file_info *fi)
{
    /* Just a stub.  This method is optional and can safely be left unimplemented */

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

int usage_already_printed = 0;

/**
 * print usage message on STDOUT
 *
 * @param progname the name tagsistant was invoked as
 */
void tagsistant_usage(gchar *progname, int verbose)
{
	if (usage_already_printed++)
		return;

	gchar *license = verbose
	?
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
	:
		"";

	gchar *endnote = verbose ? "" :	" Add --verbose for more information\n\n";

	fprintf(stderr, "\n"
		" Tagsistant (tagfs) v.%s Build: %s FUSE_USE_VERSION: %d\n"
		" Semantic File System for Linux kernels\n"
		" (c) 2006-2013 Tx0 <tx0@strumentiresistenti.org>\n"
		" \n%s"
		" Usage: \n"
		" \n"
		"  %s [OPTIONS] [--db=<OPTIONS>] [--repository=<PATH>] /mountpoint\n"
		"  %s [OPTIONS] [--db=<OPTIONS>] [/repository/path] /mountpoint\n"
		"\n"
		"    -q                     be quiet\n"
		"    -r                     mount readonly\n"
		"    -v                     verbose syslogging\n"
		"    --debug=bcfFlpqrs2     enable specific debugging output:\n"
		"                             b: boot\n"
		"                             c: cache\n"
		"                             f: file tree (readdir)\n"
		"                             F: FUSE operations (open, read, symlink, ...)\n"
		"                             l: low level\n"
		"                             p: plugin\n"
		"                             q: query parsing\n"
		"                             r: reasoning\n"
		"                             s: SQL queries\n"
		"                             2: deduplication\n"
		"\n%s"
		, PACKAGE_VERSION, TAGSISTANT_BUILDNUMBER, FUSE_USE_VERSION, license, progname, progname, endnote
	);
}

/**
 * cleanup hook used by signal()
 *
 * @param s the signal number passed by signal()
 */
void cleanup(int s)
{
	dbg('b', LOG_ERR, "Got Signal %d", s);
	exit(s);
}

/**
 * Return and entry from the repository.ini file
 *
 * @param section the INI section
 * @param key the INI key
 * @return the value as a string
 */
gchar *tagsistant_get_ini_entry(gchar *section, gchar *key) {
	if (!tagsistant_ini) return (NULL);
	return (g_key_file_get_value(tagsistant_ini, section, key, NULL));
}

extern void tagsistant_show_config();

static GOptionEntry tagsistant_options[] =
{
  { "help", 'h', 0,	 			G_OPTION_ARG_NONE,				&tagsistant.show_help,	 	"Show help screen", NULL },
  { "dbg", 'd', 0,	 			G_OPTION_ARG_NONE,				&tagsistant.debug,	 		"Enable debug", NULL },
  { "debug", 0, 0,	 			G_OPTION_ARG_STRING,			&tagsistant.debug_flags, 	"Debug flags", "bcfFlpqrs2" },
  { "repository", 0, 0,			G_OPTION_ARG_FILENAME,			&tagsistant.repository, 	"Repository path", "<repository path>" },
  { "foreground", 'f', 0, 		G_OPTION_ARG_NONE, 				&tagsistant.foreground, 	"Run in foreground", NULL },
  { "single-thread", 's', 0,	G_OPTION_ARG_NONE,				&tagsistant.singlethread, 	"Don't spawn other threads", NULL },
  { "db", 0, 0,					G_OPTION_ARG_STRING,			&tagsistant.dboptions, 		"Database connection options", "backend:[host:[db:[user:[password]]]]" },
  { "readonly", 'r', 0, 		G_OPTION_ARG_NONE,				&tagsistant.readonly, 		"Mount read-only", NULL },
  { "verbose", 'v', 0,			G_OPTION_ARG_NONE,				&tagsistant.verbose, 		"Be verbose", NULL },
  { "quiet", 'q', 0, 			G_OPTION_ARG_NONE,				&tagsistant.quiet, 			"Be quiet", NULL },
  { "show-config", 'p', 0, 		G_OPTION_ARG_NONE,				&tagsistant.show_config, 	"Print repository INI file", NULL },
  { "version", 'V', 0,			G_OPTION_ARG_NONE,				&tagsistant.show_version, 	"Print version", NULL },
  { "fuse-opt", 'o', 0, 		G_OPTION_ARG_STRING_ARRAY, 		&tagsistant.fuse_opts, 		"Pass options to FUSE", "allow_other, allow_root, ..." },
  { G_OPTION_REMAINING, 0, 0, 	G_OPTION_ARG_FILENAME_ARRAY, 	&tagsistant.remaining_opts, "", "" },
  { NULL, 0, 0, 				0, 								NULL, 						NULL, NULL }
};

/**
 * Call FUSE event loop
 *
 * @param args FUSE arguments structure
 * @param tagsistant_oper the FUSE operation table
 */
int tagsistant_fuse_main(
	struct fuse_args *args,
	struct fuse_operations *tagsistant_oper)
{
#if FUSE_VERSION <= 25
	return (fuse_main(args->argc, args->argv, tagsistant_oper));
#else
	return (fuse_main(args->argc, args->argv, tagsistant_oper, NULL));
#endif
}

int main(int argc, char *argv[])
{
    struct fuse_args args = { 0, NULL, 0 };
	int res;

#ifndef MACOSX
	char *destfile = getenv("MALLOC_TRACE");
	if (destfile != NULL && strlen(destfile)) {
		fprintf(stderr, " *** logging g_malloc() calls to %s ***\n", destfile);
		mtrace();
	}
#endif

	tagsistant.progname = argv[0];
	tagsistant.debug = FALSE;

	int i = 0;
	for (; i < 128; i++) tagsistant.dbg[i] = 0;

	/*
	 * parse command line options
	 */
	GError *error = NULL;
	GOptionContext *context = g_option_context_new ("[repository path] <mount point>");
	g_option_context_add_main_entries (context, tagsistant_options, NULL);
	g_option_context_set_help_enabled (context, FALSE);
	if (!g_option_context_parse (context, &argc, &argv, &error)) {
		fprintf(stderr, " *** option parsing failed: %s\n", error->message);
		exit (1);
	}

	/*
	 * print the help screen
	 */
	if (tagsistant.show_help) {
		tagsistant_usage(argv[0], tagsistant.verbose);

		if (tagsistant.verbose) {
			fuse_opt_add_arg(&args, argv[0]);
			fuse_opt_add_arg(&args, "--help");
			tagsistant_fuse_main(&args, &tagsistant_oper);
		}

		exit(0);
	}

	/*
	 * show Tagsistant and FUSE version
	 */
	if (tagsistant.show_version) {
		fprintf(stderr, "Tagsistant (tagfs) v.%s Build: %s FUSE_USE_VERSION: %d\n", PACKAGE_VERSION, TAGSISTANT_BUILDNUMBER, FUSE_USE_VERSION);

		fuse_opt_add_arg(&args, "-V");
		fuse_opt_add_arg(&args, "--version");
		tagsistant_fuse_main(&args, &tagsistant_oper);

		exit(0);
	}

	/*
	 * look for a mount point (and a repository too)
	 */
	if (tagsistant.remaining_opts && *tagsistant.remaining_opts) {
		if (tagsistant.remaining_opts[1] && *(tagsistant.remaining_opts[1])) {
			tagsistant.repository = *tagsistant.remaining_opts;
			tagsistant.mountpoint = *(tagsistant.remaining_opts + 1);

			// fprintf(stderr, " *** repository %s *** \n\n", tagsistant.repository);
			// fprintf(stderr, " *** mountpoint %s *** \n\n", tagsistant.mountpoint);
		} else {
			tagsistant.mountpoint = *tagsistant.remaining_opts;

			// fprintf(stderr, " *** mountpoint %s *** \n\n", tagsistant.mountpoint);
		}
	} else {
		fprintf(stderr, " *** No mountpoint provided *** \n\n");
		tagsistant_usage(argv[0], 0);
		exit(2);
	}

	/*
	 * default repository
	 */
	if (!tagsistant.repository) {
		tagsistant.repository = g_strdup_printf("%s/.tagsistant/", g_getenv("HOME"));
	}

	/* do some tuning on FUSE options */
	fuse_opt_add_arg(&args, "-s");
//	fuse_opt_add_arg(&args, "-odirect_io");
	fuse_opt_add_arg(&args, "-obig_writes");
	fuse_opt_add_arg(&args, "-omax_write=32768");
	fuse_opt_add_arg(&args, "-omax_read=32768");
	fuse_opt_add_arg(&args, "-ofsname=tagsistant");
//	fuse_opt_add_arg(&args, "-ofstype=tagsistant");
//	fuse_opt_add_arg(&args, "-ouse_ino,readdir_ino");
//	fuse_opt_add_arg(&args, "-oallow_other");

#ifdef MACOSX
	fuse_opt_add_arg(&args, "-odefer_permissions");
	gchar *volname = g_strdup_printf("-ovolname=%s", tagsistant.mountpoint);
	fuse_opt_add_arg(&args, volname);
	g_free_null(volname);
#else
	/* fuse_opt_add_arg(&args, "-odefault_permissions"); */
#endif

	/*
	 * parse debugging flags
	 */
	if (tagsistant.debug_flags) {
		char *dbg_ptr = tagsistant.debug_flags;
		while (*dbg_ptr) {
			tagsistant.dbg[(int) *dbg_ptr] = 1;
			dbg_ptr++;
		}
	}

	/*
	 * Will run as a single threaded application?
	 */
	if (tagsistant.singlethread) {
		if (!tagsistant.quiet)
			fprintf(stderr, " *** operating in single thread mode ***\n");
		fuse_opt_add_arg(&args, "-s");
	}

	/*
	 * Will run readonly?
	 */
	if (tagsistant.readonly) {
		if (!tagsistant.quiet)
			fprintf(stderr, " *** mounting tagsistant read-only ***\n");
		fuse_opt_add_arg(&args, "-r");
	}

	/*
	 * Will run in foreground?
	 */
	if (tagsistant.foreground) {
		if (!tagsistant.quiet)
			fprintf(stderr, " *** will run in foreground ***\n");
		fuse_opt_add_arg(&args, "-f");
	}

	/*
	 * Will be verbose?
	 */
	if (tagsistant.verbose) {
		if (!tagsistant.quiet)
			fprintf(stderr, " *** will log verbosely ***\n");
		fuse_opt_add_arg(&args, "-d");
	}

	/*
	 * Have received DB options?
	 */
	if (tagsistant.dboptions) {
		if (!tagsistant.quiet)
			fprintf(stderr, " *** connecting to %s\n", tagsistant.dboptions);
	}

	/*
	 * The repository was provided?
	 */
	if (tagsistant.repository) {
		if (!tagsistant.quiet)
			fprintf(stderr, " *** saving repository in %s\n", tagsistant.repository);
	}

	/*
	 * add FUSE options
	 */
	gchar **fuse_opt = tagsistant.fuse_opts;
	while (fuse_opt && *fuse_opt) {
		fprintf(stderr, " *** Adding FUSE options %s\n", *fuse_opt);
		gchar *fuse_opt_string = g_strdup_printf("-o%s", *fuse_opt);
		fuse_opt_add_arg(&args, fuse_opt_string);
		fuse_opt++;
	}

	/*
	 * checking if mount point exists or can be created
	 */
	struct stat mst;
	if ((lstat(tagsistant.mountpoint, &mst) == -1) && (errno == ENOENT)) {
		if (mkdir(tagsistant.mountpoint, S_IRWXU|S_IRGRP|S_IXGRP) != 0) {
			// tagsistant_usage(tagsistant.progname);
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
		" (c) 2006-2013 Tx0 <tx0@strumentiresistenti.org>\n"
		" For license informations, see %s -h\n\n"
		, PACKAGE_VERSION, TAGSISTANT_BUILDNUMBER, FUSE_USE_VERSION, tagsistant.progname
	);

	/* checking repository */
	if (!tagsistant.repository || (strcmp(tagsistant.repository, "") == 0)) {
		if (strlen(getenv("HOME"))) {
			g_free_null(tagsistant.repository);
			tagsistant.repository = g_strdup_printf("%s/.tagsistant", getenv("HOME"));
			if (!tagsistant.quiet)
				fprintf(stderr, " Using default repository %s\n", tagsistant.repository);
		} else {
			// tagsistant_usage(tagsistant.progname);
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
			g_free_null(tagsistant.repository);
			tagsistant.repository = g_strdup_printf("%s%s", home_path, relative_path);
			g_free_null(relative_path);
			dbg('b', LOG_INFO, "Repository path is %s", tagsistant.repository);
		} else {
			dbg('b', LOG_ERR, "Repository path starts with '~', but $HOME was not available!");
		}
	} else 

	/* checking if repository is a relative path */
	if (tagsistant.repository[0] != '/') {
		dbg('b', LOG_ERR, "Repository path is relative [%s]", tagsistant.repository);
		char *cwd = getcwd(NULL, 0);
		if (cwd == NULL) {
			dbg('b', LOG_ERR, "Error getting working directory, will leave repository path as is");
		} else {
			gchar *absolute_repository = g_strdup_printf("%s/%s", cwd, tagsistant.repository);
			g_free_null(tagsistant.repository);
			tagsistant.repository = absolute_repository;
			dbg('b', LOG_ERR, "Repository path is %s", tagsistant.repository);
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

	/* tags.sql is also used by getattr() as a guaranteed file when asked for stats/ files */
	struct stat tags_st;
	if (-1 == stat(tagsistant.tags, &tags_st)) {
		int tags_fd = creat(tagsistant.tags, S_IRUSR|S_IWUSR);
		if (tags_fd) close(tags_fd);
	}

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

	dbg('b', LOG_INFO, "Debug is enabled: %s", tagsistant.debug ? "yes" : "no");

	umask(0);

#ifdef _DEBUG_SYSLOG
	tagsistant_init_syslog();
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

	/*
	 * load repository.ini
	 */
	tagsistant_manage_repository_ini();

	dbg('b', LOG_INFO, "Mounting filesystem");

	dbg('b', LOG_INFO, "Fuse options:");
	int fargc = args.argc;
	while (fargc) {
		dbg('b', LOG_INFO, "%.2d: %s", fargc, args.argv[fargc]);
		fargc--;
	}

	/*
	 * initialize db connection, SQL schema,
	 * path_resolution.c and utils.c structures
	 */
	tagsistant_db_init();
	tagsistant_create_schema();
	tagsistant_path_resolution_init();
	tagsistant_reasoner_init();
	tagsistant_utils_init();
	tagsistant_deduplication_init();

	/* SQLite requires tagsistant to run in single thread mode */
	if (tagsistant.sql_database_driver == TAGSISTANT_DBI_SQLITE_BACKEND) {
		tagsistant.singlethread = TRUE;
		fuse_opt_add_arg(&args, "-s");
	}

	/*
	 * print configuration if requested
	 */
	if (tagsistant.show_config) tagsistant_show_config();

	/* add the mount point */
	fuse_opt_add_arg(&args, tagsistant.mountpoint);

	/*
	 * run FUSE main event loop
	 */
	res = tagsistant_fuse_main(&args, &tagsistant_oper);
	fuse_opt_free_args(&args);

	/*
	 * unloading plugins
	 */
	tagsistant_plugin_unloader();

	/* free memory to better perform memory leak profiling */
	g_free_null(tagsistant.dboptions);
	g_free_null(tagsistant.repository);
	g_free_null(tagsistant.archive);
	g_free_null(tagsistant.tags);

	return(res);
}
