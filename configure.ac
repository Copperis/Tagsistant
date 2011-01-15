#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.63)
AC_INIT([tagsistant],[0.3.20101117],[tx0@strumentiresistenti.org])
AM_INIT_AUTOMAKE(tagsistant, 0.2.20101117)
AC_COPYRIGHT([tagsistant is (C) Tx0 <tx0@strumentiresistenti.org>])
AC_CONFIG_SRCDIR([src/tagsistant.c])
AC_CONFIG_HEADER([config.h])
AC_CANONICAL_TARGET

LT_INIT
# AC_PROG_RANLIB

# Checks for programs.
AC_PROG_CC

# Checks for libraries.

# Checks for header files.
AC_HEADER_DIRENT
AC_HEADER_STDC
AC_CHECK_HEADERS([arpa/inet.h fcntl.h netinet/in.h stddef.h stdlib.h string.h sys/socket.h sys/statfs.h sys/statvfs.h sys/vfs.h syslog.h unistd.h utime.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_TYPE_UID_T
AC_TYPE_MODE_T
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_CHECK_MEMBERS([struct stat.st_blksize])
AC_STRUCT_ST_BLOCKS
AC_CHECK_MEMBERS([struct stat.st_rdev])

# Checks for library functions.
AC_FUNC_CLOSEDIR_VOID
AC_FUNC_LSTAT
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK
AC_FUNC_MALLOC
AC_DIAGNOSE([obsolete],[your code may safely assume C89 semantics that RETSIGTYPE is void.
Remove this warning and the `AC_CACHE_CHECK` when you adjust the code.])dnl
AC_CACHE_CHECK([return type of signal handlers],[ac_cv_type_signal],[AC_COMPILE_IFELSE(
[AC_LANG_PROGRAM([#include <sys/types.h>
#include <signal.h>
],
		 [return *(signal (0, 0)) (0) == 1;])],
		   [ac_cv_type_signal=int],
		   [ac_cv_type_signal=void])])
AC_DEFINE_UNQUOTED([RETSIGTYPE],[$ac_cv_type_signal],[Define as the return type of signal handlers
		    (`int' or `void').])

AC_FUNC_STAT
AC_FUNC_UTIME_NULL
AC_CHECK_FUNCS([inet_ntoa lchown mempcpy memset mkdir rmdir setenv socket strdup strerror utime])

dnl AC_SUBST([CFLAGS],["${CFLAGS} -g -O0 -D_FILE_OFFSET_BITS=64 -Wall -W"])
AC_SUBST([CFLAGS],["${CFLAGS} -D_FILE_OFFSET_BITS=64 -Wall -W"])

# add option for debugging on console instead of through syslog facility
debug_stderr=no
AC_ARG_ENABLE(
	[debug-stderr],
	[AS_HELP_STRING([--enable-debug-stderr], [print debugging statements on stderr instead of using syslog @<:@default=syslog@:>@])],
	[debug_stderr=yes]
)
if test [ "x$debug_stderr" == "xyes" ]; then
	AC_SUBST([CFLAGS],["-D_DEBUG_STDERR ${CFLAGS}"])
else
	AC_SUBST([CFLAGS],["-D_DEBUG_SYSLOG ${CFLAGS}"])
fi

# add option for debugging calls to free()
debug_free=no
AC_ARG_ENABLE(
	[debug-free],
	[AS_HELP_STRING([--enable-debug-free], [double check before free()ing a symbol @<:@default=no@:>@])],
	[debug_free=yes]
)
if test [ "x$debug_free" == "xyes" ]; then
	AC_SUBST([CFLAGS],["-D_DEBUG_FREE_CALLS ${CFLAGS}"])
fi

### # add profiling with cprof --- BROKENNNNN!!!!
### # simply compiling with -lcprof -finstrument-functions makes
### # programs segv before executing main!
###
### cprofiler=no
### AC_ARG_ENABLE(
### 	[cprofiler],
### 	[AS_HELP_STRING([--enable-cprofiler], [enable profiling using cprof @<:@default=no@:>@])],
### 	[cprofiler=yes]
### )
### if test [ "x$cprofiler" == "xyes" ]; then
### 	AC_SUBST([CFLAGS],["-D_FILE_OFFSET_BITS=64 -lcprof -finstrument-functions"])
### fi

AC_CHECK_LIB([fuse],[fuse_main],,[
	echo "FUSE (Filesystem in USErspace) it's a great user space library"
	echo "which allows linux kernel users to create filesystem implementations"
	echo "without playing with complex kernel internals. tagsistant is entirely based"
	echo "on FUSE. Please install libfuse-dev package (or the one which best"
	echo "fits your linux distribution) or download and compile sources at:"
	echo "http://fuse.sourceforge.net/"
	exit 1
])

### AC_CHECK_LIB([sqlite3],[sqlite3_open],,[
### 	echo "SQLite it's an SQL implementation targeted to small and embedded systems."
### 	echo "SQL database is kept inside a single file. Memory resources required are"
### 	echo "less than normal client/server implementations. tagsistant use SQLite to store"
### 	echo "file<->tag relationship. For more informations please visit:"
### 	echo "http://www.sqlite.org/"
### 	exit 1
### ])

AC_CHECK_LIB([dbi],[dbi_conn_new],,[
	echo "DBI is an generic database abstactio layer used to connect to MySQL"
	echo "and will probably used to connect to SQLite too in future releases"
	echo "http://libdbi.sourceforge.net/"
	exit 1
])

echo $host .. $target
case $host in
	*-*-linux*)
		echo "Live long and prosper, GNU/Linux"
		;;
	*-*-darwin*)
		AC_SUBST([CFLAGS],["${CFLAGS} -D__FreeBSD__=10 -DMACOSX -DFUSE_USE_VERSION=25"])
		AC_SUBST([LDFLAGS],["${LDFLAGS} -flat_namespace -force_flat_namespace"])
		;;
esac

AC_CONFIG_FILES([Makefile
                 src/Makefile])
AC_OUTPUT
