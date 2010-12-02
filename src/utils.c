/*
   Tagsistant (tagfs) -- utils.c
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

#include "tagsistant.h"

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
	char *res = g_malloc0(sizeof(char) * (strlen(orig) + 1));
	memcpy(res, orig, strlen(orig));
	if (debugfd != NULL) fprintf(debugfd, "0x%.8x: strdup(%s) @%s:%d\n", (unsigned int) res, orig, file, line);
	return res;
}
#endif

int debug = 0;
int log_enabled = 0;

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

#ifdef MACOSX
ssize_t getline(char **lineptr, size_t *n, FILE *stream)
{
	if (*lineptr == NULL)
		*lineptr = g_malloc0(sizeof(char) * (*n + 1));

	if (*lineptr == NULL)
		return 0;

	if (fgets(*lineptr, *n, stream) == NULL)
		*n = 0;
	else
		*n = strlen(*lineptr);

	return *n;
}
#endif

gchar *querytree_types[QTYPE_TOTAL];
int querytree_types_initialized = 0;

gchar *tagsistant_query_type(querytree_t *qtree)
{
	if (!querytree_types_initialized) {
		querytree_types[QTYPE_MALFORMED] = g_strdup("QTYPE_MALFORMED");
		querytree_types[QTYPE_ROOT] = g_strdup("QTYPE_ROOT");
		querytree_types[QTYPE_ARCHIVE] = g_strdup("QTYPE_ARCHIVE");
		querytree_types[QTYPE_TAGS] = g_strdup("QTYPE_TAGS");
		querytree_types[QTYPE_RELATIONS] = g_strdup("QTYPE_RELATIONS");
		querytree_types[QTYPE_STATS] = g_strdup("QTYPE_STATS");
		querytree_types_initialized++;
	}

	return querytree_types[qtree->type];
}

/*
 * since paths are issued without the tagsistant_id trailing
 * the filename (as in path/path/filename), while after being
 * created inside tagsistant, filenames get the additional ID
 * (as in path/path/3273.filename), an hash table to point
 * original paths to actual paths is required.
 *
 * the aliases hash table gets instantiated inside main() and
 * must be used with tagsistant_get_alias(), tagsistant_set_alias() and tagsistant_delete_alias().
 *
 */
void tagsistant_set_alias(const char *alias, const char *aliased) {
	dbg(LOG_INFO, "Setting alias %s for %s", aliased, alias);
	tagsistant_query("insert into aliases (alias, aliased) values (\"%s\", \"%s\")", NULL, NULL, alias, aliased);
}

gchar *tagsistant_get_alias(const char *alias) {
	gchar *aliased = NULL;
	tagsistant_query("select aliased from aliases where alias = \"%s\"", tagsistant_return_string, &aliased, alias);
	dbg(LOG_INFO, "Looking for an alias for %s, found %s", alias, aliased);
	return aliased;
}

void tagsistant_delete_alias(const char *alias) {
	dbg(LOG_INFO, "Deleting alias for %s", alias);
	tagsistant_query("delete from aliases where alias = \"%s\"", NULL, NULL, alias);
}

