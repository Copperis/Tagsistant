/*
   Tagsistant (tagfs) -- plugin.c
   Copyright (C) 2006-2013 Tx0 <tx0@strumentiresistenti.org>

   Tagsistant (tagfs) plugin support

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

/******************\
 * PLUGIN SUPPORT *
\******************/

#if EXTRACTOR_VERSION & 0x00050000      // libextractor 0.5.x
static EXTRACTOR_ExtractorList *elist;
#elif EXTRACTOR_VERSION & 0x00060000    // libextractor 0.6.x
static EXTRACTOR_ExtractorList *elist;
#endif

static GRegex *tagsistant_rx_date;

#ifndef errno
#define errno
#endif

/**
 * process a file using plugin chain
 *
 * \param filename file to be processed (just the name, will be looked up in /archive)
 * \return(zero on fault, one on success)
 */
int tagsistant_process(tagsistant_querytree *qtree)
{
#if EXTRACTOR_VERSION & 0x00050000 // libextractor 0.5.x
	int res = 0;
	const gchar *mime_type = NULL;
	gchar *mime_generic = NULL;

	dbg('p', LOG_INFO, "Processing file %s", qtree->full_archive_path);

	/*
	 * Extract the keywords and remove duplicated ones
	 */
	EXTRACTOR_KeywordList *keywords = EXTRACTOR_getKeywords(elist, qtree->full_archive_path);
	keywords = EXTRACTOR_removeDuplicateKeywords (keywords, 0);

	/*
	 *  loop through the keywords extracted to get the MIME type
	 */
	EXTRACTOR_KeywordList *keyword_pointer = keywords;
	while (keyword_pointer) {
		if (EXTRACTOR_MIMETYPE == keyword_pointer->keywordType) {
			mime_type = EXTRACTOR_getKeywordTypeAsString(keyword_pointer->keywordType);
			break;
		}
		keyword_pointer = keyword_pointer->next;
	}

	/* tag by date */
	const gchar *date = tagsistant_plugin_get_keyword_value("date", keywords);
	if (date) tagsistant_plugin_tag_by_date(qtree, date);

	/*
	 * guess the generic MIME type
	 */
	mime_generic = g_strdup(mime_type);
	gchar *slash = index(mime_generic, '/');
	if (slash) {
		slash++; *slash = '*';
		slash++; *slash = '\0';
	}

	/*
	 *  apply plugins in order
	 */
	tagsistant_plugin_t *plugin = tagsistant.plugins;
	while (plugin != NULL) {
		if (
			(strcmp(plugin->mime_type, mime_type) == 0) ||
			(strcmp(plugin->mime_type, mime_generic) == 0) ||
			(strcmp(plugin->mime_type, "*/*") == 0)
		) {
			/* call plugin processor */
			dbg('p', LOG_INFO, "Applying plugin %s", plugin->filename);
			res = (plugin->processor)(qtree, keywords);

			/* report about processing */
			switch (res) {
				case TP_ERROR:
					dbg('p', LOG_ERR, "Plugin %s was supposed to apply to %s, but failed!", plugin->filename, qtree->full_archive_path);
					break;
				case TP_OK:
					dbg('p', LOG_INFO, "Plugin %s tagged %s", plugin->filename, qtree->full_archive_path);
					break;
				case TP_STOP:
					dbg('p', LOG_INFO, "Plugin %s stopped chain on %s", plugin->filename, qtree->full_archive_path);
					goto STOP_CHAIN_TAGGING;
					break;
				case TP_NULL:
					dbg('p', LOG_INFO, "Plugin %s did not tagged %s", plugin->filename, qtree->full_archive_path);
					break;
				default:
					dbg('p', LOG_ERR, "Plugin %s returned unknown result %d", plugin->filename, res);
					break;
			}
		}
		plugin = plugin->next;
	}

STOP_CHAIN_TAGGING:

//	g_free_null(mime_type); /* mustn't be freed because it's static code from libextractor */
	g_free_null(mime_generic);

	dbg('p', LOG_INFO, "Processing of %s ended.", qtree->full_archive_path);

	/* free the keyword structure */
	EXTRACTOR_freeKeywords(keywords);

	return(res);
#else
	return(0);
#endif
}

/**
 * Iterate over a set of keywords. For each keyword matching
 * regular expression regex, construct a tag as "keyword_name:keyword_value"
 * and tags the qtree object.
 *
 * @param qtree the querytree object to tag
 * @param keyworkd a EXTRACTOR_KeywordList * list of keywords
 * @param regex a precompiled GRegex object to match against each keyword
 */
void tagsistant_plugin_iterator(const tagsistant_querytree *qtree, EXTRACTOR_KeywordList *keywords, GRegex *regex)
{
	/*
	 * loop through the keywords to tag the file
	 */
	EXTRACTOR_KeywordList *keyword_pointer = keywords;
	while (keyword_pointer) {

		/* if the keyword name matches the filter regular expression... */
		if (g_regex_match(regex, EXTRACTOR_getKeywordTypeAsString(keyword_pointer->keywordType), 0, NULL)) {

			/* ... build a tag which is "keyword_name:keyword_value" ... */
			gchar *tag = g_strdup_printf("%s:%s",
				EXTRACTOR_getKeywordTypeAsString(keyword_pointer->keywordType),
				keyword_pointer->keyword);

			/* ... turn each slash and space in a dash */
			gchar *tpointer = tag;
			while (*tpointer) {
				if (*tpointer == '/' || *tpointer == ' ') *tpointer = '-';
				tpointer++;
			}

			/* ... then tag the file ... */
			tagsistant_sql_tag_object(qtree->dbi, tag, qtree->inode);

			/* ... cleanup and step over */
			g_free_null(tag);
			keyword_pointer = keyword_pointer->next;
		}
	}
}

/**
 * Return the value of a keyword from a keyword list, if available.
 *
 * @param keyword the keyword to fetch
 * @param keywords the linked list of EXTRACTOR_KeywordList object
 * @return the value of the keyword. This is a pointer to the original value and must not be modified or freed
 */
const gchar *tagsistant_plugin_get_keyword_value(gchar *keyword, EXTRACTOR_KeywordList *keywords)
{
	EXTRACTOR_KeywordList *keyword_pointer = keywords;
	while (keyword_pointer) {
		if (g_strcmp0(keyword, EXTRACTOR_getKeywordTypeAsString(keyword_pointer->keywordType)) == 0) {
			return (keyword_pointer->keyword);
		}
		keyword_pointer = keyword_pointer->next;
	}
	return (NULL);
}

/**
 * Tag a querytree by date.
 *
 * @param qtree the querytree object to tag
 * @param date a constant string in format "YYYY:MM:DD HH:MM:SS"
 */
void tagsistant_plugin_tag_by_date(const tagsistant_querytree *qtree, const gchar *date)
{
	GMatchInfo *match_info;
	GError *error;

	if (g_regex_match_full(tagsistant_rx_date, date, -1, 0, 0, &match_info, &error)) {
		gchar *tag = g_strdup_printf("year:%s", g_match_info_fetch(match_info, 1));
		tagsistant_sql_tag_object(qtree->dbi, tag, qtree->inode);
		g_free(tag);

		tag = g_strdup_printf("month:%s", g_match_info_fetch(match_info, 1));
		tagsistant_sql_tag_object(qtree->dbi, tag, qtree->inode);
		g_free(tag);

		tag = g_strdup_printf("day:%s", g_match_info_fetch(match_info, 1));
		tagsistant_sql_tag_object(qtree->dbi, tag, qtree->inode);
		g_free(tag);

		tag = g_strdup_printf("hour:%s", g_match_info_fetch(match_info, 1));
		tagsistant_sql_tag_object(qtree->dbi, tag, qtree->inode);
		g_free(tag);

		tag = g_strdup_printf("minute:%s", g_match_info_fetch(match_info, 1));
		tagsistant_sql_tag_object(qtree->dbi, tag, qtree->inode);
		g_free(tag);

#if 0
		tag = g_strdup_printf("second:%s", g_match_info_fetch(match_info, 1));
		tagsistant_sql_tag_object(qtree->dbi, tag, qtree->inode);
		g_free(tag);
#endif

	}

	g_match_info_unref(match_info);
}

/**
 * Loads the plugins
 */
void tagsistant_plugin_loader()
{
#if EXTRACTOR_VERSION & 0x00050000 // libextractor 0.5.x
	elist =  EXTRACTOR_loadDefaultLibraries();
#endif

	/* init some useful regex */
	tagsistant_rx_date = g_regex_new(
		"^([0-9][0-9][0-9][0-9]):([0-9][0-9]):([0-9][0-9]) ([0-9][0-9]):([0-9][0-9]):([0-9][0-9])$",
		TAGSISTANT_RX_COMPILE_FLAGS, 0, NULL);

	char *tagsistant_plugins = NULL;
	if (getenv("TAGSISTANT_PLUGINS") != NULL) {
		tagsistant_plugins = g_strdup(getenv("TAGSISTANT_PLUGINS"));
		if (!tagsistant.quiet) fprintf(stderr, " Using user defined plugin dir: %s\n", tagsistant_plugins);
	} else {
		tagsistant_plugins = g_strdup(PLUGINS_DIR);
		if (!tagsistant.quiet) fprintf(stderr, " Using default plugin dir: %s\n", tagsistant_plugins);
	}

	struct stat st;
	if (lstat(tagsistant_plugins, &st) == -1) {
		if (!tagsistant.quiet)
			fprintf(stderr, " *** error opening directory %s: %s ***\n", tagsistant_plugins, strerror(errno));
	} else if (!S_ISDIR(st.st_mode)) {
		if (!tagsistant.quiet)
			fprintf(stderr, " *** error opening directory %s: not a directory ***\n", tagsistant_plugins);
	} else {
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

				/*
				 * file is a tagsistant plugin (beginning by right prefix) and
				 * is processed allocate new plugin object
				 */
				tagsistant_plugin_t *plugin = g_new0(tagsistant_plugin_t, 1);

				if (NULL == plugin) {
					dbg('p', LOG_ERR, "Error allocating plugin object");
					continue;
				}

				char *pname = g_strdup_printf("%s/%s", tagsistant_plugins, de->d_name);

				/* load the plugin */
				plugin->handle = dlopen(pname, RTLD_NOW/* |RTLD_GLOBAL */);
				if (plugin->handle == NULL) {
					if (!tagsistant.quiet)
						fprintf(stderr, " *** error dlopen()ing plugin %s: %s ***\n", de->d_name, dlerror());
					g_free_null(plugin);
				} else {
					/* search for init function and call it */
					int (*init_function)() = NULL;
					init_function = dlsym(plugin->handle, "tagsistant_plugin_init");
					if (init_function != NULL) {
						// TODO valgrind says: check for leaks
						int init_res = init_function();
						if (!init_res) {
							/* if init failed, ignore this plugin */
							dbg('p', LOG_ERR, " *** error calling plugin_init() on %s ***\n", de->d_name);
							g_free_null(plugin);
							continue;
						}
					}

					/* search for MIME type string */
					plugin->mime_type = dlsym(plugin->handle, "mime_type");
					if (plugin->mime_type == NULL) {
						if (!tagsistant.quiet)
							fprintf(stderr, " *** error finding %s processor function: %s ***\n", de->d_name, dlerror());
						g_free_null(plugin);
					} else {
						/* search for processor function */
						plugin->processor = dlsym(plugin->handle, "tagsistant_processor");
						if (plugin->processor == NULL) {
							if (!tagsistant.quiet)
								fprintf(stderr, " *** error finding %s processor function: %s ***\n", de->d_name, dlerror());
							g_free_null(plugin);
						} else {
							plugin->free = dlsym(plugin->handle, "tagsistant_plugin_free");
							if (plugin->free == NULL) {
								if (!tagsistant.quiet)
									fprintf(stderr, " *** error finding %s free function: %s (still registering the plugin) ***", de->d_name, dlerror());
							}

							/* add this plugin on queue head */
							plugin->filename = g_strdup(de->d_name);
							plugin->next = tagsistant.plugins;
							tagsistant.plugins = plugin;
							if (!tagsistant.quiet)
								fprintf(stderr, " Loaded plugin: %20s -> %s\n", plugin->mime_type, plugin->filename);
						}
					}
				}
				g_free_null(pname);
			}
			closedir(p);
		}
	}

	g_free_null(tagsistant_plugins);
}

/**
 * Unloads all the plugins and disposes the regular expressions
 */
void tagsistant_plugin_unloader()
{
	/* unregistering plugins */
	tagsistant_plugin_t *pp = tagsistant.plugins;
	tagsistant_plugin_t *ppnext = pp;
	while (pp != NULL) {
		/* call plugin free method to let it free allocated resources */
		if (pp->free != NULL) {
			(pp->free)();
		}
		g_free_null(pp->filename);	/* free plugin filename */
		dlclose(pp->handle);		/* unload the plugin */
		ppnext = pp->next;			/* save next plugin in tagsistant chain */
		g_free_null(pp);			/* free this plugin entry in tagsistant chain */
		pp = ppnext;				/* point to next plugin in tagsistant chain */
	}

	g_regex_unref(tagsistant_rx_date);
}

/**
 * Apply a regular expression to a buffer (first N bytes of a file) and use each
 * matched token to tag the object
 *
 * @param qtree the querytree object to be tagged
 * @param buf the text to be matched by the regex
 * @param m a mutex to protect the regular expression
 * @param rx the regular expression
 */
void tagsistant_plugin_apply_regex(const tagsistant_querytree *qtree, const char *buf, GMutex *m, GRegex *rx)
{
	GMatchInfo *match_info;

	/* apply the regex, locking the mutex if provided */
	if (NULL != m) g_mutex_lock(m);
	g_regex_match(rx, buf, 0, &match_info);
	if (NULL != m) g_mutex_unlock(m);

	/* process the matched entries */
	while (g_match_info_matches(match_info)) {
		gchar *raw = g_match_info_fetch(match_info, 1);
		dbg('p', LOG_INFO, "Found raw data: %s", raw);

		gchar **tokens = g_strsplit_set(raw, " \t,.!?/", 255);
		g_free_null(raw);

		int x = 0;
		while (tokens[x]) {
			if (strlen(tokens[x]) >= 3) tagsistant_sql_tag_object(qtree->dbi, tokens[x], qtree->inode);
			x++;
		}

		g_strfreev(tokens);

		g_match_info_next(match_info, NULL);
	}
}

