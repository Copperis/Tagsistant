/*
   Tagsistant (tagfs) -- plugin.c
   Copyright (C) 2006-2009 Tx0 <tx0@strumentiresistenti.org>

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

	char *ext_space = g_strdup_printf("%s ", ext); /* trailing space is used later in matching */

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
					type = g_strdup(line);
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
int process(int file_id)
{
	int res = 0, process_res = 0;

	// load the filename from the database
	char *filename = NULL;
	tagsistant_query("select objectname from objects where object_id = %d", return_string, &filename, file_id);
	if (filename == NULL) {
		dbg(LOG_INFO, "process() unable to locate filename with id %u", file_id);
		return 0;
	}

	dbg(LOG_INFO, "Processing file %s", filename);

	char *mime_type = get_file_mimetype(filename);

	if (mime_type == NULL) {
		dbg(LOG_ERR, "process() wasn't able to guess mime type for %s", filename);
		return 0;
	}

	char *mime_generic = g_strdup(mime_type);
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

void plugin_loader()
{
	char *tagsistant_plugins = NULL;
	if (getenv("TAGSISTANT_PLUGINS") != NULL) {
		tagsistant_plugins = g_strdup(getenv("TAGSISTANT_PLUGINS"));
		if (!tagsistant.quiet)
			fprintf(stderr, " Using user defined plugin dir: %s\n", tagsistant_plugins);
	} else {
		tagsistant_plugins = g_strdup(PLUGINS_DIR);
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
				tagsistant_plugin_t *plugin = g_new0(tagsistant_plugin_t, 1);

				char *pname = g_strdup_printf("%s/%s", tagsistant_plugins, de->d_name);

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
							plugin->filename = g_strdup(de->d_name);
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

	freenull(tagsistant_plugins);
}

void plugin_unloader()
{
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
}
