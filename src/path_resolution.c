/*
   TAGFS -- path_resolution.h
   Copyright (C) 2006-2007 Tx0 <tx0@autistici.org>

   Transform paths in queries and apply queries to file sets to
   grep files matching with queries.

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

char *get_tag_name(const char *path)
{
	char *idx = rindex(path, '/');
	idx++;
	char *tagname = strdup(idx);
	return tagname;
}

char *get_tag_path(const char *tag)
{
	char *tagpath = malloc(strlen(tagfs.tagsdir) + strlen(tag) + 1);
	if (tagpath == NULL) {
		dbg(LOG_ERR, "error allocating memory in get_tag_path");
		return NULL;
	}
	strcpy(tagpath,tagfs.tagsdir);
	strcat(tagpath,tag);
	return tagpath;
}

char *get_file_path(const char *tag)
{
	char *filepath = malloc(strlen(tagfs.archive) + strlen(tag) + 1);
	if (filepath == NULL) {
		dbg(LOG_ERR, "error allocating memory in get_file_path");
		return NULL;
	}
	strcpy(filepath,tagfs.archive);
	strcat(filepath,tag);
	return filepath;
}

#ifdef OBSOLETE_CODE
char *get_tmp_file_path(const char *tag)
{
	char *filepath = malloc(strlen(tagfs.tmparchive) + strlen(tag) + 1);
	if (filepath == NULL) {
		dbg(LOG_ERR, "error allocating memory in get_tmp_file_path");
		return NULL;
	}
	strcpy(filepath,tagfs.tmparchive);
	strcat(filepath,tag);
	return filepath;
}
#endif /* OBSOLETE_CODE */

/**
 * build tree from path, organizing relationship based on operators
 *
 * \param @path path to parse
 */
path_tree_t *build_pathtree(const char *path)
{
	char *pcopy = strdup(path);
	dbg(LOG_INFO, "Building tree for path %s", path);

	path_tree_t *pt = malloc(sizeof(path_tree_t));
	if (pt == NULL) {
		dbg(LOG_ERR, "Can't allocate space for path_tree_t node");
		return NULL;
	}
	pt->name = NULL;
	pt->AND = pt->OR = NULL;
	pcopy++; /* skip first slash */

	if ( *pcopy == '\0' ) return pt; /* path end reached */

	char *idx = index(pcopy, '/');
	if (idx == NULL) {
		pt->name = strdup(pcopy);
		dbg(LOG_INFO, "Next element is %s [%d]", pt->name, strlen(pt->name));
		dbg(LOG_INFO, "Path end reached");
		return pt;
	}

	pt->name = strndup(pcopy, idx - pcopy);

	dbg(LOG_INFO, "Next element is %s [%d]", pt->name, strlen(pt->name));

	idx++; /* skip slash after the name */

	if ( *idx == '\0' ) return pt; /* path end reached */

	dbg(LOG_INFO, "More path to parse: %s", idx);

	if ( strncasecmp(idx, "OR", 2) == 0 ) {
		dbg(LOG_INFO, "Continuing for an OR!");
		idx += strlen("OR");
		pt->OR = build_pathtree(idx);
	} else if ( strncasecmp(idx, "AND", 3) == 0 ) {
		dbg(LOG_INFO, "Continuing for an AND!");
		idx += strlen("AND");
		pt->AND = build_pathtree(idx);
	} else {
		dbg(LOG_INFO, "Should be just a file name: %s", idx);
	}

	return pt;
}

void destroy_path_tree(path_tree_t *pt)
{
	if (pt == NULL)
		return;
	
	if (pt->name != NULL)
		free(pt->name);
	
	if (pt->AND != NULL)
		destroy_path_tree(pt->AND);

	if (pt->OR != NULL)
		destroy_path_tree(pt->OR);
}

#define ctx_AND 1
#define ctx_OR  2

int check_in_pathtree(char *filename, path_tree_t *pt)
{
	path_tree_t *ptx = pt;
	int result = 1;

	dbg(LOG_INFO, "Checking if %s is to be served", filename);
	while (ptx != NULL) {
		char *dirpath = get_tag_path(ptx->name);
		char *inpath = malloc(strlen(dirpath) + strlen(filename) + 2);

		if (inpath != NULL) {
			strcpy(inpath, dirpath);
			strcat(inpath, "/");
			strcat(inpath, filename);
			dbg(LOG_INFO, " ** Checking %s", inpath);

			struct stat st;
			if (stat(inpath, &st) == -1) {
				dbg(LOG_INFO, " xx %s not found!", inpath);
				/* skip to next OR */
				while ( ptx->AND != NULL ) {
					dbg(LOG_INFO, "Skipping next AND: %s", ptx->AND->name);
					ptx = ptx->AND;
				}
				ptx = ptx->OR;
				result = 0;
			} else {
				dbg(LOG_INFO, "%s exists!", inpath);
				/* skip to next AND */
				while ( ptx->OR != NULL ) {
					dbg(LOG_INFO, "Skipping next OR: %s", ptx->OR->name);
					ptx = ptx->OR;
				}
				ptx = ptx->AND;
				result = 1;
			}
		} else {
			dbg(LOG_ERR, "Can't allocate room for inpath!");
		}

		free(inpath);
		free(dirpath);
	}

	return result;
}

file_handle_t *build_filetree(path_tree_t *pt)
{
	file_handle_t *fh = malloc(sizeof(file_handle_t));
	if ( fh == NULL ) {
		dbg(LOG_ERR, "Can't allocate memory in build_filetree");
		return NULL;
	}
	fh->next = NULL;
	fh->name = NULL;

	file_handle_t *result = fh;

	if (pt == NULL) {
		dbg(LOG_ERR, "NULL path_tree_t object provided to build_filetree");
		return NULL;
	}

	/*
	 * externally we traverse the tree to get all
	 * the directory names to be opendir() to catch all
	 * candidate files
	 */
	path_tree_t *ptextern = pt;
	while (ptextern != NULL) {
		char *dir = get_tag_path(ptextern->name);
		DIR *dh;
		if ((dh = opendir(dir)) != NULL) {
			struct dirent *de;
			while ( (de = readdir(dh)) != NULL ) {
				/* catch each file in this directory */
				if ((strcmp(de->d_name,".")!=0) && (strcmp(de->d_name,"..")!=0)) {

					/*
					 * internally we traverse the tree to check
					 * if file in de->d_name fulfill all requirements
					 * in logical path.
					 *
					 * to simplify code, this nested level is resolved
					 * in separate function check_in_pathtree()
					 */
					if (check_in_pathtree(de->d_name, pt)) {
						/* avoid duplicates */
						int exists = 0;
						file_handle_t *uniq_fh = result;
						while (uniq_fh != NULL) {
							if (uniq_fh->name == NULL)
								break;
							if (strcmp(uniq_fh->name, de->d_name) == 0) {
								exists = 1;
								break;
							}
							uniq_fh = uniq_fh->next;
							dbg(LOG_INFO, "*****");
						}

						/* name is not in cache */
						if (!exists) {
							fh->name = strdup(de->d_name);
							dbg(LOG_INFO, "adding %s to filetree", fh->name);
							fh->next = malloc(sizeof(file_handle_t));
							if (fh->next == NULL) {
								dbg(LOG_ERR, "Can't allocate memory in build_filetree");
								closedir(dh);
								free(dir);
								return result;
							}
							fh = fh->next;
							fh->next = NULL;
							fh->name = NULL;
						}
					}
				}
			}
			closedir(dh);
		}
		if (ptextern->AND != NULL)
			ptextern = ptextern->AND;
		else
			ptextern = ptextern->OR;
	}

	return result;
}

void destroy_file_tree(file_handle_t *fh)
{
	if (fh == NULL)
		return;
	
	if (fh->name != NULL)
		free(fh->name);
	
	if (fh->next != NULL)
		destroy_file_tree(fh->next);
}

// vim:ts=4:nowrap:nocindent
