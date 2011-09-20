/*
   Tagsistant (tagfs) -- tp_html.c
   Copyright (C) 2006-2009 Tx0 <tx0@strumentiresistenti.org>

   Tagsistant html plugin which makes decisions on file MIME types.

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
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

/* declaring mime type */
char mime_type[] = "text/html";

/* flags used by regex compiler */
#define _RX_COMPILE_FLAGS G_REGEX_CASELESS|G_REGEX_MULTILINE|G_REGEX_DOTALL|G_REGEX_EXTENDED|G_REGEX_OPTIMIZE

/* static mutex used by processor */
GStaticMutex processor_mutex = G_STATIC_MUTEX_INIT;

GRegex *_rx_title = NULL;
GRegex *_rx_keywords = NULL;

/* exported init function */
int plugin_init()
{
	/* intialize regular expressions */
	_rx_title = g_regex_new("<title>([^<]+)</title>", _RX_COMPILE_FLAGS, 0, NULL);
	_rx_keywords = g_regex_new("<meta name=[\"']keywords['\"] content=['\"]([^'\"]+)[\"']/?>", _RX_COMPILE_FLAGS, 0, NULL);

	return 1;
}

void tp_html_appy_regex(const tagsistant_querytree_t *qtree, const char *buf, GRegex *rx)
{
	GMatchInfo *match_info;

	/* apply the regex */
	g_static_mutex_lock(&processor_mutex);
	g_regex_match(rx, buf, 0, &match_info);
	g_static_mutex_unlock(&processor_mutex);

	/* process the matched entries */
	while (g_match_info_matches(match_info)) {
		gchar *raw = g_match_info_fetch(match_info, 1);
		dbg(LOG_INFO, "Found raw data: %s", raw);

		gchar **tokens = g_strsplit_set(raw, " \t,.!?", 255);
		g_free(raw);

		int x = 0;
		while (tokens[x]) {
			if (strlen(tokens[x]) >= 3) sql_tag_object(tokens[x], qtree->object_id);
			x++;
		}

		g_strfreev(tokens);

		g_match_info_next(match_info, NULL);
	}
}

/* exported processor function */
int processor(const tagsistant_querytree_t *qtree)
{
	/* default tagging */
	sql_tag_object("document", qtree->object_id);
	sql_tag_object("webpage", qtree->object_id);
	sql_tag_object("html", qtree->object_id);

	/* apply regular expressions to document content */

	/* 1. open document */
	int fd = open(qtree->full_archive_path, 0);
	if (-1 == fd) {
		int error = errno;
		dbg(LOG_ERROR, "Unable to open %s: %s", qtree->full_archive_path, strerror(error));
		return TP_ERROR;
	}

	/* 2. read 64K of document content */
	char buf[65535];
	int r = read(fd, buf, 65535);
	(void) r;

	/* 3. look for matching portions */
	tp_html_apply_regex(qtree, buf, _rx_title);
	tp_html_apply_regex(qtree, buf, _rx_keywords);

	return TP_STOP;
}

/* exported finalize function */
void plugin_free()
{
	/* unreference regular expressions */
	g_regex_unref(_rx_title);
}

/* vim:ts=4:autoindent:nocindent:syntax=c */
