/*
   Tagsistant (tagfs) -- tp_jpeg.c
   Copyright (C) 2006-2013 Tx0 <tx0@strumentiresistenti.org>

   Tagsistant jpeg plugin which makes decisions on file MIME types.

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

#include "../tagsistant.h"
#include <stdio.h>
#include <string.h>
#include <libexif/exif-data.h>
#define DEFAULT_TAG "image"

/* declaring mime type */
char mime_type[] = "image/jpeg";

/* exported init function */
int tagsistant_plugin_init()
{
	return(1);
}

#define leave_processor() {\
	exif_mnote_data_unref(mn);\
	exif_data_unref(ed);\
	return(TP_STOP);\
}

/* exported processor function */
int tagsistant_processor(const tagsistant_querytree *qtree)
{
	ExifData *ed = NULL;
	ExifMnoteData *mn = NULL;

	// doing basic tagging
	tagsistant_sql_tag_object(qtree->dbi, DEFAULT_TAG, qtree->inode);

	// doing extended tagging using libexif
	ed = exif_data_new_from_file(qtree->full_archive_path);
	if (!ed) leave_processor();

	mn = exif_data_get_mnote_data(ed);
	if (!mn) leave_processor();

	// loop through tags
	int tag_count = exif_mnote_data_count(mn);
	int i;
	for (i = 0; i < tag_count; ++i) {
		char buf[1024];
		gchar *exiftag = g_strdup_printf("%s:%s", exif_mnote_data_get_title(mn, i), exif_mnote_data_get_value(mn, i, buf, 1024));
		g_strstrip(exiftag);
		char *c = exiftag;
		while (*c != '\0') {
			if (*c == ' ') {
				*c = '_';
			}
			c++;
		}

		tagsistant_sql_tag_object(qtree->dbi, exiftag, qtree->inode);
		g_free_null(exiftag);
	}

	// ok
	leave_processor();

	return( TP_STOP);
}

/* exported finalize function */
void tagsistant_plugin_free()
{
}

// vim:ts=4:autoindent:nocindent:syntax=c
