/*
   Tagsistant (tagfs) -- tp_jpeg.c
   Copyright (C) 2006-2009 Tx0 <tx0@strumentiresistenti.org>

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

#include "tagsistant.h"
#include <stdio.h>
#include <string.h>
#include <libexif/exif-data.h>
#define DEFAULT_TAG "image"

/* declaring mime type */
char mime_type[] = "image/jpeg";

/* exported init function */
int plugin_init()
{
	return 1;
}

/*
static void trim_spaces(char *buf) 
	char *s = buf-1; 
	for (; *buf; ++buf) {
		if (*buf != ' ')
			s = buf;
	}
	*++s = 0;
}
*/

void add_tag(const tagsistant_querytree_t *qtree, ExifData *ed, unsigned tag)
{
	ExifMnoteData *mn = exif_data_get_mnote_data(ed);
	if (!mn) return;

	int num = exif_mnote_data_count(mn);
	int i; 

	for (i=0; i < num; ++i) { 
		const char* tname = exif_tag_get_name_in_ifd (exif_mnote_data_get_id(mn, i), EXIF_SUPPORT_LEVEL_UNKNOWN);
		dbg(LOG_INFO, "Found tag %s", tname);

		/*
		char buf[1024]; 
		if (exif_mnote_data_get_id(mn, i) == tag) {
			if (exif_mnote_data_get_value(mn, i, buf, sizeof(buf))) { 
				// trim_spaces(buf);
				if (*buf) {
					dbg(LOG_INFO, "%u: Found tag %s", qtree->object_id, buf);
				}
			}
		}
		*/
	}
}

/* exported processor function */
int processor(const tagsistant_querytree_t *qtree)
{
	dbg(LOG_INFO, "Tagging object %u as %s", qtree->object_id, DEFAULT_TAG);
	sql_tag_object(DEFAULT_TAG, qtree->object_id);

	ExifData *ed = exif_data_new_from_file(qtree->full_archive_path);
	if (!ed) return TP_STOP;

	add_tag(qtree, ed, EXIF_TAG_ARTIST);

	exif_data_unref(ed); 
	return TP_STOP;
}

/* exported finalize function */
void plugin_free()
{
}

// vim:ts=4:autoindent:nocindent:syntax=c
