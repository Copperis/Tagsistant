/*
   Tagsistant (tagfs) -- tp_generic.c
   Copyright (C) 2006-2009 Tx0 <tx0@strumentiresistenti.org>

   Tagsistant generic plugin which makes decisions on file MIME types.

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

/* declaring mime type */
char mime_type[] = "*/*";

/* exported init function */
int tagsistant_plugin_init()
{
	/* dbg(LOG_INFO, "Plugin generic loaded, nice to meet you!"); */
	return 1;
}

/* exported processor function */
int tagsistant_processor(const char *filename)
{
	(void) filename;
	return TP_NULL;
}

/* exported finalize function */
void tagsistant_plugin_free()
{
	/* dbg(LOG_INFO, "Plugin generic gets unloaded, see you!"); */
}

// vim:ts=4:autoindent:nocindent:syntax=c
