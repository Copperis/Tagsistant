/*
   Tagsistant (tagfs) -- debug.h
   Copyright (C) 2006 Tx0 <tx0@strumentiresistenti.org>

   Some debug code.

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

#include <assert.h>
#include <errno.h>
#include <string.h>

#undef _DEBUG_STDERR

#ifdef _DEBUG_SYSLOG
#include <syslog.h>
#define dbg(facility, string, ...) {\
	if (!tagsistant.quiet) {\
		gchar *line = g_strdup_printf(string " [@%s:%d]", ##__VA_ARGS__, __FILE__, __LINE__);\
		syslog(facility, line);\
		g_free(line);\
	}\
}
#endif

#ifdef _DEBUG_STDERR
#include <stdio.h>
#define dbg(facility,string,...) {\
	fprintf(stderr,"TS> ");\
	if ((*string != '/') && (*string != '\\')) fprintf(stderr,"| ");\
	fprintf(stderr,string, ##__VA_ARGS__);\
	fprintf(stderr," [@%s:%d]\n", __FILE__, __LINE__);\
	if (*string == '\\') fprintf(stderr,"TS> \n");\
}
#endif

extern int tagsistant_debug;

#define strlen(string) ((string == NULL) ? 0 : strlen(string))

// vim:ts=4
