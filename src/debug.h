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
	if (!tagsistant.quiet && (!strstr(string, "SQL") || tagsistant.verbose)) {\
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

#ifdef TAGSISTANT_TIME_PROFILE
#include <sys/time.h>
#include <time.h>
#define init_time_profile() struct timeval tv_start, tv_stop, result;
#define start_time_profile() gettimeofday(&tv_start, NULL);
#define stop_time_profile() {\
	gettimeofday(&tv_stop, NULL);\
	timeval_subtract(&result, &tv_stop, &tv_start);\
	dbg(LOG_INFO, "time_profile: Started at %ld, took %ld.%.6ld",\
		(long int) tv_start.tv_sec, (long int) result.tv_sec, (long int) result.tv_usec);\
}
#define stop_labeled_time_profile(label) {\
	gettimeofday(&tv_stop, NULL);\
	timeval_subtract(&result, &tv_stop, &tv_start);\
	dbg(LOG_INFO, "time_profile: [%s] Started at %ld, took %ld.%.6ld", label,\
		(long int) tv_start.tv_sec, (long int) result.tv_sec, (long int) result.tv_usec);\
}
#else
#define init_time_profile() {}
#define start_time_profile() {}
#define stop_time_profile() {}
#define stop_labeled_time_profile(label) {}
#endif

extern int tagsistant_debug;

#define oldfree(symbol) assert(symbol != NULL); dbg(LOG_INFO, "g_free(%s)", __STRING(symbol)); g_free(symbol);
#define befree(symbol) {\
	if (\
		(strcmp(__STRING(symbol),"myself") == 0) ||\
		(strcmp(__STRING(symbol),"&myself") == 0) ||\
		(strcmp(__STRING(symbol),"owner") == 0) ||\
		(strcmp(__STRING(symbol),"node") == 0) \
	) {\
		dbg(LOG_INFO, "Trying to g_free(%s) which is probably wrong!", __STRING(symbol));\
	} else {\
		dbg(LOG_INFO, "g_free(%s)", __STRING(symbol));\
		assert(symbol != NULL);\
		g_free(symbol);\
	}\
}

#define strlen(string) ((string == NULL) ? 0 : strlen(string))
extern int strlen0(const char *string);

// vim:ts=4
