#include "../../tagsistant.h"

#define DUPLICATE_DEBUG 1

#define sb(msg, ...) {\
	if (DUPLICATE_DEBUG) {\
		fprintf(stderr, "  [");\
		fprintf(stderr, msg, ## __VA_ARGS__);\
		fprintf(stderr, "]\n");\
	}\
	GtkStatusbar *sb = (GtkStatusbar *) lookup_widget(tagman, "sb");\
	if (sb != NULL) {\
		char *formatted = malloc(1024);\
		if (formatted != NULL) {\
			sprintf(formatted, msg,## __VA_ARGS__);\
			gtk_statusbar_push(sb, 0, formatted);\
			free(formatted);\
		}\
	}\
}

extern GtkWidget *tagman;
extern GtkWidget *chooserepository;
extern GtkWidget *aboutdialog;

extern struct tagsistant tagsistant;
