#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include <gtk/gtk.h>

#include "callbacks.h"
#include "interface.h"
#include "support.h"
#include "tagman.h"

struct tagsistant tagsistant;

void
on_open1_activate                      (GtkMenuItem     *menuitem,
                                        gpointer         user_data)
{
	gtk_widget_show(chooserepository);	
}


void
on_quit1_activate                      (GtkMenuItem     *menuitem,
                                        gpointer         user_data)
{
	exit(0);
}


void
on_cut1_activate                       (GtkMenuItem     *menuitem,
                                        gpointer         user_data)
{

}


void
on_copy1_activate                      (GtkMenuItem     *menuitem,
                                        gpointer         user_data)
{

}


void
on_paste1_activate                     (GtkMenuItem     *menuitem,
                                        gpointer         user_data)
{

}


void
on_delete1_activate                    (GtkMenuItem     *menuitem,
                                        gpointer         user_data)
{

}


void
on_relations1_activate                 (GtkMenuItem     *menuitem,
                                        gpointer         user_data)
{

}


void
on_by_tag1_activate                    (GtkMenuItem     *menuitem,
                                        gpointer         user_data)
{

}


void
on_by_file1_activate                   (GtkMenuItem     *menuitem,
                                        gpointer         user_data)
{

}


void
on_about1_activate                     (GtkMenuItem     *menuitem,
                                        gpointer         user_data)
{
	gtk_widget_show(aboutdialog);
}


void
on_aboutdialog_close                   (GtkDialog       *dialog,
                                        gpointer         user_data)
{
	gtk_widget_hide(aboutdialog);
}


void
on_tagman_destroy                      (GtkObject       *object,
                                        gpointer         user_data)
{
	exit(0);
}


int
on_chooserepository_unrealize          (GtkWidget       *widget,
                                        gpointer         user_data)
{
	gtk_widget_hide(chooserepository);
	return TRUE;
}


void
on_repositorychooser_cancel_clicked    (GtkButton       *button,
                                        gpointer         user_data)
{

}

int
add_relation(void *data, int argc, char **argv, char **azColumName)
{
	sb("Adding relation %s %s %s", argv[0], argv[1], argv[2]);	

	GtkTreeView *tv = lookup_widget(tagman, "relationsview");
	if (tv != NULL) {
		GtkTreeStore *ts = GTK_TREE_STORE(gtk_tree_view_get_model(tv));
		if (ts != NULL) {
			GtkTreeIter iter;
			gtk_tree_store_append(ts, &iter, NULL);
			gtk_tree_store_set(ts, &iter, 0, argv[0], 1, argv[1], 2, argv[2], -1);
		}
	}

	return 0;
}


int
on_repositoryopen_open_clicked         (GtkButton       *button,
                                        gpointer         user_data)
{
	tagsistant.repository = gtk_file_chooser_get_filename(GTK_FILE_CHOOSER(chooserepository));
	gtk_widget_hide(chooserepository);

	struct stat st;
	if (lstat(tagsistant.repository, &st) == -1) {
		int errnotmp = errno;
		sb("Error opening %s: %s", tagsistant.repository, strerror(errnotmp));
		return TRUE;
	}

	tagsistant.archive = calloc(sizeof(char), strlen(tagsistant.repository) + strlen("/archive/") + 1);
	strcat(tagsistant.archive, tagsistant.repository);
	strcat(tagsistant.archive, "/archive/");
	if (lstat(tagsistant.repository, &st) == -1) {
		int errnotmp = errno;
		sb("Error on %s: %s", tagsistant.archive, strerror(errnotmp));
		return TRUE;
	}

	tagsistant.tags = calloc(sizeof(char), strlen(tagsistant.repository) + strlen("/tags.sql") + 1);
	strcat(tagsistant.tags, tagsistant.repository);
	strcat(tagsistant.tags, "/tags.sql");
	if (lstat(tagsistant.tags, &st) == -1) {
		int errnotmp = errno;
		sb("Error on %s: %s", tagsistant.tags, strerror(errnotmp));
		return TRUE;
	}

	sb("Opening SQL archive @%s", tagsistant.tags);

	/* setting the store for relations view */
	GtkTreeStore *ts = gtk_tree_store_new(3, G_TYPE_STRING, G_TYPE_STRING, G_TYPE_STRING);
	gtk_tree_view_set_model(lookup_widget(tagman, "relationsview"), GTK_TREE_MODEL(ts));

	do_sql(NULL, "select tag1, relation, tag2 from relations;", add_relation, NULL);

	return TRUE;
}


int
on_chooserepository_close              (GtkDialog       *dialog,
                                        gpointer         user_data)
{
	gtk_widget_hide(chooserepository);
	return TRUE;
}


int
on_chooserepository_destroy            (GtkObject       *object,
                                        gpointer         user_data)
{
	gtk_widget_hide(chooserepository);
	return TRUE;
}


int
on_chooserepository_unmap              (GtkWidget       *widget,
                                        gpointer         user_data)
{
	gtk_widget_hide(chooserepository);
	return TRUE;
}

