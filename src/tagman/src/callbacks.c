#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include <gtk/gtk.h>

#include "callbacks.h"
#include "interface.h"
#include "support.h"
#include "tagman.h"

struct tagsistant tagsistant;

/* selected relation view index */
unsigned int selected_relation_id = 0;
GtkTreeIter selected_relation_iter;
unsigned int last_relation_id = 0;

void
on_open1_activate                      (GtkMenuItem     *menuitem,
                                        gpointer         user_data)
{
	(void) menuitem;
	(void) user_data;
	gtk_widget_show(chooserepository);	
}


void
on_quit1_activate                      (GtkMenuItem     *menuitem,
                                        gpointer         user_data)
{
	(void) menuitem;
	(void) user_data;
	exit(0);
}


void
on_cut1_activate                       (GtkMenuItem     *menuitem,
                                        gpointer         user_data)
{
	(void) menuitem;
	(void) user_data;
}


void
on_copy1_activate                      (GtkMenuItem     *menuitem,
                                        gpointer         user_data)
{
	(void) menuitem;
	(void) user_data;

}


void
on_paste1_activate                     (GtkMenuItem     *menuitem,
                                        gpointer         user_data)
{
	(void) menuitem;
	(void) user_data;
}


void
on_delete1_activate                    (GtkMenuItem     *menuitem,
                                        gpointer         user_data)
{
	(void) menuitem;
	(void) user_data;
}


void
on_relations1_activate                 (GtkMenuItem     *menuitem,
                                        gpointer         user_data)
{
	(void) menuitem;
	(void) user_data;
	gtk_notebook_set_current_page(GTK_NOTEBOOK(lookup_widget(tagman, "notebook")), 0);
}


void
on_by_tag1_activate                    (GtkMenuItem     *menuitem,
                                        gpointer         user_data)
{
	(void) menuitem;
	(void) user_data;
	gtk_notebook_set_current_page(GTK_NOTEBOOK(lookup_widget(tagman, "notebook")), 1);
}


void
on_by_file1_activate                   (GtkMenuItem     *menuitem,
                                        gpointer         user_data)
{
	(void) menuitem;
	(void) user_data;
	gtk_notebook_set_current_page(GTK_NOTEBOOK(lookup_widget(tagman, "notebook")), 2);
}


void
on_about1_activate                     (GtkMenuItem     *menuitem,
                                        gpointer         user_data)
{
	(void) menuitem;
	(void) user_data;
	gtk_widget_show(aboutdialog);
}


void
on_aboutdialog_close                   (GtkDialog       *dialog,
                                        gpointer         user_data)
{
	(void) dialog;
	(void) user_data;
	gtk_widget_hide(aboutdialog);
}


void
on_tagman_destroy                      (GtkObject       *object,
                                        gpointer         user_data)
{
	(void) object;
	(void) user_data;
	exit(0);
}


void
on_repositorychooser_cancel_clicked    (GtkButton       *button,
                                        gpointer         user_data)
{
	(void) button;
	(void) user_data;
	gtk_widget_hide(chooserepository);
}

int
add_relation(void *data, int argc, char **argv, char **azColumName)
{
	(void) data;
	(void) argc;
	(void) azColumName;

	sb("Adding relation %s: %s %s %s", argv[3], argv[0], argv[1], argv[2]);	

	GtkTreeView *tv = GTK_TREE_VIEW(lookup_widget(tagman, "relationsview"));
	if (tv != NULL) {
		GtkTreeStore *ts = GTK_TREE_STORE(gtk_tree_view_get_model(tv));
		if (ts != NULL) {
			last_relation_id = atoi(argv[3]);
			GtkTreeIter iter;
			gtk_tree_store_append(ts, &iter, NULL);
			gtk_tree_store_set(ts, &iter, 0, argv[0], 1, argv[1], 2, argv[2], 3, last_relation_id, -1);
		}
	}

	return 0;
}


int
on_repositoryopen_open_clicked         (GtkButton       *button,
                                        gpointer         user_data)
{
	(void) button;
	(void) user_data;

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
	GtkTreeStore *ts = gtk_tree_store_new(4, G_TYPE_STRING, G_TYPE_STRING, G_TYPE_STRING, G_TYPE_INT);
	GtkTreeView *tv = GTK_TREE_VIEW(lookup_widget(tagman, "relationsview"));
	gtk_tree_view_set_model(tv, GTK_TREE_MODEL(ts));

	gtk_tree_view_columns_autosize(tv);

	do_sql(NULL, "select tag1, relation, tag2, id from relations;", add_relation, NULL);

	return TRUE;
}


void
init_interface()
{
	/* setting the store for relations view */
	GtkTreeStore *ts = gtk_tree_store_new(3, G_TYPE_STRING, G_TYPE_STRING, G_TYPE_STRING);
	GtkTreeView *tv = GTK_TREE_VIEW(lookup_widget(tagman, "relationsview"));
	gtk_tree_view_set_model(tv, GTK_TREE_MODEL(ts));

	/* adding columns to relationsview widget */
	GtkTreeViewColumn *col = NULL;
	GtkCellRenderer *renderer = gtk_cell_renderer_text_new();
	col = gtk_tree_view_column_new_with_attributes("Tag1", renderer, "text", 0, NULL);
	gtk_tree_view_append_column(tv, col);

	renderer = gtk_cell_renderer_text_new();
	col = gtk_tree_view_column_new_with_attributes("Relation", renderer, "text", 1, NULL);
	gtk_tree_view_append_column(tv, col);

	renderer = gtk_cell_renderer_text_new();
	col = gtk_tree_view_column_new_with_attributes("Tag2", renderer, "text", 2, NULL);
	gtk_tree_view_append_column(tv, col);

	gtk_file_chooser_set_show_hidden(GTK_FILE_CHOOSER(chooserepository), TRUE);
}

void
on_relationsview_row_activated         (GtkTreeView     *treeview,
                                        GtkTreePath     *path,
                                        GtkTreeViewColumn *column,
                                        gpointer         user_data)
{
	(void) column;
	(void) user_data;

	gchar *tag1, *relation, *tag2;
	gtk_tree_model_get_iter(gtk_tree_view_get_model(treeview), &selected_relation_iter, path);
	gtk_tree_model_get(gtk_tree_view_get_model(treeview), &selected_relation_iter, 0, &tag1, 1, &relation, 2, &tag2, 3, &selected_relation_id, -1);

	sb("%u: %s %s %s", selected_relation_id, tag1, relation, tag2);

	gtk_entry_set_text(GTK_ENTRY(lookup_widget(tagman, "tag1")), tag1);
	gtk_entry_set_text(GTK_ENTRY(lookup_widget(tagman, "tag2")), tag2);

	if (strcmp(relation, "is equivalent") == 0) {
		gtk_combo_box_set_active(GTK_COMBO_BOX(lookup_widget(tagman, "relationstype")), 1);
	} else if (strcmp(relation, "includes") == 0) {
		gtk_combo_box_set_active(GTK_COMBO_BOX(lookup_widget(tagman, "relationstype")), 0);
	} else {
		gtk_combo_box_set_active(GTK_COMBO_BOX(lookup_widget(tagman, "relationstype")), -1);
		sb("Error: unknown relation type: %s", relation);
	}

	g_free(tag1);
	g_free(relation);
	g_free(tag2);
}


void
on_chooserepository_file_activated     (GtkFileChooser  *filechooser,
                                        gpointer         user_data)
{
	(void) filechooser;
	(void) user_data;
}


gboolean
on_relationsview_select_cursor_row     (GtkTreeView     *treeview,
                                        gboolean         start_editing,
                                        gpointer         user_data)
{
	(void) treeview;
	(void) start_editing;
	(void) user_data;
	return FALSE;
}


void
on_relationsview_cursor_changed        (GtkTreeView     *treeview,
                                        gpointer         user_data)
{
	(void) treeview;
	(void) user_data;
}


void
on_add_relation_button_clicked         (GtkButton       *button,
                                        gpointer         user_data)
{
	(void) button;
	(void) user_data;

	if (tagsistant.tags == NULL)
		return;

	GtkTreeIter iter;
	GtkTreeView *tv = GTK_TREE_VIEW(lookup_widget(tagman, "relationsview"));
	GtkTreeStore *ts = GTK_TREE_STORE(gtk_tree_view_get_model(tv));
	gtk_tree_store_append(ts, &iter, NULL);

	const char *tag1 = gtk_entry_get_text(GTK_ENTRY(lookup_widget(tagman, "tag1")));
	const char *tag2 = gtk_entry_get_text(GTK_ENTRY(lookup_widget(tagman, "tag2")));
	char *relation = NULL;

	switch (gtk_combo_box_get_active(GTK_COMBO_BOX(lookup_widget(tagman, "relationstype")))) {
		case 0:
			relation = strdup("includes");
			break;
		case 1:
			relation = strdup("is equivalent");
			break;
		default:
			break;
	}

	last_relation_id++;
	gtk_tree_store_set(ts, &iter, 0, tag1, 1, relation, 2, tag2, 3, last_relation_id, -1);

#	define INSERT_RELATION "insert into relations (id, tag1, relation, tag2) values (%u, \"%s\", \"%s\", \"%s\");"

	char *sql = calloc(sizeof(char), strlen(INSERT_RELATION) + strlen(tag1) + strlen(relation) + strlen(tag2) + 10);
	if (sql != NULL) {
		sprintf(sql, INSERT_RELATION, last_relation_id, tag1, relation, tag2);
		sb("SQL: %s", sql);
		do_sql(NULL, sql, NULL, NULL);
		free(sql);
	} else {
		sb("Error: allocating memory @%s:%d", __FILE__, __LINE__);
	}

	sb("Relation %s %s %s added", tag1, relation, tag2);

	free(relation);
	/* tag1 and tag2 points to internal space, don't need to be de-allocated */
}


void
on_update_relation_button_clicked      (GtkButton       *button,
                                        gpointer         user_data)
{
	(void) button;
	(void) user_data;

	if (tagsistant.tags == NULL)
		return;

	GtkTreeView *tv = GTK_TREE_VIEW(lookup_widget(tagman, "relationsview"));
	GtkTreeStore *ts = GTK_TREE_STORE(gtk_tree_view_get_model(tv));

	const char *tag1 = gtk_entry_get_text(GTK_ENTRY(lookup_widget(tagman, "tag1")));
	const char *tag2 = gtk_entry_get_text(GTK_ENTRY(lookup_widget(tagman, "tag2")));
	char *relation = NULL;

	switch (gtk_combo_box_get_active(GTK_COMBO_BOX(lookup_widget(tagman, "relationstype")))) {
		case 0:
			relation = strdup("includes");
			break;
		case 1:
			relation = strdup("is equivalent");
			break;
		default:
			break;
	}

	gtk_tree_store_set(ts, &selected_relation_iter, 0, tag1, 1, relation, 2, tag2, -1);

#	define UPDATE_RELATION "update relations set tag1 = \"%s\", relation = \"%s\", tag2 = \"%s\" where id = %u;"

	char *sql = calloc(sizeof(char), strlen(UPDATE_RELATION) + strlen(tag1) + strlen(relation) + strlen(tag2) + 10);
	if (sql != NULL) {
		sprintf(sql, UPDATE_RELATION, tag1, relation, tag2, selected_relation_id);
		sb("SQL: %s", sql);
		do_sql(NULL, sql, NULL, NULL);
		free(sql);
	} else {
		sb("Error: allocating memory @%s:%d", __FILE__, __LINE__);
	}

	sb("Relation %s %s %s updated", tag1, relation, tag2);

	free(relation);
	/* tag1 and tag2 points to internal space, don't need to be de-allocated */
}


void
on_delete_relation_button_clicked      (GtkButton       *button,
                                        gpointer         user_data)
{
	(void) button;
	(void) user_data;

	if (tagsistant.tags == NULL)
		return;

	GtkTreeView *tv = GTK_TREE_VIEW(lookup_widget(tagman, "relationsview"));
	GtkTreeStore *ts = GTK_TREE_STORE(gtk_tree_view_get_model(tv));

	gtk_tree_store_remove(ts, &selected_relation_iter);

#	define DELETE_RELATION "delete from relations where id = %u;"

	char *sql = calloc(sizeof(char), strlen(DELETE_RELATION) + 10);
	if (sql != NULL) {
		sprintf(sql, DELETE_RELATION, selected_relation_id);
		sb("SQL: %s", sql);
		do_sql(NULL, sql, NULL, NULL);
		free(sql);
	} else {
		sb("Error: allocating memory @%s:%d", __FILE__, __LINE__);
	}

	sb("Relation %u deleted", selected_relation_id);
}


gboolean
on_relationsview_button_press_event    (GtkWidget       *widget,
                                        GdkEventButton  *event,
                                        gpointer         user_data)
{
	(void) user_data;
	GtkTreePath *path;
	GtkTreeViewColumn *column;
	int cell_x, cell_y;
	GtkTreeStore *ts;

	ts = GTK_TREE_STORE(gtk_tree_view_get_model(GTK_TREE_VIEW(widget)));

	if (gtk_tree_view_get_path_at_pos(GTK_TREE_VIEW(widget), event->x, event->y, &path, &column, &cell_x, &cell_y)) {
		if (gtk_tree_model_get_iter(GTK_TREE_MODEL(ts), &selected_relation_iter, path)) {
			gchar *tag1, *relation, *tag2;
			gtk_tree_model_get(gtk_tree_view_get_model(GTK_TREE_VIEW(widget)), &selected_relation_iter, 0, &tag1, 1, &relation, 2, &tag2, 3, &selected_relation_id, -1);

			gtk_entry_set_text(GTK_ENTRY(lookup_widget(tagman, "tag1")), tag1);
			gtk_entry_set_text(GTK_ENTRY(lookup_widget(tagman, "tag2")), tag2);

			if (strcmp(relation, "is equivalent") == 0) {
				gtk_combo_box_set_active(GTK_COMBO_BOX(lookup_widget(tagman, "relationstype")), 1);
			} else if (strcmp(relation, "includes") == 0) {
				gtk_combo_box_set_active(GTK_COMBO_BOX(lookup_widget(tagman, "relationstype")), 0);
			} else {
				gtk_combo_box_set_active(GTK_COMBO_BOX(lookup_widget(tagman, "relationstype")), -1);
				sb("Error: unknown relation type: %s", relation);
			}

			sb("Selected %u: %s %s %s", selected_relation_id, tag1, relation, tag2);

			g_free(tag1);
			g_free(relation);
			g_free(tag2);
		}
	}
	return FALSE;
}


void
on_tag_file_button_clicked             (GtkButton       *button,
                                        gpointer         user_data)
{
	(void) button;
	(void) user_data;

	if (tagsistant.tags == NULL)
		return;

}


void
on_untag_file_button_clicked           (GtkButton       *button,
                                        gpointer         user_data)
{
	(void) button;
	(void) user_data;

	if (tagsistant.tags == NULL)
		return;

}


void
on_new_tag_button_clicked              (GtkButton       *button,
                                        gpointer         user_data)
{
	(void) button;
	(void) user_data;

	if (tagsistant.tags == NULL)
		return;

}


void
on_drop_tag_button_clicked             (GtkButton       *button,
                                        gpointer         user_data)
{
	(void) button;
	(void) user_data;

	if (tagsistant.tags == NULL)
		return;

}


void
on_add_tag_button_clicked              (GtkButton       *button,
                                        gpointer         user_data)
{
	(void) button;
	(void) user_data;

	if (tagsistant.tags == NULL)
		return;

}


void
on_remove_tag_button_clicked           (GtkButton       *button,
                                        gpointer         user_data)
{
	(void) button;
	(void) user_data;

	if (tagsistant.tags == NULL)
		return;

}


void
on_drop_file_button_clicked            (GtkButton       *button,
                                        gpointer         user_data)
{
	(void) button;
	(void) user_data;

	if (tagsistant.tags == NULL)
		return;

}


void
on_aboutdialog_response                (GtkDialog       *dialog,
                                        gint             response_id,
                                        gpointer         user_data)
{
	(void) dialog;
	(void) response_id;
	(void) user_data;
	gtk_widget_hide(aboutdialog);

}


gboolean
on_aboutdialog_delete_event            (GtkWidget       *widget,
                                        GdkEvent        *event,
                                        gpointer         user_data)
{
	(void) widget;
	(void) event;
	(void) user_data;
	return TRUE;
}

gboolean
on_chooserepository_delete_event       (GtkWidget       *widget,
                                        GdkEvent        *event,
                                        gpointer         user_data)
{
	(void) widget;
	(void) event;
	(void) user_data;
	gtk_widget_hide(chooserepository);
	return TRUE;
}


void
on_information1_activate               (GtkMenuItem     *menuitem,
                                        gpointer         user_data)
{
	(void) menuitem;
	(void) user_data;
	gtk_widget_show(helpdialog);
}


gboolean
on_helpdialog_delete_event             (GtkWidget       *widget,
                                        GdkEvent        *event,
                                        gpointer         user_data)
{
	(void) widget;
	(void) event;
	(void) user_data;
	gtk_widget_hide(helpdialog);
	return TRUE;
}


void
on_closebutton1_clicked                (GtkButton       *button,
                                        gpointer         user_data)
{
	(void) button;
	(void) user_data;
	gtk_widget_hide(helpdialog);
}


void
on_how_to2_activate                    (GtkMenuItem     *menuitem,
                                        gpointer         user_data)
{
	(void) menuitem;
	(void) user_data;
	gtk_widget_show(helpdialog);
}

