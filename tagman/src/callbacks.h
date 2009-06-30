#include <gtk/gtk.h>


void
on_open1_activate                      (GtkMenuItem     *menuitem,
                                        gpointer         user_data);

void
on_quit1_activate                      (GtkMenuItem     *menuitem,
                                        gpointer         user_data);

void
on_cut1_activate                       (GtkMenuItem     *menuitem,
                                        gpointer         user_data);

void
on_copy1_activate                      (GtkMenuItem     *menuitem,
                                        gpointer         user_data);

void
on_paste1_activate                     (GtkMenuItem     *menuitem,
                                        gpointer         user_data);

void
on_delete1_activate                    (GtkMenuItem     *menuitem,
                                        gpointer         user_data);

void
on_relations1_activate                 (GtkMenuItem     *menuitem,
                                        gpointer         user_data);

void
on_by_tag1_activate                    (GtkMenuItem     *menuitem,
                                        gpointer         user_data);

void
on_by_file1_activate                   (GtkMenuItem     *menuitem,
                                        gpointer         user_data);

void
on_about1_activate                     (GtkMenuItem     *menuitem,
                                        gpointer         user_data);

void
on_aboutdialog_close                   (GtkDialog       *dialog,
                                        gpointer         user_data);

void
on_tagman_destroy                      (GtkObject       *object,
                                        gpointer         user_data);

void
on_repositorychooser_cancel_clicked    (GtkButton       *button,
                                        gpointer         user_data);
int
on_repositoryopen_open_clicked         (GtkButton       *button,
                                        gpointer         user_data);

void
on_relationsview_row_activated         (GtkTreeView     *treeview,
                                        GtkTreePath     *path,
                                        GtkTreeViewColumn *column,
                                        gpointer         user_data);

void
on_chooserepository_file_activated     (GtkFileChooser  *filechooser,
                                        gpointer         user_data);

gboolean
on_relationsview_select_cursor_row     (GtkTreeView     *treeview,
                                        gboolean         start_editing,
                                        gpointer         user_data);

void
on_relationsview_cursor_changed        (GtkTreeView     *treeview,
                                        gpointer         user_data);

void
on_add_relation_button_clicked         (GtkButton       *button,
                                        gpointer         user_data);

void
on_update_relation_button_clicked      (GtkButton       *button,
                                        gpointer         user_data);

void
on_delete_relation_button_clicked      (GtkButton       *button,
                                        gpointer         user_data);

gboolean
on_relationsview_button_press_event    (GtkWidget       *widget,
                                        GdkEventButton  *event,
                                        gpointer         user_data);

void
on_tag_file_button_clicked             (GtkButton       *button,
                                        gpointer         user_data);

void
on_untag_file_button_clicked           (GtkButton       *button,
                                        gpointer         user_data);

void
on_new_tag_button_clicked              (GtkButton       *button,
                                        gpointer         user_data);

void
on_drop_tag_button_clicked             (GtkButton       *button,
                                        gpointer         user_data);

void
on_add_tag_button_clicked              (GtkButton       *button,
                                        gpointer         user_data);

void
on_remove_tag_button_clicked           (GtkButton       *button,
                                        gpointer         user_data);

void
on_drop_file_button_clicked            (GtkButton       *button,
                                        gpointer         user_data);

void
on_aboutdialog_response                (GtkDialog       *dialog,
                                        gint             response_id,
                                        gpointer         user_data);

gboolean
on_aboutdialog_delete_event            (GtkWidget       *widget,
                                        GdkEvent        *event,
                                        gpointer         user_data);

gboolean
on_chooserepository_delete_event       (GtkWidget       *widget,
                                        GdkEvent        *event,
                                        gpointer         user_data);

void
on_information1_activate               (GtkMenuItem     *menuitem,
                                        gpointer         user_data);

gboolean
on_helpdialog_delete_event             (GtkWidget       *widget,
                                        GdkEvent        *event,
                                        gpointer         user_data);

void
on_closebutton1_clicked                (GtkButton       *button,
                                        gpointer         user_data);

void
on_how_to2_activate                    (GtkMenuItem     *menuitem,
                                        gpointer         user_data);
