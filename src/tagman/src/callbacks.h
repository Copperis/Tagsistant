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

int
on_chooserepository_unrealize          (GtkWidget       *widget,
                                        gpointer         user_data);

void
on_repositorychooser_cancel_clicked    (GtkButton       *button,
                                        gpointer         user_data);

int
on_repositoryopen_open_clicked         (GtkButton       *button,
                                        gpointer         user_data);

int
on_chooserepository_close              (GtkDialog       *dialog,
                                        gpointer         user_data);

int
on_chooserepository_destroy            (GtkObject       *object,
                                        gpointer         user_data);

int
on_chooserepository_unmap              (GtkWidget       *widget,
                                        gpointer         user_data);
