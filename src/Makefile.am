bin_PROGRAMS = tagsistant

tagsistant_SOURCES = tagsistant.c path_resolution.c\
	compat/fuse_opt.c debug.h debug_free_calls.h

#### tagsistant_CFLAGS = -D'PLUGINS_DIR="@libexecdir@/tagsistant"'
tagsistant_CFLAGS = -D'PLUGINS_DIR="@libdir@/"' $(CFLAGS)
tagsistant_LDFLAGS = -lfuse -lsqlite3 -ldl -rdynamic

lib_LTLIBRARIES = libtagsistant_generic-0.1.la libtagsistant_mp3-0.1.la\
	libtagsistant_gif-0.1.la libtagsistant_html-0.1.la libtagsistant_jpeg-0.1.la\
	libtagsistant_ogg-0.1.la libtagsistant_xml-0.1.la libtagsistant_png-0.1.la
libtagsistant_generic_0_1_la_SOURCES = tagsistant.h tp_generic.c
libtagsistant_mp3_0_1_la_SOURCES = tagsistant.h tp_mp3.c
libtagsistant_gif_0_1_la_SOURCES = tagsistant.h tp_gif.c
libtagsistant_html_0_1_la_SOURCES = tagsistant.h tp_html.c
libtagsistant_jpeg_0_1_la_SOURCES = tagsistant.h tp_jpeg.c
libtagsistant_ogg_0_1_la_SOURCES = tagsistant.h tp_ogg.c
libtagsistant_xml_0_1_la_SOURCES = tagsistant.h tp_xml.c
libtagsistant_png_0_1_la_SOURCES = tagsistant.h tp_png.c
