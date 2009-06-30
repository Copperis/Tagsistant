bin_PROGRAMS = tagman

tagman_SOURCES = tagman.h callbacks.c callbacks.h interface.c interface.h main.c support.c support.h ../../sql.c

tagman_CFLAGS = `pkg-config --cflags gtk+-2.0 glib-2.0` $(CFLAGS) -I/usr/include/ -D'PACKAGE_DATA_DIR="@datadir@/"' -DTAGMAN
tagman_LDFLAGS = `pkg-config --libs gtk+-2.0 glib-2.0` $(LDFLAGS) -lsqlite3 -ldl

install:
	if [ ! -d /usr/local/share/pixmaps/tagsistant/ ]; then mkdir @datadir@/pixmaps/tagsistant/; fi
	cp ../pixmaps/* @datadir@/pixmaps/tagsistant/
	test -z "/usr/local/bin" || /bin/mkdir -p "/usr/local/bin"
	/bin/bash ../../../libtool --mode=install /usr/bin/install -c 'tagman' '/usr/local/bin/tagman'
	/usr/bin/install -c tagman /usr/local/bin/tagman

