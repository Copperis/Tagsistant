/*
   Tagsistant (tagfs) -- fuse_operations/symlink.c
   Copyright (C) 2006-2009 Tx0 <tx0@strumentiresistenti.org>

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

#include "../tagsistant.h"

/**
 * symlink equivalent
 *
 * @param from existing file name
 * @param to new file name
 * @return(0 on success, -errno otherwise)
 *
 * TODO :: Huston, we have a problem with Nautilus:
 *
 * TS> / SYMLINK /home/tx0/tags/tags/t1/=/1.clutter_renamed to /tags/t1/=/Link to 1.clutter_renamed (@tagsistant.c:973)
 * TS> | SQL: [start transaction] @sql.c:240 (@sql.c:302)
 * TS> | Building querytree for /tags/t1/=/1.clutter_renamed (@path_resolution.c:254)
 * TS> | Building querytree for /tags/t1/=/Link to 1.clutter_renamed (@path_resolution.c:254)
 * TS> | Retagging /home/tx0/tags/tags/t1/=/1.clutter_renamed as internal to /home/tx0/tags (@tagsistant.c:1011)
 * TS> | Traversing querytree... (@tagsistant.c:1012)
 * TS> | SQL: [insert into tags(tagname) values("t1");] @sql.c:456 (@sql.c:302)
 * TS> | SQL Error: 1062: Duplicate entry 't1' for key 'tagname'. (@sql.c:326)
 * TS> | SQL: [select tag_id from tags where tagname = "t1" limit 1] @sql.c:440 (@sql.c:302)
 * TS> | Returning integer: 1 (@sql.c:388)
 * TS> | Tagging object 1 as t1 (1) (@sql.c:460)
 * TS> | SQL: [insert into tagging(tag_id, object_id) values("1", "1");] @sql.c:462 (@sql.c:302)
 * TS> | SQL Error: 1062: Duplicate entry '1-1' for key 'Tagging_key'. (@sql.c:326)
 * TS> | Applying sql_tag_object(t1,...) (@tagsistant.c:1012)
 * TS> | SQL: [commit] @sql.c:248 (@sql.c:302)
 * TS> \ SYMLINK from /home/tx0/tags/tags/t1/=/1.clutter_renamed to /tags/t1/=/Link to 1.clutter_renamed (QTYPE_TAGS): OK (@tagsistant.c:1048)
 *
 * May be we should reconsider the idea of retagging internal
 * paths while symlinking...
 */
int tagsistant_symlink(const char *from, const char *to)
{
	int tagsistant_errno = 0, res = 0;

	TAGSISTANT_START("/ SYMLINK %s to %s", from, to);

	/*
	 * guess if query points to an external or internal object
	 */
	char *_from = (char *) from;
	if (!TAGSISTANT_PATH_IS_EXTERNAL(from)) {
		_from = (char * ) from + strlen(tagsistant.mountpoint);
		// dbg(LOG_INFO, "%s is internal to %s, trimmed to %s", from, tagsistant.mountpoint, _from);
	}

	tagsistant_querytree *from_qtree = tagsistant_querytree_new(_from, 0);
	tagsistant_querytree *to_qtree = tagsistant_querytree_new(to, 0);

	from_qtree->is_external = (from == _from) ? 1 : 0;

	if (from_qtree->object_path)
		tagsistant_querytree_set_object_path(to_qtree, from_qtree->object_path);

	// -- malformed --
	if (QTREE_IS_MALFORMED(to_qtree)) {
		res = -1;
		tagsistant_errno = ENOENT;
	} else

	// -- object on disk --
	if (QTREE_POINTS_TO_OBJECT(to_qtree) || (QTREE_IS_TAGS(to_qtree) && QTREE_IS_COMPLETE(to_qtree))) {

		// if object_path is null, borrow it from original path
		if (strlen(to_qtree->object_path) == 0) {
			dbg(LOG_INFO, "Getting object path from %s", from);
			tagsistant_querytree_set_object_path(to_qtree, g_path_get_basename(from));
		}

		// if qtree is internal, just re-tag it, taking the tags from to_qtree but
		// the ID from from_qtree
#if TAGSISTANT_RETAG_INTERNAL_SYMLINKS
		if (0 && QTREE_IS_INTERNAL(from_qtree) && from_qtree->object_id) {
			dbg(LOG_INFO, "Retagging %s as internal to %s", from, tagsistant.mountpoint);
			tagsistant_querytree_traverse(to_qtree, tagsistant_sql_tag_object, from_qtree->object_id);
			goto SYMLINK_EXIT;
		} else
#endif

		// if qtree is taggable, do it
		if (QTREE_IS_TAGGABLE(to_qtree)) {
			dbg(LOG_INFO, "SYMLINK : Creating %s", to_qtree->object_path);
			res = tagsistant_force_create_and_tag_object(to_qtree, &tagsistant_errno);
			if (-1 == res) goto SYMLINK_EXIT;
		} else

		// nothing to do about tags
		{
			dbg(LOG_INFO, "%s is not taggable!", to_qtree->full_path); // ??? why ??? should be taggable!!
		}

		// do the real symlink on disk
		dbg(LOG_INFO, "Symlinking %s to %s", from, to_qtree->object_path);
		res = symlink(from, to_qtree->full_archive_path);
		tagsistant_errno = errno;
	} else

	// -- tags (uncomplete) --
	// -- stats --
	// -- relations --
	{
		res = -1;
		tagsistant_errno = EINVAL; /* can't symlink outside of tags/ and archive/ */
	}

SYMLINK_EXIT:
	stop_labeled_time_profile("symlink");

	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("\\ SYMLINK from %s to %s (%s) (%s): %d %d: %s", from, to, to_qtree->full_archive_path, tagsistant_querytree_type(to_qtree), res, tagsistant_errno, strerror(tagsistant_errno));
	} else {
		TAGSISTANT_STOP_OK("\\ SYMLINK from %s to %s (%s): OK", from, to, tagsistant_querytree_type(to_qtree));
	}

	tagsistant_querytree_destroy(from_qtree);
	tagsistant_querytree_destroy(to_qtree);

	return((res == -1) ? -tagsistant_errno : 0);
}
