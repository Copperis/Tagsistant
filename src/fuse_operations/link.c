/*
   Tagsistant (tagfs) -- fuse_operations/link.c
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
 * link equivalent
 *
 * @param from existing file name
 * @param to new file name
 * @return(0 on success, -errno otherwise)
 * @todo why tagsistant_link() calls tagsistant_symlink()? We should be able to perform proper linking!
 */
int tagsistant_link(const char *from, const char *to)
{
	return(tagsistant_symlink(from, to));
}
