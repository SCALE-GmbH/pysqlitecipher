#ifndef PYSQLITE_INHERIT_VFS_H
#define PYSQLITE_INHERIT_VFS_H

#include "sqlite3.h"

int pysqlite_inherit_vfs(
    sqlite3_vfs *new_vfs,            /* VFS structure to populate */
    const sqlite3_vfs *orig_vfs,     /* original VFS to forward to */
    const char *vfs_name);           /* Name to assign to the new VFS */

int pysqlite_inherit_io_methods(
    sqlite3_io_methods *new_vmt,
    const sqlite3_io_methods *orig_vmt);

#endif
