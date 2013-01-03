#ifndef PYSQLITE_VFS_H
#define PYSQLITE_VFS_H

#include "sqlite3.h"

sqlite3_vfs *pysqlite_vfs_create(void);
void pysqlite_vfs_destroy(sqlite3_vfs *);

#endif
