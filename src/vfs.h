#ifndef PYSQLITE_VFS_H
#define PYSQLITE_VFS_H

#include "Python.h"
#include "sqlite3.h"

sqlite3_vfs *pysqlite_vfs_create(PyObject *owner);
void pysqlite_vfs_destroy(sqlite3_vfs *);

#endif
