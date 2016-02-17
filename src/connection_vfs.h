#ifndef PYSQLITE_CONNECTION_VFS_H
#define PYSQLITE_CONNECTION_VFS_H

/* Creates a private VFS for each database connection. This is/was required
   to make it possible to pass the next requested lock level by setting an
   attribute on the connection. This avoids the continuous deadlock for two
   writers (as both will acquire a shared lock and a reserved lock, one of
   them can not continue that way). */

#include "Python.h"
#include "sqlite3.h"

int pysqlite_vfs_setup_types(void);
sqlite3_vfs *pysqlite_vfs_create(PyObject *owner);
void pysqlite_vfs_destroy(sqlite3_vfs *);

#endif
