#ifndef PYSQLITE_VFS_H
#define PYSQLITE_VFS_H

/* Provides wrappers for the SQLite3 VFS structures to allow access to
   locking functions etc. from Python. */

#include "Python.h"
#include "sqlite3.h"

typedef struct
{
    PyObject_HEAD

    /* Pointer to the wrapped VFS implementation. */
    sqlite3_vfs *real_vfs;
} pysqlite_VFS;


typedef struct
{
    PyObject_HEAD

    /* File name, must be released via PyMem_Free */
    char *filename;

    /* Pointer to the wrapped file (free via PyMem_Free) */
    sqlite3_file *real_file;
} pysqlite_VFSFile;


int pysqlite_vfs_register(PyObject *module);

#endif
