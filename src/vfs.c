#include "vfs.h"
#include "sqlite3.h"
#include <unistd.h>


/* Wraps sqlite3_io_methods for extension. */

typedef struct _wrapped_io_methods {
    sqlite3_io_methods vmt;
    const sqlite3_io_methods *root_vmt;

    /* PyUnicode of the filename in use */
    PyObject *filename;

    /* LockManager managing this file */
    PyObject *lock_manager;
} wrapped_io_methods;


/* Forward declarations */

static PyObject *lookup_lock_manager(void);

static int wrapped_xOpen(sqlite3_vfs*, const char *zName, sqlite3_file*, int flags, int *pOutFlags);
static int wrapped_xClose(sqlite3_file*);
static int wrapped_xLock(sqlite3_file*, int);
static int wrapped_xUnlock(sqlite3_file*, int);


/* Set up a special VFS for Python usage.

   This VFS is used to support better (customized) locking behaviour when
   SQLite is used from Python. The goal is to coordinate in-process access
   to one database to avoid expensive operating system calls.

   It is automatically registered as default VFS for now so look out if
   you are using SQLite from C inside the same process.
*/
int pysqlite_vfs_setup(void)
{
    sqlite3_vfs *wrapped_vfs = NULL;
    sqlite3_vfs *root_vfs = sqlite3_vfs_find(NULL);

    if (! root_vfs)
        return SQLITE_NOTFOUND;

    wrapped_vfs = sqlite3_malloc(sizeof(*wrapped_vfs));
    if (! wrapped_vfs)
        return SQLITE_NOMEM;

    memcpy(wrapped_vfs, root_vfs, sizeof(*wrapped_vfs));

    wrapped_vfs->zName = "pywrapped_vfs";
    wrapped_vfs->pAppData = root_vfs;
    wrapped_vfs->xOpen = wrapped_xOpen;

    return sqlite3_vfs_register(wrapped_vfs, 1);
}


/* Wrapper for xOpen of the original VFS.

   This is overriden only to allow us to replace the sqlite3_io_methods table
   that is returned by xOpen. Otherwise, we do not currently interfere with
   xOpen behaviour.
*/
static int wrapped_xOpen(
        sqlite3_vfs *vfs,
        const char *zName,
        sqlite3_file *file,
        int flags,
        int *pOutFlags)
{
    int rc;
    sqlite3_vfs *root_vfs = vfs->pAppData;
    wrapped_io_methods *new_methods = NULL;
    PyGILState_STATE gstate;

    /* Call the original open method */

    rc = root_vfs->xOpen(root_vfs, zName, file, flags, pOutFlags);
    if (!file->pMethods)
        return rc;

    /* If the original open succeeded, initialize our data. */

    gstate = PyGILState_Ensure();

    new_methods = sqlite3_malloc(sizeof(*new_methods));
    if (!new_methods) {
        rc = SQLITE_NOMEM;
        goto error_out;
    }

    memcpy(&new_methods->vmt, file->pMethods, sizeof(new_methods->vmt));
    if (zName) {
        new_methods->filename = PyUnicode_FromString(zName);
    } else {
        Py_INCREF(Py_None);
        new_methods->filename = Py_None;
    }
    if (!new_methods->filename) {
        PyErr_Clear();
        rc = SQLITE_NOMEM;
        goto error_out;
    }

    new_methods->lock_manager = lookup_lock_manager();
    if (!new_methods->lock_manager) {
        PyErr_Clear();
        rc = SQLITE_ERROR;
        goto error_out;
    }

    new_methods->root_vmt = file->pMethods;
    new_methods->vmt.xClose = wrapped_xClose;
    new_methods->vmt.xLock = wrapped_xLock;
    new_methods->vmt.xUnlock = wrapped_xUnlock;
    file->pMethods = &new_methods->vmt;

    PyGILState_Release(gstate);
    return rc;

error_out:
    file->pMethods = NULL;
    if (new_methods) {
        Py_XDECREF(new_methods->lock_manager);
        Py_XDECREF(new_methods->filename);
    }
    sqlite3_free(new_methods);
    PyGILState_Release(gstate);
    return rc;
}


/* Wrapper for xClose of the original VFS.

   We need to wrap this method only to let of of the sqlite3_io_methods table
   that was allocated in wrapped_xOpen.
*/
static int wrapped_xClose(sqlite3_file *file)
{
    int rc;
    wrapped_io_methods *methods = (wrapped_io_methods *) file->pMethods;
    const sqlite3_io_methods *root_vmt = methods->root_vmt;
    PyGILState_STATE gstate;

    rc = root_vmt->xClose(file);
    if (rc != SQLITE_OK)
        return rc;

    file->pMethods = NULL;

    gstate = PyGILState_Ensure();

    Py_XDECREF(methods->lock_manager);
    Py_XDECREF(methods->filename);
    sqlite3_free(methods);

    PyGILState_Release(gstate);
    return rc;
}

/* Wrapper for xLock of the original VFS.

   This wrapper is the main reason for creating this VFS. At this point
   we should be able to interfere with SQLite's builtin locking behaviour
   and replace busy waiting with a fair shared-exclusive lock.
*/
static int wrapped_xLock(sqlite3_file *file, int lock_mode)
{
    int rc = SQLITE_OK;
    wrapped_io_methods *methods = (wrapped_io_methods *) file->pMethods;
    const sqlite3_io_methods *root_vmt = methods->root_vmt;
    PyObject *result = NULL;
    PyGILState_STATE gstate;

    gstate = PyGILState_Ensure();

    result = PyObject_CallMethod(methods->lock_manager, "lock", "Oi", methods->filename, lock_mode);
    if (!result) {
        PyErr_Clear();
        rc = SQLITE_IOERR_LOCK;
    }
    Py_XDECREF(result);

    PyGILState_Release(gstate);

    if (rc == SQLITE_OK)
        rc = root_vmt->xLock(file, lock_mode);
    return rc;
}


/* Wrapper for xUnlock of the original VFS.

   This wrapper is needed for symmetry with the implementation of xLock.
   We need to know when a lock is released to correctly implement a better
   locking behaviour.
*/
static int wrapped_xUnlock(sqlite3_file *file, int lock_mode)
{
    int rc;
    wrapped_io_methods *methods = (wrapped_io_methods *) file->pMethods;
    const sqlite3_io_methods *root_vmt = methods->root_vmt;
    PyObject *result = NULL;
    PyGILState_STATE gstate;

    rc = root_vmt->xUnlock(file, lock_mode);
    if (rc == SQLITE_OK) {
        gstate = PyGILState_Ensure();
        result = PyObject_CallMethod(methods->lock_manager, "unlock", "Oi", methods->filename, lock_mode);
        if (!result) {
            PyErr_Clear();
            rc = SQLITE_IOERR_UNLOCK;
        }
        Py_DECREF(result);
        PyGILState_Release(gstate);
    }

    return rc;
}

static PyObject *lookup_lock_manager()
{
    PyObject *module, *lock_manager;

    module = PyImport_ImportModuleNoBlock("pysqlite2.lock_manager");
    if (!module)
        return NULL;

    lock_manager = PyObject_GetAttrString(module, "_lock_manager");
    Py_DECREF(module);
    return lock_manager;
}
