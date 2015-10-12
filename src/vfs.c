#include "Python.h"
#include "vfs.h"
#include "inherit_vfs.h"
#include "sqlite3.h"
#include "connection.h"

static PyObject* pysqlite_CallbackError = NULL;


typedef struct _my_vfs {
    sqlite3_vfs vfs_head;

    /* Connection object handled by this vfs */
    PyObject *weak_connection;

    int (*orig_xOpen)(sqlite3_vfs*, const char*, sqlite3_file*, int, int*);
} my_vfs;

/* Wraps sqlite3_io_methods for extension. */

typedef struct _my_io_methods {
    sqlite3_io_methods io_methods_head;

    /* PyUnicode of the filename in use */
    PyObject *filename;

    /* Connection object used to access this file (as weak reference) */
    PyObject *weak_connection;

    /* LockManager managing this file */
    PyObject *lock_manager;

    /* Exception type for deadlock detected by lock manager. */
    PyObject *lock_manager_DeadlockError;

    /* Pointers to original methods which we override. */
    int (*orig_xClose)(sqlite3_file*);
    int (*orig_xLock)(sqlite3_file*, int);
    int (*orig_xUnlock)(sqlite3_file*, int);
} my_io_methods;


/* Forward declarations */

static int lookup_lock_manager(my_io_methods *methods);

static int wrapped_xOpen(sqlite3_vfs*, const char *zName, sqlite3_file*, int flags, int *pOutFlags);
static int wrapped_xClose(sqlite3_file*);
static int wrapped_xLock(sqlite3_file*, int);
static int wrapped_xUnlock(sqlite3_file*, int);

static PyObject *create_orig_xLock_py(sqlite3_file *file);
static PyObject *orig_xLock_trampoline(PyObject *self, PyObject *args);

static int lockmanager_xLock(sqlite3_file *file, int lock_mode);
static void lockmanager_warn_buggy_lock(sqlite3_file *file);


sqlite3_vfs *pysqlite_vfs_create(PyObject *owner)
{
    my_vfs *wrapped_vfs = NULL;
    char *vfs_name = NULL;
    sqlite3_vfs *root_vfs = sqlite3_vfs_find(NULL);
    int rc;

    if (!root_vfs) {
        PyErr_SetString(PyExc_RuntimeError, "no default vfs found");
        goto error_out;
    }

    wrapped_vfs = sqlite3_malloc(sizeof(*wrapped_vfs));
    if (!wrapped_vfs) {
        PyErr_NoMemory();
        goto error_out;
    }
    memset(wrapped_vfs, 0, sizeof(*wrapped_vfs));

    vfs_name = sqlite3_mprintf("%p-pysqlite", (void*) wrapped_vfs);
    if (!vfs_name) {
        PyErr_NoMemory();
        goto error_out;
    }

    pysqlite_inherit_vfs(&wrapped_vfs->vfs_head, root_vfs, vfs_name);

    wrapped_vfs->orig_xOpen = wrapped_vfs->vfs_head.xOpen;
    wrapped_vfs->vfs_head.xOpen = wrapped_xOpen;

    wrapped_vfs->weak_connection = PyWeakref_NewRef(owner, NULL);
    if (!wrapped_vfs->weak_connection)
        goto error_out;

    rc = sqlite3_vfs_register(&wrapped_vfs->vfs_head, 0);
    if (rc != SQLITE_OK) {
        PyErr_SetString(PyExc_RuntimeError, "Can not register VFS for connection.");
        goto error_out;
    }

    return &wrapped_vfs->vfs_head;

error_out:
    if (wrapped_vfs) {
        Py_XDECREF(wrapped_vfs->weak_connection);
    }
    sqlite3_free(wrapped_vfs);
    sqlite3_free(vfs_name);
    return NULL;
}

void pysqlite_vfs_destroy(sqlite3_vfs *vfs)
{
    if (vfs) {
        my_vfs *self = (my_vfs *) vfs;

        sqlite3_vfs_unregister(vfs);
        Py_XDECREF(self->weak_connection);
        sqlite3_free((char*) self->vfs_head.zName);
        sqlite3_free(self);
    }
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
    my_io_methods *methods;

    my_vfs *self = (my_vfs*) vfs;
    PyGILState_STATE gstate;

    /* Call the original open method */

    rc = self->orig_xOpen(vfs, zName, file, flags, pOutFlags);

    /* Do not interfere if there was an error or if this wasn't the main database file. */
    if (!file->pMethods || (flags & SQLITE_OPEN_MAIN_DB) == 0)
        return rc;

    /* If the original open succeeded, initialize our data. */

    gstate = PyGILState_Ensure();

    methods = sqlite3_malloc(sizeof(*methods));
    if (!methods) {
        /* Call original xClose method as the original call was okay. */
        file->pMethods->xClose(file);
        file->pMethods = NULL;
        return SQLITE_NOMEM;
    }
    memset(methods, 0, sizeof(*methods));

    rc = lookup_lock_manager(methods);
    if (rc != SQLITE_OK)
        goto error_out;

    /* Special case: If lock_manager is None, we disable any special code and
       fall back to the original file. */

    if (methods->lock_manager == Py_None) {
        Py_DECREF(methods->lock_manager);
        methods->lock_manager = NULL;
        Py_DECREF(methods->lock_manager_DeadlockError);
        methods->lock_manager_DeadlockError = NULL;
        sqlite3_free(methods);

        PyGILState_Release(gstate);
        return rc;
    }

    rc = pysqlite_inherit_io_methods(&methods->io_methods_head, file->pMethods);
    if (rc != SQLITE_OK)
        goto error_out;

    if (zName) {
        methods->filename = PyUnicode_FromString(zName);
    } else {
        Py_INCREF(Py_None);
        methods->filename = Py_None;
    }
    if (!methods->filename) {
        PyErr_Clear();
        rc = SQLITE_NOMEM;
        goto error_out;
    }

    Py_INCREF(self->weak_connection);
    methods->weak_connection = self->weak_connection;

    methods->orig_xClose = file->pMethods->xClose;
    methods->io_methods_head.xClose = wrapped_xClose;
    methods->orig_xLock = file->pMethods->xLock;
    methods->io_methods_head.xLock = wrapped_xLock;
    methods->orig_xUnlock = file->pMethods->xUnlock;
    methods->io_methods_head.xUnlock = wrapped_xUnlock;
    file->pMethods = &methods->io_methods_head;

    PyGILState_Release(gstate);
    return rc;

error_out:
    file->pMethods->xClose(file);
    file->pMethods = NULL;
    Py_XDECREF(methods->weak_connection);
    Py_XDECREF(methods->lock_manager);
    Py_XDECREF(methods->lock_manager_DeadlockError);
    Py_XDECREF(methods->filename);
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
    my_io_methods *methods = (my_io_methods *) file->pMethods;
    PyGILState_STATE gstate;

    rc = methods->orig_xClose(file);
    if (rc != SQLITE_OK)
        return rc;

    file->pMethods = NULL;

    gstate = PyGILState_Ensure();

    Py_XDECREF(methods->weak_connection);
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
    int rc = lockmanager_xLock(file, lock_mode);

    if (rc == SQLITE_OK) {
        /* At this point, the database file should have been locked by the
           lock manager via the underlying VFS locking function. However, any
           bug inside the lock manager that breaks this requirement will lead
           to database corruption. As a special precaution, we therefore call
           the original VFS xLock method again to catch this case.

           This does not have any effect if the lock manager is implemented
           correctly, but if it isn't this will be either fixed here or
           flagged as an error. */

        my_io_methods *methods = (my_io_methods *) file->pMethods;
        rc = methods->orig_xLock(file, lock_mode);

        /* If orig_xLock returned an error, this means that the lock manager
           is buggy. Better inform the user in some way. Not all locking
           problems are detected here, as xLock may just lock the file. There
           is no API in the SQLite VFS to assert the current lock level. */

        if (rc != SQLITE_OK) {
            lockmanager_warn_buggy_lock(file);
            rc = SQLITE_INTERNAL;
        }
    }
    return rc;
}


/*
   Calls the lock method of the registered lockmanager for our connection.
   Returns the sqlite error code if anything goes wrong.
*/
static int lockmanager_xLock(sqlite3_file *file, int lock_mode)
{
    int rc = SQLITE_OK;
    my_io_methods *methods = (my_io_methods *) file->pMethods;
    PyObject *result = NULL;
    PyObject *connection = NULL;
    pysqlite_Connection *real_conn = NULL;
    PyObject *orig_xLock_py = NULL;
    PyGILState_STATE gstate;

    gstate = PyGILState_Ensure();

    /* Get the minimum lock level from the connection object. */

    connection = methods->weak_connection;
    real_conn = (pysqlite_Connection *) PyWeakref_GetObject(connection);
    Py_XINCREF(real_conn);
    if (!real_conn)
        goto error_out;
    if (real_conn->minimum_lock_level > lock_mode)
        lock_mode = real_conn->minimum_lock_level;

    orig_xLock_py = create_orig_xLock_py(file);
    if (!orig_xLock_py)
        goto error_out;

    result = PyObject_CallMethod(methods->lock_manager, "lock", "OOiO",
            orig_xLock_py, methods->filename, lock_mode, connection);

error_out:
    if (PyErr_Occurred()) {
        if (PyErr_ExceptionMatches(methods->lock_manager_DeadlockError)) {
            PyErr_Clear();
            rc = SQLITE_BUSY;
        } else if (PyErr_ExceptionMatches(pysqlite_CallbackError)) {
            PyObject *etype, *evalue, *etb;
            PyObject *args;
            PyErr_Fetch(&etype, &evalue, &etb);
            args = PyObject_GetAttrString(evalue, "args");
            if (!args) {
                PyErr_Print();
                rc = SQLITE_IOERR_LOCK;
            }
            else if (PyTuple_Check(args)
                    && PyTuple_GET_SIZE(args) == 1
                    && PyInt_Check(PyTuple_GET_ITEM(args, 0))) {
                rc = PyInt_AS_LONG(PyTuple_GET_ITEM(args, 0));
            } else {
                rc = SQLITE_IOERR_LOCK;
            }
            Py_XDECREF(args);
            Py_XDECREF(etype);
            Py_XDECREF(evalue);
            Py_XDECREF(etb);
        } else {
            PyErr_Print();
            rc = SQLITE_IOERR_LOCK;
        }
    }
    Py_XDECREF(result);
    Py_XDECREF((PyObject*) real_conn);
    Py_XDECREF(orig_xLock_py);

    PyGILState_Release(gstate);
    return rc;
}

static void lockmanager_warn_buggy_lock(sqlite3_file *file)
{
    PyGILState_STATE gstate;
    my_io_methods *methods = (my_io_methods *) file->pMethods;
    PyObject *filename_bytes = NULL;
    const char *filename = "<filename unavailable>";

    gstate = PyGILState_Ensure();

    filename_bytes = PyUnicode_AsUTF8String(methods->filename);
    if (filename_bytes)
        filename = PyString_AsString(filename_bytes);

    PySys_WriteStderr(
            "pysqlite2: Buggy lock manager: failed to lock file '%s'.\n",
            filename);
    Py_XDECREF(filename_bytes);
    PyGILState_Release(gstate);
}


/*
   Creates a python callable (as PyObject) that forwards calls to the
   original xLock function for the given sqlite3_file in *file*.
*/
static PyObject *create_orig_xLock_py(sqlite3_file *file)
{
    PyObject *vfs_file = NULL, *result = NULL;

    static PyMethodDef methoddef = {
        "xLock", orig_xLock_trampoline, METH_VARARGS, "Original VFS xLock method."
    };

    vfs_file = PyCObject_FromVoidPtr(file, NULL);
    if (!vfs_file)
        return NULL;
    result = PyCFunction_New(&methoddef, vfs_file);
    Py_DECREF(vfs_file);
    return result;
}

static PyObject *orig_xLock_trampoline(PyObject *self, PyObject *args)
{
    sqlite3_file *file = PyCObject_AsVoidPtr(self);
    my_io_methods *methods = (my_io_methods *) file->pMethods;
    int rc;
    int lock_mode = 0;

    if (!PyArg_ParseTuple(args, "i", &lock_mode))
        return NULL;

    Py_BEGIN_ALLOW_THREADS
    rc = methods->orig_xLock(file, lock_mode);
    Py_END_ALLOW_THREADS

    if (rc == SQLITE_OK) {
        Py_RETURN_NONE;
    }
    else {
        PyObject *exc = PyObject_CallFunction(pysqlite_CallbackError, "i", rc);
        if (exc)
            PyErr_SetObject(pysqlite_CallbackError, exc);
        return NULL;
    }
}

/* Wrapper for xUnlock of the original VFS.

   This wrapper is needed for symmetry with the implementation of xLock.
   We need to know when a lock is released to correctly implement a better
   locking behaviour.
*/
static int wrapped_xUnlock(sqlite3_file *file, int lock_mode)
{
    int rc;
    my_io_methods *methods = (my_io_methods *) file->pMethods;
    PyObject *result = NULL;
    PyObject *connection = NULL;
    PyGILState_STATE gstate;

    rc = methods->orig_xUnlock(file, lock_mode);
    if (rc == SQLITE_OK) {
        /* This code can be called during exception handling (for example if opening
           the database fails). In that case, a Python exception can be set. If we
           call back into Python in that state, we will immediately get back that
           exception and swallow it in PyErr_Print. Therefore we need to save the
           Python error state first and restore it after the xUnlock method is done.

           Failing to do so will give us the following exception:
             SystemError: error return without exception set
        */
        PyObject *etype, *evalue, *etraceback;
        gstate = PyGILState_Ensure();

        PyErr_Fetch(&etype, &evalue, &etraceback);
        connection = methods->weak_connection;
        result = PyObject_CallMethod(methods->lock_manager, "unlock", "OiO", methods->filename, lock_mode, connection);
        if (!result) {
            PyErr_Print();
            rc = SQLITE_IOERR_UNLOCK;
        }
        Py_XDECREF(result);
        PyErr_Restore(etype, evalue, etraceback);
        PyGILState_Release(gstate);
    }

    return rc;
}

static int lookup_lock_manager(my_io_methods *methods)
{
    PyObject *module = NULL, *p = NULL;

    module = PyImport_ImportModuleNoBlock("pysqlite2.lock_manager");
    if (!module)
        goto error_out;

    p = methods->lock_manager = PyObject_GetAttrString(module, "_lock_manager");
    if (!p)
        goto error_out;
    p = methods->lock_manager_DeadlockError = PyObject_GetAttrString(module, "DeadlockError");
    if (!p)
        goto error_out;

    Py_DECREF(module);
    return SQLITE_OK;

error_out:
    PyErr_Print();

    Py_XDECREF(module);
    Py_XDECREF(methods->lock_manager);
    methods->lock_manager = NULL;
    Py_XDECREF(methods->lock_manager_DeadlockError);
    methods->lock_manager_DeadlockError = NULL;
    return SQLITE_ERROR;
}


int pysqlite_vfs_setup_types()
{
    pysqlite_CallbackError = PyErr_NewException("pysqlite2.CallbackError", PyExc_Exception, NULL);
    return pysqlite_CallbackError ? 0 : -1;
}
