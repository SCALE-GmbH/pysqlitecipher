#include "Python.h"
#include "module.h"
#include "vfs.h"

PyTypeObject pysqlite_VFSType;
PyTypeObject pysqlite_VFSFileType;

static char vfs_doc[] =
PyDoc_STR("SQLite VFS (virtual file system) object.");


/*!
    Gets the version int property from VFS \a self.
 */
static PyObject *
vfs_get_version(pysqlite_VFS *self, void *unused)
{
    return PyInt_FromLong(self->real_vfs->iVersion);
}

/*!
    Gets the name string property from VFS \a self.
 */
static PyObject *
vfs_get_name(pysqlite_VFS *self, void *unused)
{
    return PyString_FromString(self->real_vfs->zName);
}

/*!
   Convert input flags in \a flags_in into \a result.
   This is a callback function for PyArg_ParseTupleAndKeywords.
   \return 1 on success, 0 on error
 */
static int
pysqlite_convert_flags(PyObject *flags_in, void *result)
{
    long value = PyLong_AsLong(flags_in);
    if (value == -1 && PyErr_Occurred())
        return 0;

    *(int*) result = (int) value;
    return 1;
}

static char vfs_open_doc[] = PyDoc_STR(
"open(name, flags=None) -> VFSFile\n\
\n\
Open the file name via the VFS and return the open file as VFSFile. Open options\n\
can be specified by passing a sequence of strings as flags. For a list of known\n\
flags see https://www.sqlite.org/c3ref/c_open_autoproxy.html. To open the file\n\
in read-only mode pass flags=['OPEN_READONLY].");

/*!
    Wraps the xOpen method of an SQLite VFS \a self for Python.
    \return VFSFile object on success, NULL on error (Python error set)
 */
static pysqlite_VFSFile *
vfs_open(pysqlite_VFS *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"name", "flags", 0};
    char *name = NULL;
    int res = 0, flags = 0, outflags = 0;
    sqlite3_vfs *real_vfs = self->real_vfs;
    pysqlite_VFSFile *vfs_file = (pysqlite_VFSFile *) PyType_GenericAlloc(&pysqlite_VFSFileType, 0);

    if (!vfs_file)
        return NULL;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "es|O&:VFS.open", kwlist,
            "utf-8", &name, pysqlite_convert_flags, &flags))
        goto error_out;

    vfs_file->filename = name;
    vfs_file->real_file = PyMem_Malloc(real_vfs->szOsFile);
    if (!vfs_file->real_file)
        goto error_out;

    Py_BEGIN_ALLOW_THREADS
    res = real_vfs->xOpen(real_vfs, vfs_file->filename, vfs_file->real_file, flags, &outflags);
    Py_END_ALLOW_THREADS

    if (res != SQLITE_OK) {
        PyErr_SetString(pysqlite_DatabaseError, sqlite3_errstr(res));
        goto error_out;
    }
    return vfs_file;

error_out:
    Py_DECREF(vfs_file);
    return NULL;
}


static PyGetSetDef vfs_getset[] = {
    {"version", (getter) vfs_get_version, (setter) 0},
    {"name", (getter) vfs_get_name, (setter) 0},
    {NULL}
};

static PyMethodDef vfs_methods[] = {
    {"open", (PyCFunction) vfs_open, METH_VARARGS|METH_KEYWORDS, vfs_open_doc},
#if 0
    {"backup", (PyCFunction)pysqlite_connection_backup, METH_VARARGS|METH_KEYWORDS,
        PyDoc_STR("Backup database.")},
#endif
    {NULL}
};


static void
vfs_dealloc(pysqlite_VFS *self)
{
    self->real_vfs = NULL;
    self->ob_type->tp_free((PyObject*) self);
}

static PyObject *
vfs_from_real(sqlite3_vfs *real_vfs)
{
    pysqlite_VFS *self = (pysqlite_VFS *) PyType_GenericAlloc(&pysqlite_VFSType, 0);
    if (!self)
        return NULL;
    self->real_vfs = real_vfs;
    return (PyObject *) self;
}

static PyObject *
vfs_new(PyTypeObject *subtype, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"name", 0};
    const char *name = NULL;
    sqlite3_vfs *real_vfs = NULL;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|s:VFS", kwlist, &name))
        return NULL;

    real_vfs = sqlite3_vfs_find(name);
    if (!real_vfs) {
        if (name)
            return PyErr_Format(PyExc_KeyError, "SQLite VFS %s not found", name);
        else
            return PyErr_Format(PyExc_RuntimeError, "no SQLite VFS found");
    }

    return vfs_from_real(real_vfs);
}


PyTypeObject pysqlite_VFSType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        MODULE_NAME ".VFS",                             /* tp_name */
        sizeof(pysqlite_VFS),                           /* tp_basicsize */
        0,                                              /* tp_itemsize */
        (destructor)vfs_dealloc,                        /* tp_dealloc */
        0,                                              /* tp_print */
        0,                                              /* tp_getattr */
        0,                                              /* tp_setattr */
        0,                                              /* tp_compare */
        0,                                              /* tp_repr */
        0,                                              /* tp_as_number */
        0,                                              /* tp_as_sequence */
        0,                                              /* tp_as_mapping */
        0,                                              /* tp_hash */
        0,                                              /* tp_call */
        0,                                              /* tp_str */
        0,                                              /* tp_getattro */
        0,                                              /* tp_setattro */
        0,                                              /* tp_as_buffer */
        Py_TPFLAGS_DEFAULT,                             /* tp_flags */
        vfs_doc,                                        /* tp_doc */
        0,                                              /* tp_traverse */
        0,                                              /* tp_clear */
        0,                                              /* tp_richcompare */
        0,                                              /* tp_weaklistoffset */
        0,                                              /* tp_iter */
        0,                                              /* tp_iternext */
        vfs_methods,                                    /* tp_methods */
        0,                                              /* tp_members */
        vfs_getset,                                     /* tp_getset */
        0,                                              /* tp_base */
        0,                                              /* tp_dict */
        0,                                              /* tp_descr_get */
        0,                                              /* tp_descr_set */
        0,                                              /* tp_dictoffset */
        0,                                              /* tp_init */
        0,                                              /* tp_alloc */
        (newfunc)vfs_new,                               /* tp_new */
        0                                               /* tp_free */
};


static char vfs_file_doc[] =
PyDoc_STR("SQLite VFS (virtual file system) file object.");

/*!
  Free resources held by VFSFile Python object \a self.
 */
static void
vfs_file_dealloc(pysqlite_VFSFile *self)
{
    int status;
    sqlite3_file *real_file = self->real_file;
    self->real_file = NULL;

    if (real_file) {
        if (real_file->pMethods) {
            Py_BEGIN_ALLOW_THREADS
            status = real_file->pMethods->xClose(real_file);
            Py_END_ALLOW_THREADS
        }
        PyMem_Free(real_file);
    }

    self->ob_type->tp_free((PyObject*) self);

    if (status != SQLITE_OK) {
        PyErr_SetString(pysqlite_DatabaseError, sqlite3_errstr(status));
    }
}


static PyMethodDef vfs_file_methods[] = {
#if 0
    {"backup", (PyCFunction)pysqlite_connection_backup, METH_VARARGS|METH_KEYWORDS,
        PyDoc_STR("Backup database.")},
#endif
    {NULL}
};

PyTypeObject pysqlite_VFSFileType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        MODULE_NAME ".VFSFile",                         /* tp_name */
        sizeof(pysqlite_VFSFile),                       /* tp_basicsize */
        0,                                              /* tp_itemsize */
        (destructor)vfs_file_dealloc,                   /* tp_dealloc */
        0,                                              /* tp_print */
        0,                                              /* tp_getattr */
        0,                                              /* tp_setattr */
        0,                                              /* tp_compare */
        0,                                              /* tp_repr */
        0,                                              /* tp_as_number */
        0,                                              /* tp_as_sequence */
        0,                                              /* tp_as_mapping */
        0,                                              /* tp_hash */
        0,                                              /* tp_call */
        0,                                              /* tp_str */
        0,                                              /* tp_getattro */
        0,                                              /* tp_setattro */
        0,                                              /* tp_as_buffer */
        Py_TPFLAGS_DEFAULT,                             /* tp_flags */
        vfs_file_doc,                                   /* tp_doc */
        0,                                              /* tp_traverse */
        0,                                              /* tp_clear */
        0,                                              /* tp_richcompare */
        0,                                              /* tp_weaklistoffset */
        0,                                              /* tp_iter */
        0,                                              /* tp_iternext */
        vfs_file_methods,                               /* tp_methods */
        0,                                              /* tp_members */
        0,                                              /* tp_getset */
        0,                                              /* tp_base */
        0,                                              /* tp_dict */
        0,                                              /* tp_descr_get */
        0,                                              /* tp_descr_set */
        0,                                              /* tp_dictoffset */
        0,                                              /* tp_init */
        0,                                              /* tp_alloc */
        0,                                              /* tp_new */
        0                                               /* tp_free */
};


/*!
  Add constant integer \a value as \a name to Python object \a target.
  \return -1 on failure (Python error set), 0 on success
 */
static inline int
set_item_int(PyObject *target, const char *name, long value)
{
    int status = -1;
    PyObject *py_value = NULL;

    py_value = PyInt_FromLong(value);
    if (py_value)
        status = PyMapping_SetItemString(target, (char *) name, py_value);

out:
    Py_XDECREF(py_value);
    return status;
}

/*!
   Export SQLITE_OPEN_... constants to the \a target dictionary/mapping.
   \param target Python mapping object to add attributes to
   \return -1 on failure (Python error set), 0 otherwise
 */
static int vfs_add_open_constants(PyObject *target)
{
#   define EXPORT_CONSTANT(name) do { \
        if (set_item_int(target, #name + 7, name)) \
            return -1; \
    } while(0)

    EXPORT_CONSTANT(SQLITE_OPEN_READONLY);
    EXPORT_CONSTANT(SQLITE_OPEN_READWRITE);
    EXPORT_CONSTANT(SQLITE_OPEN_CREATE);
    EXPORT_CONSTANT(SQLITE_OPEN_DELETEONCLOSE);
    EXPORT_CONSTANT(SQLITE_OPEN_EXCLUSIVE);
    EXPORT_CONSTANT(SQLITE_OPEN_AUTOPROXY);
    EXPORT_CONSTANT(SQLITE_OPEN_URI);
    EXPORT_CONSTANT(SQLITE_OPEN_MEMORY);
    EXPORT_CONSTANT(SQLITE_OPEN_MAIN_DB);
    EXPORT_CONSTANT(SQLITE_OPEN_TEMP_DB);
    EXPORT_CONSTANT(SQLITE_OPEN_TRANSIENT_DB);
    EXPORT_CONSTANT(SQLITE_OPEN_MAIN_JOURNAL);
    EXPORT_CONSTANT(SQLITE_OPEN_TEMP_JOURNAL);
    EXPORT_CONSTANT(SQLITE_OPEN_SUBJOURNAL);
    EXPORT_CONSTANT(SQLITE_OPEN_MASTER_JOURNAL);
    EXPORT_CONSTANT(SQLITE_OPEN_NOMUTEX);
    EXPORT_CONSTANT(SQLITE_OPEN_FULLMUTEX);
    EXPORT_CONSTANT(SQLITE_OPEN_SHAREDCACHE);
    EXPORT_CONSTANT(SQLITE_OPEN_PRIVATECACHE);
    EXPORT_CONSTANT(SQLITE_OPEN_WAL);

    return 0;
#   undef EXPORT_CONSTANT
}


int pysqlite_vfs_register(PyObject *module)
{
    int status = PyType_Ready(&pysqlite_VFSType);
    if (status < 0)
        return status;

    status = vfs_add_open_constants(pysqlite_VFSType.tp_dict);
    if (status < 0)
        return status;

    status = PyType_Ready(&pysqlite_VFSFileType);
    if (status < 0)
        return status;

    Py_INCREF(&pysqlite_VFSType);
    PyModule_AddObject(module, "VFS", (PyObject*)&pysqlite_VFSType);
    return 0;
}
