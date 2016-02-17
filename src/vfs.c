#include "Python.h"
#include "vfs.h"

PyTypeObject pysqlite_VFSType;

static char vfs_doc[] =
PyDoc_STR("SQLite VFS (virtual file system) object.");


static PyObject *
vfs_get_version(pysqlite_VFS *self, void *unused)
{
    return PyInt_FromLong(self->real_vfs->iVersion);
}

static PyObject *
vfs_get_name(pysqlite_VFS *self, void *unused)
{
    return PyString_FromString(self->real_vfs->zName);
}


static PyGetSetDef vfs_getset[] = {
    {"version", (getter) vfs_get_version, (setter) 0},
    {"name", (getter) vfs_get_name, (setter) 0},
    {NULL}
};

static PyMethodDef vfs_methods[] = {
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


int pysqlite_vfs_register(PyObject *module)
{
    int status = PyType_Ready(&pysqlite_VFSType);
    if (status < 0)
        return status;

    Py_INCREF(&pysqlite_VFSType);
    PyModule_AddObject(module, "VFS", (PyObject*)&pysqlite_VFSType);
    return 0;
}
