#define PY_SSIZE_T_CLEAN

#include <Python.h>
#include <errno.h>
#include <liburing.h>
#include <limits.h>
#include <stdbool.h>
#include <string.h>

#ifndef IO_URING_VERSION_MAJOR
#define IO_URING_VERSION_MAJOR 0
#endif

#ifndef IO_URING_VERSION_MINOR
#define IO_URING_VERSION_MINOR 0
#endif

typedef struct {
    PyObject_HEAD
    struct io_uring ring;
    bool initialized;
} UringApiRing;

static PyTypeObject UringApiRing_Type;

static int normalize_ret_errno(int ret) {
    if (ret < 0) {
        return -ret;
    }
    if (errno) {
        return errno;
    }
    return EINVAL;
}

static PyObject *liburing_version_string(void) {
    return PyUnicode_FromFormat("%d.%d", IO_URING_VERSION_MAJOR, IO_URING_VERSION_MINOR);
}

static int dict_set_owned(PyObject *dict, const char *key, PyObject *value) {
    int ret;
    if (!value) {
        return -1;
    }
    ret = PyDict_SetItemString(dict, key, value);
    Py_DECREF(value);
    return ret;
}

static int parse_entries_flags(PyObject *args, PyObject *kwargs, unsigned int default_entries, unsigned int *entries,
                               unsigned int *flags) {
    static char *keywords[] = {"entries", "flags", NULL};
    unsigned long entries_value = default_entries;
    unsigned long flags_value = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|kk", keywords, &entries_value, &flags_value)) {
        return -1;
    }
    if (entries_value == 0 || entries_value > UINT_MAX) {
        PyErr_SetString(PyExc_ValueError, "entries must be between 1 and UINT_MAX");
        return -1;
    }
    if (flags_value > UINT_MAX) {
        PyErr_SetString(PyExc_ValueError, "flags must fit in an unsigned int");
        return -1;
    }
    *entries = (unsigned int)entries_value;
    *flags = (unsigned int)flags_value;
    return 0;
}

static PyObject *build_probe_result(bool available, int errnum, const char *message, struct io_uring_params *params) {
    PyObject *result = PyDict_New();
    if (!result) {
        return NULL;
    }

    if (PyDict_SetItemString(result, "available", available ? Py_True : Py_False) < 0 ||
        dict_set_owned(result, "errno", errnum ? PyLong_FromLong(errnum) : Py_NewRef(Py_None)) < 0 ||
        dict_set_owned(result, "message", message ? PyUnicode_FromString(message) : Py_NewRef(Py_None)) < 0 ||
        dict_set_owned(result, "features", PyLong_FromUnsignedLong(params ? params->features : 0)) < 0 ||
        dict_set_owned(result, "sq_entries", PyLong_FromUnsignedLong(params ? params->sq_entries : 0)) < 0 ||
        dict_set_owned(result, "cq_entries", PyLong_FromUnsignedLong(params ? params->cq_entries : 0)) < 0 ||
        dict_set_owned(result, "liburing_version", liburing_version_string()) < 0) {
        Py_DECREF(result);
        return NULL;
    }
    return result;
}

static PyObject *uring_api_probe(PyObject *self, PyObject *args, PyObject *kwargs) {
    struct io_uring ring;
    struct io_uring_params params;
    unsigned int entries;
    unsigned int flags;
    int ret;

    if (parse_entries_flags(args, kwargs, 2, &entries, &flags) < 0) {
        return NULL;
    }

    memset(&ring, 0, sizeof(ring));
    memset(&params, 0, sizeof(params));
    params.flags = flags;

    errno = 0;
    Py_BEGIN_ALLOW_THREADS
    ret = io_uring_queue_init_params(entries, &ring, &params);
    Py_END_ALLOW_THREADS

    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        return build_probe_result(false, errnum, strerror(errnum), &params);
    }

    io_uring_queue_exit(&ring);
    return build_probe_result(true, 0, NULL, &params);
}

static int UringApiRing_init(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    struct io_uring_params params;
    unsigned int entries;
    unsigned int flags;
    int ret;

    if (parse_entries_flags(args, kwargs, 8, &entries, &flags) < 0) {
        return -1;
    }

    if (self->initialized) {
        io_uring_queue_exit(&self->ring);
        self->initialized = false;
    }

    memset(&self->ring, 0, sizeof(self->ring));
    memset(&params, 0, sizeof(params));
    params.flags = flags;

    errno = 0;
    Py_BEGIN_ALLOW_THREADS
    ret = io_uring_queue_init_params(entries, &self->ring, &params);
    Py_END_ALLOW_THREADS

    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        errno = errnum;
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }

    self->initialized = true;
    return 0;
}

static void UringApiRing_dealloc(UringApiRing *self) {
    if (self->initialized) {
        io_uring_queue_exit(&self->ring);
        self->initialized = false;
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *UringApiRing_close(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    if (self->initialized) {
        io_uring_queue_exit(&self->ring);
        self->initialized = false;
    }
    Py_RETURN_NONE;
}

static PyObject *UringApiRing_enter(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    Py_INCREF(self);
    return (PyObject *)self;
}

static PyObject *UringApiRing_exit(UringApiRing *self, PyObject *args) {
    if (self->initialized) {
        io_uring_queue_exit(&self->ring);
        self->initialized = false;
    }
    Py_RETURN_NONE;
}

static PyObject *UringApiRing_get_fd(UringApiRing *self, void *closure) {
    if (!self->initialized) {
        return PyLong_FromLong(-1);
    }
    return PyLong_FromLong(self->ring.ring_fd);
}

static PyObject *UringApiRing_get_features(UringApiRing *self, void *closure) {
    if (!self->initialized) {
        return PyLong_FromUnsignedLong(0);
    }
    return PyLong_FromUnsignedLong(self->ring.features);
}

static PyObject *UringApiRing_get_sq_entries(UringApiRing *self, void *closure) {
    if (!self->initialized) {
        return PyLong_FromUnsignedLong(0);
    }
    return PyLong_FromUnsignedLong(self->ring.sq.ring_entries);
}

static PyObject *UringApiRing_get_cq_entries(UringApiRing *self, void *closure) {
    if (!self->initialized) {
        return PyLong_FromUnsignedLong(0);
    }
    return PyLong_FromUnsignedLong(self->ring.cq.ring_entries);
}

static PyObject *UringApiRing_get_closed(UringApiRing *self, void *closure) {
    if (self->initialized) {
        Py_RETURN_FALSE;
    }
    Py_RETURN_TRUE;
}

static PyMethodDef UringApiRing_methods[] = {{"close", (PyCFunction)UringApiRing_close, METH_NOARGS,
                                             "Close the io_uring instance."},
                                            {"__enter__", (PyCFunction)UringApiRing_enter, METH_NOARGS, NULL},
                                            {"__exit__", (PyCFunction)UringApiRing_exit, METH_VARARGS, NULL},
                                            {NULL, NULL, 0, NULL}};

static PyGetSetDef UringApiRing_getset[] = {{"fd", (getter)UringApiRing_get_fd, NULL, NULL, NULL},
                                            {"features", (getter)UringApiRing_get_features, NULL, NULL, NULL},
                                            {"sq_entries", (getter)UringApiRing_get_sq_entries, NULL, NULL, NULL},
                                            {"cq_entries", (getter)UringApiRing_get_cq_entries, NULL, NULL, NULL},
                                            {"closed", (getter)UringApiRing_get_closed, NULL, NULL, NULL},
                                            {NULL, NULL, NULL, NULL, NULL}};

static PyTypeObject UringApiRing_Type = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_uring_api.Ring",
    .tp_basicsize = sizeof(UringApiRing),
    .tp_dealloc = (destructor)UringApiRing_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "io_uring ring",
    .tp_methods = UringApiRing_methods,
    .tp_getset = UringApiRing_getset,
    .tp_init = (initproc)UringApiRing_init,
    .tp_new = PyType_GenericNew,
};

static PyMethodDef uring_api_methods[] = {
    {"probe", _PyCFunction_CAST(uring_api_probe), METH_VARARGS | METH_KEYWORDS,
     "Probe whether a minimal io_uring instance can be created."},
    {NULL, NULL, 0, NULL},
};

static int uring_api_exec(PyObject *module) {
    PyObject *version = NULL;

    if (PyType_Ready(&UringApiRing_Type) < 0) {
        return -1;
    }
    Py_INCREF(&UringApiRing_Type);
    if (PyModule_AddObject(module, "Ring", (PyObject *)&UringApiRing_Type) < 0) {
        Py_DECREF(&UringApiRing_Type);
        return -1;
    }

    version = liburing_version_string();
    if (!version) {
        return -1;
    }
    if (PyModule_AddObject(module, "__liburing_version__", version) < 0) {
        Py_DECREF(version);
        return -1;
    }

    return 0;
}

/* CPython API uses void* in module slots; this conversion is intentional. */
#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
static PyModuleDef_Slot uring_api_slots[] = {{Py_mod_exec, uring_api_exec},
#if defined(Py_mod_gil)
                                             {Py_mod_gil, Py_MOD_GIL_NOT_USED},
#endif
                                             {0, NULL}};
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

static struct PyModuleDef uring_api_module = {PyModuleDef_HEAD_INIT,
                                             "_uring_api",
                                             "Small wrapper around Linux io_uring.",
                                             0,
                                             uring_api_methods,
                                             uring_api_slots,
                                             NULL,
                                             NULL,
                                             NULL};

PyMODINIT_FUNC PyInit__uring_api(void) { return PyModuleDef_Init(&uring_api_module); }