/*
 * Module definition for the _uring_api extension.
 */

#include "uring_api_bufgroup.h"
#include "uring_api_bufview.h"
#include "uring_api_completion.h"
#include "uring_api_core.h"
#include "uring_api_probe.h"
#include "uring_api_ring.h"
#include "uring_api_statx.h"

PyObject *UringApiSubmissionQueueFullError;

static PyMethodDef uring_api_methods[] = {
    {"probe", _PyCFunction_CAST(uring_api_probe), METH_VARARGS | METH_KEYWORDS,
     "Probe whether a minimal io_uring instance can be created."},
    {"statx_st_size", _PyCFunction_CAST(UringApiStatx_st_size), METH_O,
     "Read stx_size from a completed statx buffer filled with STATX_SIZE."},
    {NULL, NULL, 0, NULL},
};

static int uring_api_exec(PyObject *module) {
    PyObject *version = NULL;
    PyObject *version_info = NULL;

    if (PyType_Ready(&UringApiCompletion_Type) < 0) {
        return -1;
    }
    if (PyType_Ready(&UringApiBufGroup_Type) < 0) {
        return -1;
    }
    if (PyType_Ready(&UringApiBufView_Type) < 0) {
        return -1;
    }
    if (PyType_Ready(&UringApiRing_Type) < 0) {
        return -1;
    }
    UringApiSubmissionQueueFullError = PyErr_NewException("_uring_api.SubmissionQueueFull", PyExc_RuntimeError, NULL);
    if (!UringApiSubmissionQueueFullError) {
        return -1;
    }
    if (PyModule_AddObjectRef(module, "SubmissionQueueFull", UringApiSubmissionQueueFullError) < 0) {
        return -1;
    }
    Py_INCREF(&UringApiCompletion_Type);
    if (PyModule_AddObject(module, "Completion", (PyObject *)&UringApiCompletion_Type) < 0) {
        Py_DECREF(&UringApiCompletion_Type);
        return -1;
    }
    Py_INCREF(&UringApiBufGroup_Type);
    if (PyModule_AddObject(module, "BufGroup", (PyObject *)&UringApiBufGroup_Type) < 0) {
        Py_DECREF(&UringApiBufGroup_Type);
        return -1;
    }
    Py_INCREF(&UringApiBufView_Type);
    if (PyModule_AddObject(module, "BufView", (PyObject *)&UringApiBufView_Type) < 0) {
        Py_DECREF(&UringApiBufView_Type);
        return -1;
    }
    Py_INCREF(&UringApiRing_Type);
    if (PyModule_AddObject(module, "Ring", (PyObject *)&UringApiRing_Type) < 0) {
        Py_DECREF(&UringApiRing_Type);
        return -1;
    }
    if (module_add_setup_flag_constants(module) < 0 || module_add_cqe_flag_constants(module) < 0 ||
        module_add_recvsend_flag_constants(module) < 0 || module_add_completion_kind_constants(module) < 0 ||
        module_add_statx_constants(module) < 0) {
        return -1;
    }

    version = liburing_version_string();
    if (!version) {
        return -1;
    }
    if (PyModule_AddObjectRef(module, "__liburing_version__", version) < 0 ||
        PyModule_AddObjectRef(module, "__compiled_liburing_version__", version) < 0) {
        Py_DECREF(version);
        return -1;
    }
    Py_DECREF(version);

    version_info = liburing_version_info();
    if (!version_info) {
        return -1;
    }
    if (PyModule_AddObject(module, "__compiled_liburing_version_info__", version_info) < 0) {
        Py_DECREF(version_info);
        return -1;
    }

    if (uring_api_export_capi(module) < 0) {
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