/*
 * Module definition for the _uring_api extension.
 */

#include "uring_api_core.h"
#include "uring_api_dispatch.h"
#include "uring_api_probe.h"
#include "uring_api_properties.h"
#include "uring_api_ring.h"
#include "uring_api_submit.h"

PyObject *UringApiSubmissionQueueFullError;

static PyMethodDef UringApiRing_methods[] = {
    {"close", (PyCFunction)UringApiRing_close, METH_NOARGS, "Close the io_uring instance."},
    {"serve_completions", (PyCFunction)UringApiRing_serve_completions, METH_NOARGS,
     "Serve completions until stop_serving is called."},
    {"stop_serving", (PyCFunction)UringApiRing_stop_serving, METH_NOARGS, "Ask completion workers to stop."},
    {"reset_serving", (PyCFunction)UringApiRing_reset_serving, METH_NOARGS, "Clear the completion service stop flag."},
    {"submit_recv", _PyCFunction_CAST(UringApiRing_submit_recv), METH_VARARGS | METH_KEYWORDS,
     "Submit a recv operation."},
    {"submit_recv_multishot", _PyCFunction_CAST(UringApiRing_submit_recv_multishot), METH_VARARGS | METH_KEYWORDS,
     "Submit a multishot recv operation."},
    {"submit_send", _PyCFunction_CAST(UringApiRing_submit_send), METH_VARARGS | METH_KEYWORDS,
     "Submit a send operation."},
    {"submit_send_zc", _PyCFunction_CAST(UringApiRing_submit_send_zc), METH_VARARGS | METH_KEYWORDS,
     "Submit a zero-copy send operation."},
    {"submit_recvmsg", _PyCFunction_CAST(UringApiRing_submit_recvmsg), METH_VARARGS | METH_KEYWORDS,
     "Submit a recvmsg operation."},
    {"submit_sendto", _PyCFunction_CAST(UringApiRing_submit_sendto), METH_VARARGS | METH_KEYWORDS,
     "Submit a sendto operation."},
    {"submit_sendmsg", _PyCFunction_CAST(UringApiRing_submit_sendmsg), METH_VARARGS | METH_KEYWORDS,
     "Submit a sendmsg operation."},
    {"submit_sendmsg_zc", _PyCFunction_CAST(UringApiRing_submit_sendmsg_zc), METH_VARARGS | METH_KEYWORDS,
     "Submit a zero-copy sendmsg operation."},
    {"submit_accept", _PyCFunction_CAST(UringApiRing_submit_accept), METH_VARARGS | METH_KEYWORDS,
     "Submit an accept operation."},
    {"submit_accept_multishot", _PyCFunction_CAST(UringApiRing_submit_accept_multishot), METH_VARARGS | METH_KEYWORDS,
     "Submit a multishot accept operation."},
    {"submit_connect", _PyCFunction_CAST(UringApiRing_submit_connect), METH_VARARGS | METH_KEYWORDS,
     "Submit a connect operation."},
    {"submit_cancel", _PyCFunction_CAST(UringApiRing_submit_cancel), METH_VARARGS | METH_KEYWORDS,
     "Submit an async cancel operation targeting a pending completion."},
    {"submit_shutdown", _PyCFunction_CAST(UringApiRing_submit_shutdown), METH_VARARGS | METH_KEYWORDS,
     "Submit a socket shutdown operation."},
    {"submit_close", _PyCFunction_CAST(UringApiRing_submit_close), METH_VARARGS | METH_KEYWORDS,
     "Submit a close operation for a caller-owned fd."},
    {"submit_socket", _PyCFunction_CAST(UringApiRing_submit_socket), METH_VARARGS | METH_KEYWORDS,
     "Submit a socket creation operation."},
    {"break_wait", (PyCFunction)UringApiRing_break_wait, METH_NOARGS,
     "Interrupt a thread blocked in wait without producing a user completion."},
    {"wait", _PyCFunction_CAST(UringApiRing_wait), METH_VARARGS | METH_KEYWORDS,
     "Wait for one completion and return its result."},
    {"__enter__", (PyCFunction)UringApiRing_enter, METH_NOARGS, NULL},
    {"__exit__", (PyCFunction)UringApiRing_exit, METH_VARARGS, NULL},
    {NULL, NULL, 0, NULL}};

static PyGetSetDef UringApiRing_getset[] = {
    {"fd", (getter)UringApiRing_get_fd, NULL, NULL, NULL},
    {"features", (getter)UringApiRing_get_features, NULL, NULL, NULL},
    {"sq_entries", (getter)UringApiRing_get_sq_entries, NULL, NULL, NULL},
    {"cq_entries", (getter)UringApiRing_get_cq_entries, NULL, NULL, NULL},
    {"closed", (getter)UringApiRing_get_closed, NULL, NULL, NULL},
    {"running", (getter)UringApiRing_get_running, NULL, NULL, NULL},
    {"callback", (getter)UringApiRing_get_callback, (setter)UringApiRing_set_callback, NULL, NULL},
    {NULL, NULL, NULL, NULL, NULL}};

PyTypeObject UringApiRing_Type = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_uring_api.Ring",
    .tp_basicsize = sizeof(UringApiRing),
    .tp_dealloc = (destructor)UringApiRing_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)UringApiRing_traverse,
    .tp_clear = (inquiry)UringApiRing_clear,
    .tp_doc = "io_uring ring",
    .tp_methods = UringApiRing_methods,
    .tp_getset = UringApiRing_getset,
    .tp_init = (initproc)UringApiRing_init,
    .tp_new = UringApiRing_new,
};

static PyGetSetDef UringApiCompletion_getset[] = {
    {"user_data", (getter)UringApiCompletion_get_user_data, NULL, NULL, NULL},
    {"kind", (getter)UringApiCompletion_get_kind, NULL, NULL, NULL},
    {"res", (getter)UringApiCompletion_get_res, NULL, NULL, NULL},
    {"flags", (getter)UringApiCompletion_get_flags, NULL, NULL, NULL},
    {"result", (getter)UringApiCompletion_get_result, NULL, NULL, NULL},
    {"sequence", (getter)UringApiCompletion_get_sequence, NULL, NULL, NULL},
    {NULL, NULL, NULL, NULL, NULL},
};

PyTypeObject UringApiCompletion_Type = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_uring_api.Completion",
    .tp_basicsize = sizeof(UringApiCompletion),
    .tp_dealloc = (destructor)UringApiCompletion_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)UringApiCompletion_traverse,
    .tp_clear = (inquiry)UringApiCompletion_clear,
    .tp_doc = "io_uring completion result",
    .tp_getset = UringApiCompletion_getset,
};

static PyMethodDef uring_api_methods[] = {
    {"probe", _PyCFunction_CAST(uring_api_probe), METH_VARARGS | METH_KEYWORDS,
     "Probe whether a minimal io_uring instance can be created."},
    {NULL, NULL, 0, NULL},
};

static int uring_api_exec(PyObject *module) {
    PyObject *version = NULL;
    PyObject *version_info = NULL;

    if (PyType_Ready(&UringApiCompletion_Type) < 0) {
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
    Py_INCREF(&UringApiRing_Type);
    if (PyModule_AddObject(module, "Ring", (PyObject *)&UringApiRing_Type) < 0) {
        Py_DECREF(&UringApiRing_Type);
        return -1;
    }
    if (module_add_setup_flag_constants(module) < 0 || module_add_cqe_flag_constants(module) < 0 ||
        module_add_recvsend_flag_constants(module) < 0 || module_add_completion_kind_constants(module) < 0) {
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