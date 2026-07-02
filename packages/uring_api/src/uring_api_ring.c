/*
 * Ring lifecycle methods for the _uring_api extension.
 */

#include "uring_api_ring.h"
#include "uring_api_bufgroup.h"
#include "uring_api_bufview.h"
#include "uring_api_core.h"
#include "uring_api_dispatch.h"
#include "uring_api_submit.h"

PyObject *UringApiRing_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    UringApiRing *self = (UringApiRing *)type->tp_alloc(type, 0);

    (void)args;
    (void)kwargs;
    if (!self) {
        return NULL;
    }

#ifdef URING_API_USE_PYTHREAD_RING_LOCK
    self->ring_lock = PyThread_allocate_lock();
    if (!self->ring_lock) {
        PyErr_NoMemory();
        PyObject_GC_Del(self);
        return NULL;
    }
#endif
#ifdef URING_API_USE_PYTHREAD_MUTEX
    self->receive_mutex = PyThread_allocate_lock();
    if (!self->receive_mutex) {
#ifdef URING_API_USE_PYTHREAD_RING_LOCK
        PyThread_free_lock(self->ring_lock);
        self->ring_lock = NULL;
#endif
        PyErr_NoMemory();
        PyObject_GC_Del(self);
        return NULL;
    }
#endif
    return (PyObject *)self;
}

int UringApiRing_init(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    struct io_uring_params params;
    unsigned int entries;
    unsigned int flags;
    int ret;
    int failed = 0;

    if (parse_entries_flags(args, kwargs, 8, &entries, &flags) < 0) {
        return -1;
    }

    if (delivery_check_not_running(self) < 0) {
        return -1;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (self->initialized) {
        io_uring_queue_exit(&self->ring);
        self->initialized = false;
    }
    self->receive_state = URING_API_RECEIVE_IDLE;
    self->delivery_stop_requested = false;
    self->delivery_active_workers = 0;
    self->next_buf_group = 1;
    self->setup_flags = flags;
    self->owner_thread_id = 0;

    memset(&self->ring, 0, sizeof(self->ring));
    memset(&params, 0, sizeof(params));
    params.flags = flags;

    errno = 0;
    Py_BEGIN_ALLOW_THREADS;
    ret = io_uring_queue_init_params(entries, &self->ring, &params);
    Py_END_ALLOW_THREADS;

    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        errno = errnum;
        PyErr_SetFromErrno(PyExc_OSError);
        failed = 1;
    } else {
        self->initialized = true;
    }
    Py_END_CRITICAL_SECTION();

    return failed ? -1 : 0;
}

void UringApiRing_dealloc(UringApiRing *self) {
    PyObject_GC_UnTrack(self);
    (void)UringApiRing_stop_delivery(self);
    if (self->initialized) {
        io_uring_queue_exit(&self->ring);
        self->initialized = false;
    }
    (void)UringApiRing_clear(self);
    UringApiRing_clear_free_buf_group_ids(self);
    self->c_delivery_callback = NULL;
    self->c_delivery_callback_user_data = NULL;
    if (self->delivery_wait_lock) {
        PyThread_free_lock(self->delivery_wait_lock);
        self->delivery_wait_lock = NULL;
    }
#ifdef URING_API_USE_PYTHREAD_MUTEX
    if (self->receive_mutex) {
        PyThread_free_lock(self->receive_mutex);
        self->receive_mutex = NULL;
    }
#endif
#ifdef URING_API_USE_PYTHREAD_RING_LOCK
    if (self->ring_lock) {
        PyThread_free_lock(self->ring_lock);
        self->ring_lock = NULL;
    }
#endif
    PyObject_GC_Del(self);
}

int UringApiRing_traverse(UringApiRing *self, visitproc visit, void *arg) {
    Py_VISIT(self->delivery_callback);
    return 0;
}

int UringApiRing_clear(UringApiRing *self) {
    Py_CLEAR(self->delivery_callback);
    return 0;
}

PyObject *UringApiRing_close(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    if (delivery_check_not_running(self) < 0) {
        return NULL;
    }
    Py_BEGIN_CRITICAL_SECTION(self);
    if (self->initialized) {
        io_uring_queue_exit(&self->ring);
        self->initialized = false;
    }
    self->receive_state = URING_API_RECEIVE_IDLE;
    self->delivery_stop_requested = false;
    self->delivery_active_workers = 0;
    self->next_buf_group = 1;
    UringApiRing_clear_free_buf_group_ids(self);
    self->setup_flags = 0;
    self->owner_thread_id = 0;
    Py_END_CRITICAL_SECTION();
    Py_RETURN_NONE;
}

PyObject *UringApiRing_enter(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    Py_INCREF(self);
    return (PyObject *)self;
}

PyObject *UringApiRing_exit(UringApiRing *self, PyObject *args) { return UringApiRing_close(self, NULL); }

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
    return PyLong_FromUnsignedLong(ring_sq_entries(self));
}

static PyObject *UringApiRing_get_cq_entries(UringApiRing *self, void *closure) {
    if (!self->initialized) {
        return PyLong_FromUnsignedLong(0);
    }
    return PyLong_FromUnsignedLong(ring_cq_entries(self));
}

static PyObject *UringApiRing_get_closed(UringApiRing *self, void *closure) {
    if (self->initialized) {
        Py_RETURN_FALSE;
    }
    Py_RETURN_TRUE;
}

static PyObject *UringApiRing_get_running(UringApiRing *self, void *closure) {
    if (self->receive_state == URING_API_RECEIVE_DELIVERING) {
        Py_RETURN_TRUE;
    }
    Py_RETURN_FALSE;
}

static PyObject *UringApiRing_get_callback(UringApiRing *self, void *closure) {
    PyObject *callback;

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    callback = Py_XNewRef(self->delivery_callback);
    Py_END_CRITICAL_SECTION_MUTEX();
    if (!callback) {
        Py_RETURN_NONE;
    }
    return callback;
}

int UringApiRing_set_callback(UringApiRing *self, PyObject *value, void *closure) {
    PyObject *callback;
    PyObject *old_callback = NULL;
    int ret = 0;

    if (!value) {
        PyErr_SetString(PyExc_TypeError, "cannot delete callback");
        return -1;
    }
    if (value != Py_None && !PyCallable_Check(value)) {
        PyErr_SetString(PyExc_TypeError, "callback must be callable or None");
        return -1;
    }

    callback = value == Py_None ? NULL : Py_NewRef(value);
    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    if (delivery_is_running_locked(self)) {
        PyErr_SetString(PyExc_RuntimeError, "cannot change callback while completion service is active");
        ret = -1;
    } else {
        old_callback = self->delivery_callback;
        self->delivery_callback = callback;
        callback = NULL;
    }
    Py_END_CRITICAL_SECTION_MUTEX();
    Py_XDECREF(callback);
    Py_XDECREF(old_callback);
    return ret;
}

static PyMethodDef UringApiRing_methods[] = {
    {"close", (PyCFunction)UringApiRing_close, METH_NOARGS, "Close the io_uring instance."},
    {"serve_completions", (PyCFunction)UringApiRing_serve_completions, METH_NOARGS,
     "Serve completions until stop_serving is called."},
    {"stop_serving", (PyCFunction)UringApiRing_stop_serving, METH_NOARGS, "Ask completion workers to stop."},
    {"reset_serving", (PyCFunction)UringApiRing_reset_serving, METH_NOARGS, "Clear the completion service stop flag."},
    {"create_buf_group", _PyCFunction_CAST(UringApiRing_create_buf_group), METH_VARARGS | METH_KEYWORDS,
     "Create a provided-buffer group for multishot receive operations."},
    {"create_buf_view", _PyCFunction_CAST(UringApiRing_create_buf_view), METH_VARARGS | METH_KEYWORDS,
     "Create a read-only leased view into a buffer group slot."},
    {"submit_recv", _PyCFunction_CAST(UringApiRing_submit_recv), METH_VARARGS | METH_KEYWORDS,
     "Submit a recv operation."},
    {"submit_recv_buf", _PyCFunction_CAST(UringApiRing_submit_recv_buf), METH_VARARGS | METH_KEYWORDS,
     "Submit a one-shot provided-buffer recv operation."},
    {"submit_recv_multishot", _PyCFunction_CAST(UringApiRing_submit_recv_multishot), METH_VARARGS | METH_KEYWORDS,
     "Submit a multishot provided-buffer recv operation."},
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
    {"submit_poll", _PyCFunction_CAST(UringApiRing_submit_poll), METH_VARARGS | METH_KEYWORDS,
     "Submit a one-shot poll operation."},
    {"submit_poll_multishot", _PyCFunction_CAST(UringApiRing_submit_poll_multishot), METH_VARARGS | METH_KEYWORDS,
     "Submit a multishot poll operation."},
    {"submit_poll_remove", _PyCFunction_CAST(UringApiRing_submit_poll_remove), METH_VARARGS | METH_KEYWORDS,
     "Remove a previously submitted poll request."},
    {"submit_cancel", _PyCFunction_CAST(UringApiRing_submit_cancel), METH_VARARGS | METH_KEYWORDS,
     "Submit an async cancel operation targeting a pending completion."},
    {"submit_shutdown", _PyCFunction_CAST(UringApiRing_submit_shutdown), METH_VARARGS | METH_KEYWORDS,
     "Submit a socket shutdown operation."},
    {"submit_close", _PyCFunction_CAST(UringApiRing_submit_close), METH_VARARGS | METH_KEYWORDS,
     "Submit a close operation for a caller-owned fd."},
    {"submit_read", _PyCFunction_CAST(UringApiRing_submit_read), METH_VARARGS | METH_KEYWORDS,
     "Submit a file read operation at an explicit offset."},
    {"submit_write", _PyCFunction_CAST(UringApiRing_submit_write), METH_VARARGS | METH_KEYWORDS,
     "Submit a file write operation at an explicit offset."},
    {"submit_openat", _PyCFunction_CAST(UringApiRing_submit_openat), METH_VARARGS | METH_KEYWORDS,
     "Submit an openat operation and return a caller-owned fd on success."},
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
