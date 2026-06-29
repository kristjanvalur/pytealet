/*
 * Ring lifecycle methods for the _uring_api extension.
 *
 * This file owns Ring initialisation, teardown, context-manager support, and
 * close handling. It is included by _uring_api.c as part of the single
 * extension translation unit.
 */

static PyObject *UringApiRing_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
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

static int UringApiRing_init(UringApiRing *self, PyObject *args, PyObject *kwargs) {
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

static void UringApiRing_dealloc(UringApiRing *self) {
    PyObject_GC_UnTrack(self);
    (void)UringApiRing_stop_delivery(self);
    if (self->initialized) {
        io_uring_queue_exit(&self->ring);
        self->initialized = false;
    }
    (void)UringApiRing_clear(self);
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

static int UringApiRing_traverse(UringApiRing *self, visitproc visit, void *arg) {
    Py_VISIT(self->delivery_callback);
    return 0;
}

static int UringApiRing_clear(UringApiRing *self) {
    Py_CLEAR(self->delivery_callback);
    return 0;
}

static PyObject *UringApiRing_close(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
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
    Py_END_CRITICAL_SECTION();
    Py_RETURN_NONE;
}

static PyObject *UringApiRing_enter(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    Py_INCREF(self);
    return (PyObject *)self;
}

static PyObject *UringApiRing_exit(UringApiRing *self, PyObject *args) {
    if (delivery_check_not_running(self) < 0) {
        return NULL;
    }
    if (self->initialized) {
        io_uring_queue_exit(&self->ring);
        self->initialized = false;
    }
    self->receive_state = URING_API_RECEIVE_IDLE;
    self->delivery_stop_requested = false;
    self->delivery_active_workers = 0;
    self->next_buf_group = 1;
    Py_RETURN_NONE;
}
