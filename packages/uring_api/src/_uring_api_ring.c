/*
 * Ring lifecycle methods for the _uring_api extension.
 *
 * This file owns Ring initialisation, teardown, context-manager support, and
 * close handling. It is included by _uring_api.c as part of the single
 * extension translation unit.
 */

static int UringApiRing_init(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    struct io_uring_params params;
    unsigned int entries;
    unsigned int flags;
    int ret;

    if (parse_entries_flags(args, kwargs, 8, &entries, &flags) < 0) {
        return -1;
    }

    if (delivery_check_not_running(self) < 0) {
        return -1;
    }

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
        return -1;
    }

    self->initialized = true;
    return 0;
}

static void UringApiRing_dealloc(UringApiRing *self) {
    (void)UringApiRing_stop_delivery(self);
    if (self->initialized) {
        io_uring_queue_exit(&self->ring);
        self->initialized = false;
    }
    Py_CLEAR(self->delivery_callback);
    self->c_delivery_callback = NULL;
    self->c_delivery_callback_user_data = NULL;
    if (self->delivery_wait_lock) {
        PyThread_free_lock(self->delivery_wait_lock);
        self->delivery_wait_lock = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *UringApiRing_close(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
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
    Py_RETURN_NONE;
}
