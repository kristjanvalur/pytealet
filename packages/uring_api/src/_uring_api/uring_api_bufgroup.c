/*
 * Provided-buffer group support for the _uring_api extension.
 */

#include "uring_api_bufgroup.h"
#include "uring_api_core.h"

static bool buf_group_is_power_of_two(unsigned long value) { return value != 0 && (value & (value - 1)) == 0; }

void UringApiRing_clear_free_buf_group_ids(UringApiRing *ring) {
    PyMem_Free(ring->free_buf_group_ids);
    ring->free_buf_group_ids = NULL;
    ring->free_buf_group_id_count = 0;
    ring->free_buf_group_id_capacity = 0;
}

unsigned short UringApiRing_alloc_buf_group_id(UringApiRing *ring) {
    if (ring->free_buf_group_id_count > 0) {
        return ring->free_buf_group_ids[--ring->free_buf_group_id_count];
    }
    if (ring->next_buf_group == 0) {
        return 0;
    }
    return ring->next_buf_group++;
}

static void UringApiRing_shrink_free_buf_group_tail(UringApiRing *ring) {
    while (ring->free_buf_group_id_count > 0) {
        unsigned short tail_id = ring->free_buf_group_ids[ring->free_buf_group_id_count - 1];

        if (tail_id != (unsigned short)(ring->next_buf_group - 1)) {
            break;
        }
        ring->free_buf_group_id_count--;
        ring->next_buf_group--;
    }
}

/*
 * Return a buffer-group ID to the per-ring pool after io_uring_free_buf_ring().
 * Returns 0 on success. Returns -1 when freelist growth hits PyMem_Realloc OOM;
 * the ID is then lost for the rest of this ring session (effective pool shrinks).
 * BufGroup create paths must treat -1 as PyErr_NoMemory(). Teardown may ignore
 * -1: the kernel buf ring is already gone and losing reuse of one numeric ID is
 * acceptable best-effort behaviour under memory pressure.
 */
int UringApiRing_release_buf_group_id(UringApiRing *ring, unsigned short group_id) {
    unsigned int new_capacity;
    unsigned short *new_ids;

    if (group_id == 0) {
        return 0;
    }
    if (group_id == (unsigned short)(ring->next_buf_group - 1)) {
        ring->next_buf_group--;
        UringApiRing_shrink_free_buf_group_tail(ring);
        return 0;
    }
    if (ring->free_buf_group_id_count >= ring->free_buf_group_id_capacity) {
        new_capacity = ring->free_buf_group_id_capacity == 0 ? 16 : ring->free_buf_group_id_capacity * 2;
        new_ids = (unsigned short *)PyMem_Realloc(ring->free_buf_group_ids, new_capacity * sizeof(unsigned short));
        if (!new_ids) {
            return -1;
        }
        ring->free_buf_group_ids = new_ids;
        ring->free_buf_group_id_capacity = new_capacity;
    }
    ring->free_buf_group_ids[ring->free_buf_group_id_count++] = group_id;
    return 0;
}

static PyObject *UringApiBufGroup_get_buffer_size(UringApiBufGroup *self, void *Py_UNUSED(closure)) {
    return PyLong_FromUnsignedLong(self->buffer_size);
}

static PyObject *UringApiBufGroup_get_buffer_count(UringApiBufGroup *self, void *Py_UNUSED(closure)) {
    return PyLong_FromUnsignedLong(self->buffer_count);
}

static PyObject *UringApiBufGroup_get_leased_count(UringApiBufGroup *self, void *Py_UNUSED(closure)) {
    return PyLong_FromUnsignedLong(self->leased_count);
}

static PyObject *UringApiBufGroup_get_inflight_count(UringApiBufGroup *self, void *Py_UNUSED(closure)) {
    return PyLong_FromUnsignedLong(atomic_load_explicit(&self->inflight, memory_order_acquire));
}

static PyObject *UringApiBufGroup_get_group_id(UringApiBufGroup *self, void *Py_UNUSED(closure)) {
    return PyLong_FromUnsignedLong(self->group_id);
}

static PyObject *UringApiBufGroup_get_ring(UringApiBufGroup *self, void *Py_UNUSED(closure)) {
    if (!self->ring) {
        Py_RETURN_NONE;
    }
    return Py_NewRef(self->ring);
}

static PyObject *UringApiBufGroup_get_release_callback(UringApiBufGroup *self, void *Py_UNUSED(closure)) {
    if (!self->release_callback) {
        Py_RETURN_NONE;
    }
    return Py_NewRef(self->release_callback);
}

static int UringApiBufGroup_set_release_callback(UringApiBufGroup *self, PyObject *value, void *Py_UNUSED(closure)) {
    if (value == NULL) {
        Py_CLEAR(self->release_callback);
        return 0;
    }
    if (value != Py_None && !PyCallable_Check(value)) {
        PyErr_SetString(PyExc_TypeError, "release_callback must be callable or None");
        return -1;
    }
    if (value == Py_None) {
        Py_CLEAR(self->release_callback);
        return 0;
    }
    Py_XINCREF(value);
    Py_XSETREF(self->release_callback, value);
    return 0;
}

static int UringApiBufGroup_traverse(UringApiBufGroup *self, visitproc visit, void *arg) {
    Py_VISIT(self->ring);
    Py_VISIT(self->release_callback);
    return 0;
}

static void UringApiBufGroup_free_buf_ring(UringApiBufGroup *self) {
    unsigned short group_id;
    UringApiRing *ring;

    ring = self->ring;
    group_id = self->group_id;
    if (!ring || !ring->initialized || group_id == 0) {
        self->ring_buffer = NULL;
        return;
    }

    Py_BEGIN_CRITICAL_SECTION(ring);
    if (self->ring_buffer) {
        (void)io_uring_free_buf_ring(&ring->ring, self->ring_buffer, self->buffer_count, group_id);
        self->ring_buffer = NULL;
    }
    /* Teardown OOM while queueing the ID is non-fatal: the kernel ring is gone. */
    (void)UringApiRing_release_buf_group_id(ring, group_id);
    self->group_id = 0;
    Py_END_CRITICAL_SECTION();
}

/*
 * close(): tell the caller whether the kernel ring was released.
 * inflight > 0 keeps the ring registered (bgid reuse would alias a later
 * group). The release_callback still runs so an owner can defer dispose.
 * Idle + callback hands the group back without freeing. Idle + no callback
 * frees the ring. Does not clear release_callback.
 */
static PyObject *UringApiBufGroup_close(UringApiBufGroup *self, PyObject *Py_UNUSED(args)) {
    PyObject *callback;
    PyObject *result;
    int busy;

    busy = atomic_load_explicit(&self->inflight, memory_order_acquire) > 0;
    callback = self->release_callback;
    if (callback != NULL) {
        result = PyObject_CallOneArg(callback, (PyObject *)self);
        if (result == NULL) {
            return NULL;
        }
        Py_DECREF(result);
        if (busy) {
            Py_RETURN_FALSE;
        }
        Py_RETURN_TRUE;
    }
    if (busy) {
        Py_RETURN_FALSE;
    }

    UringApiBufGroup_free_buf_ring(self);
    Py_RETURN_TRUE;
}

static int UringApiBufGroup_clear(UringApiBufGroup *self) {
    UringApiBufGroup_free_buf_ring(self);
    Py_CLEAR(self->ring);
    Py_CLEAR(self->release_callback);
    return 0;
}

static void UringApiBufGroup_dealloc(UringApiBufGroup *self) {
    PyObject_GC_UnTrack(self);
    UringApiBufGroup_free_buf_ring(self);
    PyMem_Free(self->storage);
    self->storage = NULL;
    (void)UringApiBufGroup_clear(self);
    PyObject_GC_Del(self);
}

PyObject *UringApiBufGroup_create(UringApiRing *ring, unsigned int buffer_size, unsigned int buffer_count) {
    UringApiBufGroup *self;
    size_t total_size;
    int ret = 0;
    unsigned int index;

    if (buffer_size == 0) {
        PyErr_SetString(PyExc_ValueError, "buffer_size must be positive");
        return NULL;
    }
    if (!buf_group_is_power_of_two(buffer_count) || buffer_count > USHRT_MAX + 1U) {
        PyErr_SetString(PyExc_ValueError, "buffer_count must be a power of two no larger than 65536");
        return NULL;
    }
    if ((size_t)buffer_count > SIZE_MAX / (size_t)buffer_size) {
        PyErr_SetString(PyExc_ValueError, "buffer pool is too large");
        return NULL;
    }

    self = PyObject_GC_New(UringApiBufGroup, &UringApiBufGroup_Type);
    if (!self) {
        return NULL;
    }
    self->ring = ring;
    Py_INCREF(ring);
    self->ring_buffer = NULL;
    self->storage = NULL;
    self->buffer_size = buffer_size;
    self->buffer_count = buffer_count;
    self->leased_count = 0;
    atomic_init(&self->inflight, 0);
    self->group_id = 0;
    self->mask = 0;
    self->release_callback = NULL;

    total_size = (size_t)buffer_count * (size_t)buffer_size;
    self->storage = PyMem_Malloc(total_size);
    if (!self->storage) {
        Py_DECREF(self);
        PyErr_NoMemory();
        return NULL;
    }

    self->group_id = UringApiRing_alloc_buf_group_id(ring);
    if (self->group_id == 0) {
        Py_DECREF(self);
        PyErr_SetString(PyExc_RuntimeError, "buffer group ID space exhausted (max 65535 live groups per ring)");
        return NULL;
    }
    self->mask = io_uring_buf_ring_mask(buffer_count);
    self->ring_buffer = io_uring_setup_buf_ring(&ring->ring, buffer_count, self->group_id, 0, &ret);
    if (!self->ring_buffer) {
        int errnum = normalize_ret_errno(ret);

        /* Caller holds the ring critical section; release the ID before DECREF. */
        if (UringApiRing_release_buf_group_id(ring, self->group_id) < 0) {
            self->group_id = 0;
            Py_DECREF(self);
            return PyErr_NoMemory();
        }
        self->group_id = 0;
        Py_DECREF(self);
        errno = errnum;
        return PyErr_SetFromErrno(PyExc_OSError);
    }

    for (index = 0; index < buffer_count; index++) {
        io_uring_buf_ring_add(self->ring_buffer, self->storage + ((size_t)index * buffer_size), buffer_size,
                              (unsigned short)index, self->mask, (int)index);
    }
    io_uring_buf_ring_advance(self->ring_buffer, (int)buffer_count);
    PyObject_GC_Track(self);
    return (PyObject *)self;
}

void UringApiBufGroup_recycle(UringApiBufGroup *self, unsigned int buffer_id) {
    /* free_buf_ring nulls ring_buffer; never re-add onto a disposed group. */
    if (!self->ring_buffer) {
        return;
    }
    io_uring_buf_ring_add(self->ring_buffer, self->storage + ((size_t)buffer_id * self->buffer_size), self->buffer_size,
                          (unsigned short)buffer_id, self->mask, 0);
    io_uring_buf_ring_advance(self->ring_buffer, 1);
}

void UringApiBufGroup_note_leased(UringApiBufGroup *self) { self->leased_count++; }

void UringApiBufGroup_note_unleased(UringApiBufGroup *self) {
    if (self->leased_count > 0) {
        self->leased_count--;
    }
}

void UringApiBufGroup_note_request(UringApiBufGroup *self) {
    atomic_fetch_add_explicit(&self->inflight, 1, memory_order_acq_rel);
}

void UringApiBufGroup_note_request_done(UringApiBufGroup *self) {
    unsigned int previous = atomic_fetch_sub_explicit(&self->inflight, 1, memory_order_acq_rel);

    /* a terminal CQE without a matching SQE fill is our bug. */
    assert(previous > 0);
    (void)previous;
}

PyObject *UringApiRing_create_buf_group(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"buffer_size", "buffer_count", NULL};
    unsigned long buffer_size;
    unsigned long buffer_count;
    PyObject *buf_group = NULL;
    int failed = 0;

    if (!URING_API_PARSE_KEYWORDS("kk", keywords, &buffer_size, &buffer_count)) {
        return NULL;
    }
    if (buffer_size > UINT_MAX || buffer_count > UINT_MAX) {
        PyErr_SetString(PyExc_ValueError, "buffer_size and buffer_count must fit in uint32_t");
        return NULL;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        buf_group = UringApiBufGroup_create(self, (unsigned int)buffer_size, (unsigned int)buffer_count);
        if (!buf_group) {
            failed = 1;
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        return NULL;
    }
    return buf_group;
}

static PyObject *UringApiBufGroup_new(PyTypeObject *Py_UNUSED(type), PyObject *args, PyObject *kwargs) {
    (void)args;
    (void)kwargs;
    PyErr_SetString(PyExc_TypeError, "BufGroup cannot be instantiated directly");
    return NULL;
}

static PyGetSetDef UringApiBufGroup_getset[] = {
    {"buffer_size", (getter)UringApiBufGroup_get_buffer_size, NULL, NULL, NULL},
    {"buffer_count", (getter)UringApiBufGroup_get_buffer_count, NULL, NULL, NULL},
    {"leased_count", (getter)UringApiBufGroup_get_leased_count, NULL, NULL, NULL},
    {"inflight_count", (getter)UringApiBufGroup_get_inflight_count, NULL,
     "Armed recv_buf and recv_multishot requests. One per filled SQE, dropped "
     "on the terminal !MORE CQE. Not the number of leased BufViews.",
     NULL},
    {"group_id", (getter)UringApiBufGroup_get_group_id, NULL, NULL, NULL},
    {"ring", (getter)UringApiBufGroup_get_ring, NULL, NULL, NULL},
    {"release_callback", (getter)UringApiBufGroup_get_release_callback, (setter)UringApiBufGroup_set_release_callback,
     "optional callable(pool); when set, close() returns the group to its owner "
     "(callback should clear this attribute when done)",
     NULL},
    {NULL, NULL, NULL, NULL, NULL},
};

static PyMethodDef UringApiBufGroup_methods[] = {
    {"close", (PyCFunction)UringApiBufGroup_close, METH_NOARGS,
     "Return True if the group is idle (callback invoked, or the ring freed).\n"
     "Return False when inflight_count is non-zero: the ring stays registered.\n"
     "release_callback still runs in that case. Does not clear the callback."},
    {NULL, NULL, 0, NULL},
};

PyTypeObject UringApiBufGroup_Type = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_uring_api.BufGroup",
    .tp_basicsize = sizeof(UringApiBufGroup),
    .tp_dealloc = (destructor)UringApiBufGroup_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)UringApiBufGroup_traverse,
    .tp_clear = (inquiry)UringApiBufGroup_clear,
    .tp_doc = "io_uring provided-buffer group",
    .tp_methods = UringApiBufGroup_methods,
    .tp_getset = UringApiBufGroup_getset,
    .tp_new = UringApiBufGroup_new,
};
