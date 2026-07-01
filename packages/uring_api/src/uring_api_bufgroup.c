/*
 * Provided-buffer group support for the _uring_api extension.
 */

#include "uring_api_bufgroup.h"
#include "uring_api_core.h"

static bool buf_group_is_power_of_two(unsigned long value) { return value != 0 && (value & (value - 1)) == 0; }

static PyObject *UringApiBufGroup_get_buffer_size(UringApiBufGroup *self, void *Py_UNUSED(closure)) {
    return PyLong_FromUnsignedLong(self->buffer_size);
}

static PyObject *UringApiBufGroup_get_buffer_count(UringApiBufGroup *self, void *Py_UNUSED(closure)) {
    return PyLong_FromUnsignedLong(self->buffer_count);
}

static PyObject *UringApiBufGroup_get_leased_count(UringApiBufGroup *self, void *Py_UNUSED(closure)) {
    return PyLong_FromUnsignedLong(self->leased_count);
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

static int UringApiBufGroup_traverse(UringApiBufGroup *self, visitproc visit, void *arg) {
    Py_VISIT(self->ring);
    return 0;
}

static void UringApiBufGroup_free_buf_ring(UringApiBufGroup *self) {
    if (!self->ring_buffer || !self->ring || !self->ring->initialized) {
        return;
    }
    Py_BEGIN_CRITICAL_SECTION(self->ring);
    (void)io_uring_free_buf_ring(&self->ring->ring, self->ring_buffer, self->buffer_count, self->group_id);
    Py_END_CRITICAL_SECTION();
    self->ring_buffer = NULL;
}

static int UringApiBufGroup_clear(UringApiBufGroup *self) {
    UringApiBufGroup_free_buf_ring(self);
    Py_CLEAR(self->ring);
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
    self->group_id = 0;
    self->mask = 0;

    total_size = (size_t)buffer_count * (size_t)buffer_size;
    self->storage = PyMem_Malloc(total_size);
    if (!self->storage) {
        Py_DECREF(self);
        PyErr_NoMemory();
        return NULL;
    }

    if (ring->next_buf_group == 0) {
        Py_DECREF(self);
        PyErr_SetString(PyExc_RuntimeError, "buffer group ID space exhausted (max 65535 groups per ring)");
        return NULL;
    }
    self->group_id = ring->next_buf_group++;
    self->mask = io_uring_buf_ring_mask(buffer_count);
    self->ring_buffer = io_uring_setup_buf_ring(&ring->ring, buffer_count, self->group_id, 0, &ret);
    if (!self->ring_buffer) {
        int errnum = normalize_ret_errno(ret);
        Py_DECREF(self);
        errno = errnum;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
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
    io_uring_buf_ring_add(self->ring_buffer, self->storage + ((size_t)buffer_id * self->buffer_size), self->buffer_size,
                          (unsigned short)buffer_id, self->mask, 0);
    io_uring_buf_ring_advance(self->ring_buffer, 1);
}

void UringApiBufGroup_note_leased(UringApiBufGroup *self) { self->leased_count++; }

void UringApiBufGroup_note_unleased(UringApiBufGroup *self) { self->leased_count--; }

PyObject *UringApiRing_create_buf_group(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"buffer_size", "buffer_count", NULL};
    unsigned long buffer_size;
    unsigned long buffer_count;
    PyObject *buf_group = NULL;
    int failed = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "kk", keywords, &buffer_size, &buffer_count)) {
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
    {"group_id", (getter)UringApiBufGroup_get_group_id, NULL, NULL, NULL},
    {"ring", (getter)UringApiBufGroup_get_ring, NULL, NULL, NULL},
    {NULL, NULL, NULL, NULL, NULL},
};

PyTypeObject UringApiBufGroup_Type = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_uring_api.BufGroup",
    .tp_basicsize = sizeof(UringApiBufGroup),
    .tp_dealloc = (destructor)UringApiBufGroup_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)UringApiBufGroup_traverse,
    .tp_clear = (inquiry)UringApiBufGroup_clear,
    .tp_doc = "io_uring provided-buffer group",
    .tp_getset = UringApiBufGroup_getset,
    .tp_new = UringApiBufGroup_new,
};