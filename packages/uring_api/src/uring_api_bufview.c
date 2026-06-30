/*
 * Read-only leased-buffer views for provided-buffer receive.
 */

#include "uring_api_bufview.h"
#include "uring_api_core.h"

#ifdef URING_API_USE_PYTHREAD_RING_LOCK
#define BUFVIEW_BEGIN_CRITICAL_SECTION(view) {
#define BUFVIEW_END_CRITICAL_SECTION() }
#else
#define BUFVIEW_BEGIN_CRITICAL_SECTION(view) Py_BEGIN_CRITICAL_SECTION(view)
#define BUFVIEW_END_CRITICAL_SECTION() Py_END_CRITICAL_SECTION()
#endif

static int UringApiBufView_recycle_locked(UringApiBufView *self) {
    UringApiBufGroup *buf_group;

    if (self->recycled) {
        return 0;
    }
    if (!self->buf_group || !PyObject_TypeCheck(self->buf_group, &UringApiBufGroup_Type)) {
        PyErr_SetString(PyExc_RuntimeError, "buf view is missing its buffer group");
        return -1;
    }
    buf_group = (UringApiBufGroup *)self->buf_group;
    if (!buf_group->ring || !buf_group->ring->initialized) {
        PyErr_SetString(PyExc_RuntimeError, "buffer group ring is closed");
        return -1;
    }
    UringApiBufGroup_recycle(buf_group, self->buffer_id);
    self->recycled = true;
    return 0;
}

static int UringApiBufView_getbuffer(PyObject *obj, Py_buffer *view, int flags) {
    UringApiBufView *self = (UringApiBufView *)obj;
    UringApiBufGroup *buf_group;
    void *buffer_base = NULL;
    int failed = 0;

    if (flags & PyBUF_WRITABLE) {
        PyErr_SetString(PyExc_BufferError, "buf view is read-only");
        return -1;
    }

    BUFVIEW_BEGIN_CRITICAL_SECTION(self);
    if (self->recycled) {
        PyErr_SetString(PyExc_BufferError, "buf view has already been released");
        failed = 1;
    } else if (!self->buf_group || !PyObject_TypeCheck(self->buf_group, &UringApiBufGroup_Type)) {
        PyErr_SetString(PyExc_BufferError, "buf view is missing its buffer group");
        failed = 1;
    } else {
        buf_group = (UringApiBufGroup *)self->buf_group;
        buffer_base = buf_group->storage + ((size_t)self->buffer_id * buf_group->buffer_size);
        self->export_count++;
        if (flags & PyBUF_ND) {
            self->export_shape = (Py_ssize_t)self->length;
        }
        if (flags & PyBUF_STRIDES) {
            self->export_stride = 1;
        }
    }
    BUFVIEW_END_CRITICAL_SECTION();

    if (failed) {
        return -1;
    }
    view->buf = buffer_base;
    view->obj = Py_NewRef(obj);
    view->len = (Py_ssize_t)self->length;
    view->readonly = 1;
    view->itemsize = 1;
    view->format = (flags & PyBUF_FORMAT) ? "B" : NULL;
    view->suboffsets = NULL;
    view->internal = NULL;

    if (flags & PyBUF_ND) {
        view->ndim = 1;
        view->shape = &self->export_shape;
    } else {
        view->ndim = 0;
        view->shape = NULL;
    }

    if (flags & PyBUF_STRIDES) {
        view->strides = &self->export_stride;
    } else {
        view->strides = NULL;
    }

    return 0;
}

static void UringApiBufView_releasebuffer(PyObject *obj, Py_buffer *view) {
    UringApiBufView *self = (UringApiBufView *)obj;
    UringApiBufGroup *buf_group;

    (void)view;
    BUFVIEW_BEGIN_CRITICAL_SECTION(self);
    if (self->export_count != 0) {
        self->export_count--;
        if (self->export_count == 0 && !self->recycled && self->buf_group &&
            PyObject_TypeCheck(self->buf_group, &UringApiBufGroup_Type)) {
            buf_group = (UringApiBufGroup *)self->buf_group;
            if (buf_group->ring && buf_group->ring->initialized) {
                Py_BEGIN_CRITICAL_SECTION(buf_group->ring);
                (void)UringApiBufView_recycle_locked(self);
                Py_END_CRITICAL_SECTION();
            }
        }
    }
    BUFVIEW_END_CRITICAL_SECTION();
}

static PyObject *UringApiBufView_get_length(UringApiBufView *self, void *Py_UNUSED(closure)) {
    return PyLong_FromUnsignedLong(self->length);
}

static PyObject *UringApiBufView_get_buffer_id(UringApiBufView *self, void *Py_UNUSED(closure)) {
    return PyLong_FromUnsignedLong(self->buffer_id);
}

static PyObject *UringApiBufView_get_buf_group(UringApiBufView *self, void *Py_UNUSED(closure)) {
    if (!self->buf_group) {
        Py_RETURN_NONE;
    }
    return Py_NewRef(self->buf_group);
}

static PyObject *UringApiBufView_get_recycled(UringApiBufView *self, void *Py_UNUSED(closure)) {
    bool recycled;

    BUFVIEW_BEGIN_CRITICAL_SECTION(self);
    recycled = self->recycled;
    BUFVIEW_END_CRITICAL_SECTION();
    return PyBool_FromLong(recycled);
}

static PyObject *UringApiBufView_close(UringApiBufView *self, PyObject *Py_UNUSED(args)) {
    UringApiBufGroup *buf_group;
    int status = 0;

    BUFVIEW_BEGIN_CRITICAL_SECTION(self);
    if (self->recycled) {
        status = 1;
    } else if (self->export_count > 0) {
        PyErr_SetString(PyExc_BufferError, "cannot close buf view while buffer exports are active");
        status = -1;
    } else if (!self->buf_group || !PyObject_TypeCheck(self->buf_group, &UringApiBufGroup_Type)) {
        PyErr_SetString(PyExc_RuntimeError, "buf view is missing its buffer group");
        status = -2;
    } else {
        buf_group = (UringApiBufGroup *)self->buf_group;
        if (buf_group->ring && buf_group->ring->initialized) {
            Py_BEGIN_CRITICAL_SECTION(buf_group->ring);
            if (UringApiBufView_recycle_locked(self) < 0) {
                status = -3;
            }
            Py_END_CRITICAL_SECTION();
        } else {
            self->recycled = true;
            status = 1;
        }
    }
    BUFVIEW_END_CRITICAL_SECTION();

    if (status < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static int UringApiBufView_traverse(UringApiBufView *self, visitproc visit, void *arg) {
    Py_VISIT(self->buf_group);
    return 0;
}

static int UringApiBufView_clear(UringApiBufView *self) {
    Py_CLEAR(self->buf_group);
    return 0;
}

static void UringApiBufView_dealloc(UringApiBufView *self) {
    UringApiBufGroup *buf_group;

    PyObject_GC_UnTrack(self);
    BUFVIEW_BEGIN_CRITICAL_SECTION(self);
    if (!self->recycled && self->export_count == 0 && self->buf_group &&
        PyObject_TypeCheck(self->buf_group, &UringApiBufGroup_Type)) {
        buf_group = (UringApiBufGroup *)self->buf_group;
        if (buf_group->ring && buf_group->ring->initialized) {
            Py_BEGIN_CRITICAL_SECTION(buf_group->ring);
            (void)UringApiBufView_recycle_locked(self);
            Py_END_CRITICAL_SECTION();
        }
    }
    BUFVIEW_END_CRITICAL_SECTION();
    (void)UringApiBufView_clear(self);
    PyObject_GC_Del(self);
}

static PyObject *UringApiBufView_new(PyTypeObject *Py_UNUSED(type), PyObject *args, PyObject *kwargs) {
    (void)args;
    (void)kwargs;
    PyErr_SetString(PyExc_TypeError, "BufView cannot be instantiated directly");
    return NULL;
}

PyObject *UringApiBufView_create(PyObject *buf_group_obj, unsigned int buffer_id, unsigned int length) {
    UringApiBufGroup *buf_group;
    UringApiBufView *self;

    if (!PyObject_TypeCheck(buf_group_obj, &UringApiBufGroup_Type)) {
        PyErr_SetString(PyExc_TypeError, "buf_group must be a BufGroup instance");
        return NULL;
    }
    buf_group = (UringApiBufGroup *)buf_group_obj;
    if (buffer_id >= buf_group->buffer_count) {
        PyErr_SetString(PyExc_ValueError, "buffer_id is out of range for this buffer group");
        return NULL;
    }
    if (length > buf_group->buffer_size) {
        PyErr_SetString(PyExc_ValueError, "length exceeds buffer group slot size");
        return NULL;
    }

    self = PyObject_GC_New(UringApiBufView, &UringApiBufView_Type);
    if (!self) {
        return NULL;
    }
    self->buf_group = Py_NewRef(buf_group_obj);
    self->buffer_id = buffer_id;
    self->length = length;
    self->export_count = 0;
    self->recycled = false;
    PyObject_GC_Track(self);
    return (PyObject *)self;
}

PyObject *UringApiRing_create_buf_view(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"buf_group", "buffer_id", "length", NULL};
    PyObject *buf_group_obj;
    unsigned long buffer_id;
    unsigned long length;
    UringApiBufGroup *buf_group;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!kk", keywords, &UringApiBufGroup_Type, &buf_group_obj, &buffer_id,
                                     &length)) {
        return NULL;
    }
    if (buffer_id > UINT_MAX || length > UINT_MAX) {
        PyErr_SetString(PyExc_ValueError, "buffer_id and length must fit in uint32_t");
        return NULL;
    }
    buf_group = (UringApiBufGroup *)buf_group_obj;

    PyObject *buf_view = NULL;
    int failed = 0;

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else if (buf_group->ring != self) {
        PyErr_SetString(PyExc_ValueError, "buf_group was not created by this ring");
        failed = 1;
    } else {
        buf_view = UringApiBufView_create(buf_group_obj, (unsigned int)buffer_id, (unsigned int)length);
        if (!buf_view) {
            failed = 1;
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        return NULL;
    }
    return buf_view;
}

static PyMethodDef UringApiBufView_methods[] = {
    {"close", (PyCFunction)UringApiBufView_close, METH_NOARGS, "Release the leased buffer back to its group."},
    {NULL, NULL, 0, NULL},
};

static PyGetSetDef UringApiBufView_getset[] = {
    {"length", (getter)UringApiBufView_get_length, NULL, NULL, NULL},
    {"buffer_id", (getter)UringApiBufView_get_buffer_id, NULL, NULL, NULL},
    {"buf_group", (getter)UringApiBufView_get_buf_group, NULL, NULL, NULL},
    {"recycled", (getter)UringApiBufView_get_recycled, NULL, NULL, NULL},
    {NULL, NULL, NULL, NULL, NULL},
};

static PyBufferProcs UringApiBufView_bufferprocs = {
    .bf_getbuffer = UringApiBufView_getbuffer,
    .bf_releasebuffer = UringApiBufView_releasebuffer,
};

PyTypeObject UringApiBufView_Type = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_uring_api.BufView",
    .tp_basicsize = sizeof(UringApiBufView),
    .tp_dealloc = (destructor)UringApiBufView_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)UringApiBufView_traverse,
    .tp_clear = (inquiry)UringApiBufView_clear,
    .tp_doc = "Read-only leased view into a provided-buffer group slot",
    .tp_methods = UringApiBufView_methods,
    .tp_getset = UringApiBufView_getset,
    .tp_new = UringApiBufView_new,
    .tp_as_buffer = &UringApiBufView_bufferprocs,
};