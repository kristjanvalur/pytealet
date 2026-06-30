/*
 * Completion object support for the _uring_api extension.
 */

#include "uring_api_completion.h"
#include "uring_api_core.h"

static int UringApiCompletion_clear(UringApiCompletion *self);

int completion_type_check(PyObject *completion) {
    if (!PyObject_TypeCheck(completion, &UringApiCompletion_Type)) {
        PyErr_SetString(PyExc_TypeError, "completion must be an _uring_api.Completion instance");
        return 0;
    }
    return 1;
}

static PyObject *sockaddr_to_object(struct sockaddr_storage *storage, socklen_t addrlen) {
    char host[INET6_ADDRSTRLEN];

    (void)addrlen;
    if (storage->ss_family == AF_INET) {
        struct sockaddr_in *addr = (struct sockaddr_in *)storage;
        if (!inet_ntop(AF_INET, &addr->sin_addr, host, sizeof(host))) {
            PyErr_SetFromErrno(PyExc_OSError);
            return NULL;
        }
        return Py_BuildValue("si", host, (int)ntohs(addr->sin_port));
    }
    if (storage->ss_family == AF_INET6) {
        struct sockaddr_in6 *addr = (struct sockaddr_in6 *)storage;
        if (!inet_ntop(AF_INET6, &addr->sin6_addr, host, sizeof(host))) {
            PyErr_SetFromErrno(PyExc_OSError);
            return NULL;
        }
        return Py_BuildValue("sIII", host, (unsigned int)ntohs(addr->sin6_port),
                             (unsigned int)ntohl(addr->sin6_flowinfo), (unsigned int)addr->sin6_scope_id);
    }
    Py_RETURN_NONE;
}

static bool is_power_of_two(unsigned long value) { return value != 0 && (value & (value - 1)) == 0; }

static void UringApiRecvBufferPool_free(UringApiRecvBufferPool *pool) {
    if (!pool) {
        return;
    }
    if (pool->ring_buffer && pool->ring && pool->ring->initialized) {
        (void)io_uring_free_buf_ring(&pool->ring->ring, pool->ring_buffer, pool->buffer_count, pool->group_id);
    }
    PyMem_Free(pool->storage);
    Py_XDECREF((PyObject *)pool->ring);
    PyMem_Free(pool);
}

UringApiRecvBufferPool *UringApiRecvBufferPool_new(UringApiRing *ring, unsigned int buffer_size,
                                                   unsigned int buffer_count) {
    UringApiRecvBufferPool *pool;
    size_t total_size;
    int ret = 0;
    unsigned int index;

    if (buffer_size == 0) {
        PyErr_SetString(PyExc_ValueError, "buffer_size must be positive");
        return NULL;
    }
    if (!is_power_of_two(buffer_count) || buffer_count > USHRT_MAX + 1U) {
        PyErr_SetString(PyExc_ValueError, "buffer_count must be a power of two no larger than 65536");
        return NULL;
    }
    if ((size_t)buffer_count > SIZE_MAX / (size_t)buffer_size) {
        PyErr_SetString(PyExc_ValueError, "buffer pool is too large");
        return NULL;
    }

    pool = PyMem_Calloc(1, sizeof(*pool));
    if (!pool) {
        PyErr_NoMemory();
        return NULL;
    }
    total_size = (size_t)buffer_count * (size_t)buffer_size;
    pool->storage = PyMem_Malloc(total_size);
    if (!pool->storage) {
        UringApiRecvBufferPool_free(pool);
        PyErr_NoMemory();
        return NULL;
    }

    pool->ring = ring;
    Py_INCREF((PyObject *)ring);
    pool->buffer_size = buffer_size;
    pool->buffer_count = buffer_count;
    pool->group_id = ring->next_buf_group++;
    pool->mask = io_uring_buf_ring_mask(buffer_count);
    pool->ring_buffer = io_uring_setup_buf_ring(&ring->ring, buffer_count, pool->group_id, 0, &ret);
    if (!pool->ring_buffer) {
        int errnum = normalize_ret_errno(ret);
        UringApiRecvBufferPool_free(pool);
        errno = errnum;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }

    for (index = 0; index < buffer_count; index++) {
        io_uring_buf_ring_add(pool->ring_buffer, pool->storage + ((size_t)index * buffer_size), buffer_size,
                              (unsigned short)index, pool->mask, (int)index);
    }
    io_uring_buf_ring_advance(pool->ring_buffer, (int)buffer_count);
    return pool;
}

static void UringApiRecvBufferPool_recycle(UringApiRecvBufferPool *pool, unsigned int buffer_id) {
    io_uring_buf_ring_add(pool->ring_buffer, pool->storage + ((size_t)buffer_id * pool->buffer_size), pool->buffer_size,
                          (unsigned short)buffer_id, pool->mask, 0);
    io_uring_buf_ring_advance(pool->ring_buffer, 1);
}

static void UringApiCompletion_dealloc(UringApiCompletion *self) {
    PyObject_GC_UnTrack(self);
    (void)UringApiCompletion_clear(self);
    PyObject_GC_Del(self);
}

static int UringApiCompletion_traverse(UringApiCompletion *self, visitproc visit, void *arg) {
    Py_VISIT(self->buffer);
    if (self->recv_pool) {
        Py_VISIT(self->recv_pool->ring);
    }
    Py_VISIT(self->user_data);
    Py_VISIT(self->result);
    return 0;
}

static int UringApiCompletion_clear(UringApiCompletion *self) {
    if (self->has_view) {
        PyBuffer_Release(&self->view);
        self->has_view = false;
    }
    Py_CLEAR(self->buffer);
    UringApiRecvBufferPool_free(self->recv_pool);
    self->recv_pool = NULL;
    Py_CLEAR(self->user_data);
    Py_CLEAR(self->result);
    return 0;
}

PyObject *UringApiCompletion_new_pending(UringApiPendingKind kind, PyObject *user_data, PyObject *buffer) {
    UringApiCompletion *completion = PyObject_GC_New(UringApiCompletion, &UringApiCompletion_Type);
    if (!completion) {
        return NULL;
    }
    completion->kind = kind;
    completion->user_data = Py_NewRef(user_data);
    completion->res = 0;
    completion->flags = 0;
    completion->result = NULL;
    completion->buffer = Py_XNewRef(buffer);
    completion->recv_pool = NULL;
    completion->sequence = 0;
    completion->has_view = false;
    completion->has_msghdr = false;
    PyObject_GC_Track(completion);
    return (PyObject *)completion;
}

PyObject *UringApiCompletion_new_pending_view(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view) {
    UringApiCompletion *completion = (UringApiCompletion *)UringApiCompletion_new_pending(kind, user_data, NULL);
    if (!completion) {
        return NULL;
    }
    completion->view = *view;
    completion->has_view = true;
    return (PyObject *)completion;
}

PyObject *UringApiCompletion_new_pending_recvmsg(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view) {
    UringApiCompletion *completion = (UringApiCompletion *)UringApiCompletion_new_pending_view(kind, user_data, view);
    if (!completion) {
        return NULL;
    }
    memset(&completion->iov, 0, sizeof(completion->iov));
    memset(&completion->msg, 0, sizeof(completion->msg));
    memset(&completion->addr, 0, sizeof(completion->addr));
    completion->addrlen = sizeof(completion->addr);
    completion->iov.iov_base = view->buf;
    completion->iov.iov_len = (size_t)view->len;
    completion->msg.msg_name = &completion->addr;
    completion->msg.msg_namelen = completion->addrlen;
    completion->msg.msg_iov = &completion->iov;
    completion->msg.msg_iovlen = 1;
    completion->has_msghdr = true;
    return (PyObject *)completion;
}

PyObject *UringApiCompletion_new_pending_sendmsg(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view) {
    UringApiCompletion *completion = (UringApiCompletion *)UringApiCompletion_new_pending_view(kind, user_data, view);
    if (!completion) {
        return NULL;
    }
    memset(&completion->iov, 0, sizeof(completion->iov));
    memset(&completion->msg, 0, sizeof(completion->msg));
    memset(&completion->addr, 0, sizeof(completion->addr));
    completion->addrlen = 0;
    completion->iov.iov_base = view->buf;
    completion->iov.iov_len = (size_t)view->len;
    completion->msg.msg_iov = &completion->iov;
    completion->msg.msg_iovlen = 1;
    completion->has_msghdr = true;
    return (PyObject *)completion;
}

bool is_zero_copy_send_kind(UringApiPendingKind kind) {
    return kind == URING_API_PENDING_SEND_ZC || kind == URING_API_PENDING_SENDMSG_ZC;
}

PyObject *UringApiCompletion_new_pending_accept(PyObject *user_data) {
    UringApiCompletion *completion =
        (UringApiCompletion *)UringApiCompletion_new_pending(URING_API_PENDING_ACCEPT, user_data, NULL);
    if (!completion) {
        return NULL;
    }
    memset(&completion->addr, 0, sizeof(completion->addr));
    completion->addrlen = sizeof(completion->addr);
    return (PyObject *)completion;
}

PyObject *UringApiCompletion_new_delivered_copy(UringApiCompletion *source) {
    UringApiCompletion *completion = PyObject_GC_New(UringApiCompletion, &UringApiCompletion_Type);
    if (!completion) {
        return NULL;
    }
    completion->kind = source->kind;
    completion->user_data = Py_NewRef(source->user_data);
    completion->res = source->res;
    completion->flags = source->flags;
    completion->result = source->result;
    source->result = NULL;
    completion->buffer = NULL;
    completion->recv_pool = NULL;
    completion->sequence = source->sequence;
    source->sequence++;
    memset(&completion->view, 0, sizeof(completion->view));
    memset(&completion->iov, 0, sizeof(completion->iov));
    memset(&completion->msg, 0, sizeof(completion->msg));
    memset(&completion->addr, 0, sizeof(completion->addr));
    completion->addrlen = 0;
    completion->has_view = false;
    completion->has_msghdr = false;
    PyObject_GC_Track(completion);
    return (PyObject *)completion;
}

void UringApiCompletion_clear_pending_state(UringApiCompletion *self) {
    if (self->has_view) {
        PyBuffer_Release(&self->view);
        self->has_view = false;
    }
    Py_CLEAR(self->buffer);
    UringApiRecvBufferPool_free(self->recv_pool);
    self->recv_pool = NULL;
}

static bool UringApiCompletion_should_clear_pending_state(UringApiCompletion *self, int res, unsigned int flags) {
    /* send_zc success has two CQEs: the operation result, then a NOTIF CQE that closes the buffer lifetime window. */
    if (is_zero_copy_send_kind(self->kind) && res >= 0 && !(flags & IORING_CQE_F_NOTIF)) {
        return false;
    }
    return !(flags & IORING_CQE_F_MORE);
}

static PyObject *UringApiCompletion_recv_multishot_payload(UringApiCompletion *self, int res, unsigned int flags) {
    unsigned int buffer_id;
    PyObject *payload;

    if (res < 0) {
        Py_RETURN_NONE;
    }
    if (res == 0 && !(flags & IORING_CQE_F_BUFFER)) {
        return PyBytes_FromStringAndSize("", 0);
    }
    if (!(flags & IORING_CQE_F_BUFFER)) {
        PyErr_SetString(PyExc_RuntimeError, "recv multishot completion did not select a buffer");
        return NULL;
    }
    buffer_id = flags >> IORING_CQE_BUFFER_SHIFT;
    if (!self->recv_pool || buffer_id >= self->recv_pool->buffer_count) {
        PyErr_SetString(PyExc_RuntimeError, "recv multishot completion selected an invalid buffer");
        return NULL;
    }
    if ((unsigned int)res > self->recv_pool->buffer_size) {
        PyErr_SetString(PyExc_RuntimeError, "recv multishot completion exceeds selected buffer size");
        return NULL;
    }

    payload = PyBytes_FromStringAndSize(
        (const char *)self->recv_pool->storage + ((size_t)buffer_id * self->recv_pool->buffer_size), (Py_ssize_t)res);
    if (!payload) {
        return NULL;
    }
    if (flags & IORING_CQE_F_MORE) {
        UringApiRecvBufferPool_recycle(self->recv_pool, buffer_id);
    }
    return payload;
}

int UringApiCompletion_complete(UringApiCompletion *self, int res, unsigned int flags) {
    PyObject *payload;

    self->res = res;
    self->flags = flags;
    if (self->kind == URING_API_PENDING_WAKE) {
        UringApiCompletion_clear_pending_state(self);
        return 1;
    }
    if (self->kind == URING_API_PENDING_RECV_MULTISHOT) {
        payload = UringApiCompletion_recv_multishot_payload(self, res, flags);
    } else if (res >= 0 && (self->kind == URING_API_PENDING_RECV || self->kind == URING_API_PENDING_SEND ||
                            is_zero_copy_send_kind(self->kind) || self->kind == URING_API_PENDING_SENDTO ||
                            self->kind == URING_API_PENDING_SENDMSG || self->kind == URING_API_PENDING_SOCKET)) {
        payload = PyLong_FromLong(res);
    } else if (res >= 0 && self->kind == URING_API_PENDING_RECVMSG) {
        self->addrlen = self->msg.msg_namelen;
        payload = sockaddr_to_object(&self->addr, self->addrlen);
    } else if (res >= 0 && self->kind == URING_API_PENDING_ACCEPT) {
        payload = sockaddr_to_object(&self->addr, self->addrlen);
        if (payload) {
            payload = Py_BuildValue("iN", res, payload);
        }
    } else if (res >= 0 && self->kind == URING_API_PENDING_CONNECT) {
        payload = Py_NewRef(Py_None);
    } else {
        payload = Py_NewRef(Py_None);
    }
    if (!payload) {
        return -1;
    }
    Py_XSETREF(self->result, payload);
    if (UringApiCompletion_should_clear_pending_state(self, res, flags)) {
        UringApiCompletion_clear_pending_state(self);
    }
    return 0;
}

static PyObject *UringApiCompletion_get_user_data(UringApiCompletion *self, void *closure) {
    return Py_NewRef(self->user_data);
}

static PyObject *UringApiCompletion_get_kind(UringApiCompletion *self, void *closure) {
    return PyLong_FromLong((long)self->kind);
}

static PyObject *UringApiCompletion_get_res(UringApiCompletion *self, void *closure) {
    return PyLong_FromLong(self->res);
}

static PyObject *UringApiCompletion_get_flags(UringApiCompletion *self, void *closure) {
    return PyLong_FromUnsignedLong(self->flags);
}

static PyObject *UringApiCompletion_get_result(UringApiCompletion *self, void *closure) {
    if (!self->result) {
        Py_RETURN_NONE;
    }
    return Py_NewRef(self->result);
}

static PyObject *UringApiCompletion_get_sequence(UringApiCompletion *self, void *closure) {
    return PyLong_FromUnsignedLongLong(self->sequence);
}

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
