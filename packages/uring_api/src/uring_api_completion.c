/*
 * Completion object support for the _uring_api extension.
 */

#include "uring_api_completion.h"
#include "uring_api_bufgroup.h"
#include "uring_api_bufview.h"
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

static void UringApiCompletion_dealloc(UringApiCompletion *self) {
    PyObject_GC_UnTrack(self);
    (void)UringApiCompletion_clear(self);
    PyObject_GC_Del(self);
}

static int UringApiCompletion_traverse(UringApiCompletion *self, visitproc visit, void *arg) {
    Py_VISIT(self->buffer);
    Py_VISIT(self->buf_group);
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
    Py_CLEAR(self->buf_group);
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
    completion->user_data = Py_NewRef(user_data != NULL ? user_data : Py_None);
    completion->res = 0;
    completion->flags = 0;
    completion->result = NULL;
    completion->buffer = Py_XNewRef(buffer);
    completion->buf_group = NULL;
    completion->sequence = 0;
    completion->has_view = false;
    completion->has_msghdr = false;
    PyObject_GC_Track(completion);
    return (PyObject *)completion;
}

PyObject *UringApiCompletion_new_pending_view(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view) {
    UringApiCompletion *completion = (UringApiCompletion *)UringApiCompletion_new_pending(kind, user_data, NULL);
    if (!completion) {
        PyBuffer_Release(view);
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
    completion->buf_group = NULL;
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
    Py_CLEAR(self->buf_group);
}

static bool UringApiCompletion_should_clear_pending_state(UringApiCompletion *self, int res, unsigned int flags) {
    if (is_zero_copy_send_kind(self->kind) && res >= 0 && !(flags & IORING_CQE_F_NOTIF)) {
        return false;
    }
    return !(flags & IORING_CQE_F_MORE);
}

static void UringApiCompletion_recycle_selected_buffer(UringApiCompletion *self, unsigned int flags) {
    UringApiBufGroup *buf_group;
    unsigned int buffer_id;

    if (!(flags & IORING_CQE_F_BUFFER)) {
        return;
    }
    if (!self->buf_group || !PyObject_TypeCheck(self->buf_group, &UringApiBufGroup_Type)) {
        return;
    }
    buf_group = (UringApiBufGroup *)self->buf_group;
    buffer_id = flags >> IORING_CQE_BUFFER_SHIFT;
    if (buffer_id >= buf_group->buffer_count) {
        return;
    }
    if (buf_group->ring && buf_group->ring->initialized) {
        Py_BEGIN_CRITICAL_SECTION(buf_group->ring);
        UringApiBufGroup_recycle(buf_group, buffer_id);
        Py_END_CRITICAL_SECTION();
    }
}

static PyObject *UringApiCompletion_recv_multishot_buf_payload(UringApiCompletion *self, int res, unsigned int flags) {
    unsigned int buffer_id;

    if (res < 0) {
        UringApiCompletion_recycle_selected_buffer(self, flags);
        Py_RETURN_NONE;
    }
    if (!self->buf_group || !PyObject_TypeCheck(self->buf_group, &UringApiBufGroup_Type)) {
        PyErr_SetString(PyExc_RuntimeError, "provided-buffer recv completion has no buffer group");
        return NULL;
    }
    if (res == 0 && !(flags & IORING_CQE_F_BUFFER)) {
        return UringApiBufView_create_empty(self->buf_group);
    }
    if (!(flags & IORING_CQE_F_BUFFER)) {
        PyErr_SetString(PyExc_RuntimeError, "provided-buffer recv completion did not select a buffer");
        return NULL;
    }
    buffer_id = flags >> IORING_CQE_BUFFER_SHIFT;
    if (buffer_id >= ((UringApiBufGroup *)self->buf_group)->buffer_count) {
        PyErr_SetString(PyExc_RuntimeError, "provided-buffer recv completion selected an invalid buffer");
        return NULL;
    }
    if ((unsigned int)res > ((UringApiBufGroup *)self->buf_group)->buffer_size) {
        PyErr_SetString(PyExc_RuntimeError, "provided-buffer recv completion exceeds selected buffer size");
        return NULL;
    }
    return UringApiBufView_create(self->buf_group, buffer_id, (unsigned int)res);
}

int UringApiCompletion_complete(UringApiCompletion *self, int res, unsigned int flags) {
    PyObject *payload;

    self->res = res;
    self->flags = flags;
    if (self->kind == URING_API_PENDING_WAKE) {
        UringApiCompletion_clear_pending_state(self);
        return 1;
    }
    if (self->kind == URING_API_PENDING_RECV_MULTISHOT || self->kind == URING_API_PENDING_RECV_BUF) {
        payload = UringApiCompletion_recv_multishot_buf_payload(self, res, flags);
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