/*
 * Completion object support for the _uring_api extension.
 */

#include "uring_api_completion.h"
#include "uring_api_bufgroup.h"
#include "uring_api_bufview.h"
#include "uring_api_core.h"

static int UringApiCompletion_clear(UringApiCompletion *self);

static PyObject *UringApiCompletion_get_buf_group(UringApiCompletion *self) {
    UringApiCompletionBufGroupState *buf_group_state;

    if (self->state_kind != URING_API_COMPLETION_STATE_BUF_GROUP || !self->state) {
        return NULL;
    }
    buf_group_state = (UringApiCompletionBufGroupState *)self->state;
    return buf_group_state->buf_group;
}

static void UringApiCompletion_release_view_state(UringApiCompletionViewState *view_state) {
    if (!view_state) {
        return;
    }
    if (view_state->has_view) {
        PyBuffer_Release(&view_state->view);
        view_state->has_view = false;
    }
}

static void UringApiCompletion_release_msg_view(UringApiCompletionMsgState *msg_state) {
    if (!msg_state) {
        return;
    }
    if (msg_state->has_view) {
        PyBuffer_Release(&msg_state->view);
        msg_state->has_view = false;
    }
}

static void UringApiCompletion_free_state(UringApiCompletion *self) {
    UringApiCompletionBufGroupState *buf_group_state;
    UringApiCompletionViewState *view_state;
    UringApiCompletionMsgState *msg_state;

    if (!self->state) {
        self->state_kind = URING_API_COMPLETION_STATE_NONE;
        return;
    }

    switch (self->state_kind) {
    case URING_API_COMPLETION_STATE_VIEW:
        view_state = (UringApiCompletionViewState *)self->state;
        UringApiCompletion_release_view_state(view_state);
        PyMem_Free(view_state);
        break;
    case URING_API_COMPLETION_STATE_BUF_GROUP:
        buf_group_state = (UringApiCompletionBufGroupState *)self->state;
        Py_CLEAR(buf_group_state->buf_group);
        PyMem_Free(buf_group_state);
        break;
    case URING_API_COMPLETION_STATE_SOCKADDR:
        PyMem_Free(self->state);
        break;
    case URING_API_COMPLETION_STATE_MSG:
        msg_state = (UringApiCompletionMsgState *)self->state;
        UringApiCompletion_release_msg_view(msg_state);
        PyMem_Free(msg_state);
        break;
    case URING_API_COMPLETION_STATE_NONE:
        break;
    }

    self->state = NULL;
    self->state_kind = URING_API_COMPLETION_STATE_NONE;
}

UringApiCompletionSockaddrState *UringApiCompletion_get_sockaddr_state(UringApiCompletion *self) {
    if (self->state_kind != URING_API_COMPLETION_STATE_SOCKADDR || !self->state) {
        return NULL;
    }
    return (UringApiCompletionSockaddrState *)self->state;
}

UringApiCompletionMsgState *UringApiCompletion_get_msg_state(UringApiCompletion *self) {
    if (self->state_kind != URING_API_COMPLETION_STATE_MSG || !self->state) {
        return NULL;
    }
    return (UringApiCompletionMsgState *)self->state;
}

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
    PyObject *buf_group;

    buf_group = UringApiCompletion_get_buf_group(self);
    Py_VISIT(buf_group);
    Py_VISIT(self->user_data);
    Py_VISIT(self->result);
    return 0;
}

static int UringApiCompletion_clear(UringApiCompletion *self) {
    UringApiCompletion_free_state(self);
    Py_CLEAR(self->user_data);
    Py_CLEAR(self->result);
    return 0;
}

static UringApiCompletion *UringApiCompletion_alloc(UringApiPendingKind kind, PyObject *user_data) {
    UringApiCompletion *completion;

    completion = PyObject_GC_New(UringApiCompletion, &UringApiCompletion_Type);
    if (!completion) {
        return NULL;
    }
    completion->kind = kind;
    completion->user_data = Py_NewRef(user_data != NULL ? user_data : Py_None);
    completion->res = 0;
    completion->flags = 0;
    completion->result = NULL;
    completion->sequence = 0;
    completion->multishot = false;
    completion->state_kind = URING_API_COMPLETION_STATE_NONE;
    completion->state = NULL;
    PyObject_GC_Track(completion);
    return completion;
}

PyObject *UringApiCompletion_new_pending(UringApiPendingKind kind, PyObject *user_data) {
    return (PyObject *)UringApiCompletion_alloc(kind, user_data);
}

PyObject *UringApiCompletion_new_pending_buf_group(UringApiPendingKind kind, PyObject *user_data, PyObject *buf_group) {
    UringApiCompletion *completion;
    UringApiCompletionBufGroupState *buf_group_state;

    completion = UringApiCompletion_alloc(kind, user_data);
    if (!completion) {
        return NULL;
    }
    buf_group_state = PyMem_Malloc(sizeof(UringApiCompletionBufGroupState));
    if (!buf_group_state) {
        Py_DECREF(completion);
        return PyErr_NoMemory();
    }
    buf_group_state->buf_group = Py_NewRef(buf_group);
    completion->state_kind = URING_API_COMPLETION_STATE_BUF_GROUP;
    completion->state = buf_group_state;
    return (PyObject *)completion;
}

PyObject *UringApiCompletion_new_pending_view(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view) {
    UringApiCompletion *completion;
    UringApiCompletionViewState *view_state;

    completion = UringApiCompletion_alloc(kind, user_data);
    if (!completion) {
        PyBuffer_Release(view);
        return NULL;
    }
    view_state = PyMem_Malloc(sizeof(UringApiCompletionViewState));
    if (!view_state) {
        Py_DECREF(completion);
        PyBuffer_Release(view);
        return PyErr_NoMemory();
    }
    view_state->view = *view;
    view_state->has_view = true;
    completion->state_kind = URING_API_COMPLETION_STATE_VIEW;
    completion->state = view_state;
    return (PyObject *)completion;
}

PyObject *UringApiCompletion_new_pending_recvmsg(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view) {
    UringApiCompletion *completion;
    UringApiCompletionMsgState *msg_state;

    completion = UringApiCompletion_alloc(kind, user_data);
    if (!completion) {
        PyBuffer_Release(view);
        return NULL;
    }
    msg_state = PyMem_Malloc(sizeof(UringApiCompletionMsgState));
    if (!msg_state) {
        Py_DECREF(completion);
        PyBuffer_Release(view);
        return PyErr_NoMemory();
    }
    memset(msg_state, 0, sizeof(*msg_state));
    msg_state->view = *view;
    msg_state->has_view = true;
    msg_state->addrlen = sizeof(msg_state->addr);
    msg_state->iov.iov_base = view->buf;
    msg_state->iov.iov_len = (size_t)view->len;
    msg_state->msg.msg_name = &msg_state->addr;
    msg_state->msg.msg_namelen = msg_state->addrlen;
    msg_state->msg.msg_iov = &msg_state->iov;
    msg_state->msg.msg_iovlen = 1;
    completion->state_kind = URING_API_COMPLETION_STATE_MSG;
    completion->state = msg_state;
    return (PyObject *)completion;
}

PyObject *UringApiCompletion_new_pending_sendmsg(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view) {
    UringApiCompletion *completion;
    UringApiCompletionMsgState *msg_state;

    completion = UringApiCompletion_alloc(kind, user_data);
    if (!completion) {
        PyBuffer_Release(view);
        return NULL;
    }
    msg_state = PyMem_Malloc(sizeof(UringApiCompletionMsgState));
    if (!msg_state) {
        Py_DECREF(completion);
        PyBuffer_Release(view);
        return PyErr_NoMemory();
    }
    memset(msg_state, 0, sizeof(*msg_state));
    msg_state->view = *view;
    msg_state->has_view = true;
    msg_state->iov.iov_base = view->buf;
    msg_state->iov.iov_len = (size_t)view->len;
    msg_state->msg.msg_iov = &msg_state->iov;
    msg_state->msg.msg_iovlen = 1;
    completion->state_kind = URING_API_COMPLETION_STATE_MSG;
    completion->state = msg_state;
    return (PyObject *)completion;
}

bool is_zero_copy_send_kind(UringApiPendingKind kind) {
    return kind == URING_API_PENDING_SEND_ZC || kind == URING_API_PENDING_SENDMSG_ZC;
}

PyObject *UringApiCompletion_new_pending_accept(PyObject *user_data) {
    UringApiCompletion *completion;
    UringApiCompletionSockaddrState *sockaddr_state;

    completion = UringApiCompletion_alloc(URING_API_PENDING_ACCEPT, user_data);
    if (!completion) {
        return NULL;
    }
    sockaddr_state = PyMem_Malloc(sizeof(UringApiCompletionSockaddrState));
    if (!sockaddr_state) {
        Py_DECREF(completion);
        return PyErr_NoMemory();
    }
    memset(sockaddr_state, 0, sizeof(*sockaddr_state));
    sockaddr_state->addrlen = sizeof(sockaddr_state->addr);
    completion->state_kind = URING_API_COMPLETION_STATE_SOCKADDR;
    completion->state = sockaddr_state;
    return (PyObject *)completion;
}

PyObject *UringApiCompletion_new_delivered_copy(UringApiCompletion *source) {
    UringApiCompletion *completion;

    completion = UringApiCompletion_alloc(source->kind, source->user_data);
    if (!completion) {
        return NULL;
    }
    completion->res = source->res;
    completion->flags = source->flags;
    completion->result = source->result;
    source->result = NULL;
    completion->sequence = source->sequence;
    source->sequence++;
    completion->multishot = source->multishot;
    return (PyObject *)completion;
}

void UringApiCompletion_clear_pending_state(UringApiCompletion *self) {
    UringApiCompletionBufGroupState *buf_group_state;
    UringApiCompletionViewState *view_state;
    UringApiCompletionMsgState *msg_state;

    switch (self->state_kind) {
    case URING_API_COMPLETION_STATE_VIEW:
        view_state = (UringApiCompletionViewState *)self->state;
        UringApiCompletion_release_view_state(view_state);
        UringApiCompletion_free_state(self);
        break;
    case URING_API_COMPLETION_STATE_BUF_GROUP:
        if (self->state) {
            buf_group_state = (UringApiCompletionBufGroupState *)self->state;
            Py_CLEAR(buf_group_state->buf_group);
        }
        UringApiCompletion_free_state(self);
        break;
    case URING_API_COMPLETION_STATE_SOCKADDR:
        UringApiCompletion_free_state(self);
        break;
    case URING_API_COMPLETION_STATE_MSG:
        msg_state = (UringApiCompletionMsgState *)self->state;
        UringApiCompletion_release_msg_view(msg_state);
        UringApiCompletion_free_state(self);
        break;
    case URING_API_COMPLETION_STATE_NONE:
        break;
    }
}

static bool UringApiCompletion_should_clear_pending_state(UringApiCompletion *self, int res, unsigned int flags) {
    if (is_zero_copy_send_kind(self->kind) && res >= 0 && !(flags & IORING_CQE_F_NOTIF)) {
        return false;
    }
    return !(flags & IORING_CQE_F_MORE);
}

static void UringApiCompletion_recycle_selected_buffer(UringApiCompletion *self, unsigned int flags) {
    UringApiBufGroup *buf_group;
    PyObject *buf_group_obj;
    unsigned int buffer_id;

    if (!(flags & IORING_CQE_F_BUFFER)) {
        return;
    }
    buf_group_obj = UringApiCompletion_get_buf_group(self);
    if (!buf_group_obj || !PyObject_TypeCheck(buf_group_obj, &UringApiBufGroup_Type)) {
        return;
    }
    buf_group = (UringApiBufGroup *)buf_group_obj;
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
    PyObject *buf_group_obj;
    unsigned int buffer_id;

    if (res < 0) {
        UringApiCompletion_recycle_selected_buffer(self, flags);
        Py_RETURN_NONE;
    }
    buf_group_obj = UringApiCompletion_get_buf_group(self);
    if (!buf_group_obj || !PyObject_TypeCheck(buf_group_obj, &UringApiBufGroup_Type)) {
        PyErr_SetString(PyExc_RuntimeError, "provided-buffer recv completion has no buffer group");
        return NULL;
    }
    if (res == 0 && !(flags & IORING_CQE_F_BUFFER)) {
        return UringApiBufView_create_empty(buf_group_obj);
    }
    if (!(flags & IORING_CQE_F_BUFFER)) {
        PyErr_SetString(PyExc_RuntimeError, "provided-buffer recv completion did not select a buffer");
        return NULL;
    }
    buffer_id = flags >> IORING_CQE_BUFFER_SHIFT;
    if (buffer_id >= ((UringApiBufGroup *)buf_group_obj)->buffer_count) {
        PyErr_SetString(PyExc_RuntimeError, "provided-buffer recv completion selected an invalid buffer");
        return NULL;
    }
    if ((unsigned int)res > ((UringApiBufGroup *)buf_group_obj)->buffer_size) {
        PyErr_SetString(PyExc_RuntimeError, "provided-buffer recv completion exceeds selected buffer size");
        return NULL;
    }
    return UringApiBufView_create(buf_group_obj, buffer_id, (unsigned int)res);
}

int UringApiCompletion_complete(UringApiCompletion *self, int res, unsigned int flags) {
    PyObject *payload;
    UringApiCompletionMsgState *msg_state;
    UringApiCompletionSockaddrState *sockaddr_state;

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
        msg_state = UringApiCompletion_get_msg_state(self);
        if (!msg_state) {
            PyErr_SetString(PyExc_RuntimeError, "recvmsg completion is missing message state");
            return -1;
        }
        msg_state->addrlen = msg_state->msg.msg_namelen;
        payload = sockaddr_to_object(&msg_state->addr, msg_state->addrlen);
    } else if (res >= 0 && self->kind == URING_API_PENDING_ACCEPT) {
        sockaddr_state = UringApiCompletion_get_sockaddr_state(self);
        if (!sockaddr_state) {
            PyErr_SetString(PyExc_RuntimeError, "accept completion is missing sockaddr state");
            return -1;
        }
        payload = sockaddr_to_object(&sockaddr_state->addr, sockaddr_state->addrlen);
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

static PyObject *UringApiCompletion_get_multishot(UringApiCompletion *self, void *closure) {
    return PyBool_FromLong(self->multishot);
}

static PyGetSetDef UringApiCompletion_getset[] = {
    {"user_data", (getter)UringApiCompletion_get_user_data, NULL, NULL, NULL},
    {"kind", (getter)UringApiCompletion_get_kind, NULL, NULL, NULL},
    {"res", (getter)UringApiCompletion_get_res, NULL, NULL, NULL},
    {"flags", (getter)UringApiCompletion_get_flags, NULL, NULL, NULL},
    {"result", (getter)UringApiCompletion_get_result, NULL, NULL, NULL},
    {"sequence", (getter)UringApiCompletion_get_sequence, NULL, NULL, NULL},
    {"multishot", (getter)UringApiCompletion_get_multishot, NULL, NULL, NULL},
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