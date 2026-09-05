/*
 * Completion object support for the _uring_api extension.
 */

#include "uring_api_completion.h"
#include "uring_api_bufgroup.h"
#include "uring_api_bufview.h"
#include "uring_api_core.h"
#include "uring_api_statx.h"

#include <string.h>

static int UringApiCompletion_clear(UringApiCompletion *self);

static UringApiCompletionStateKind UringApiCompletion_state_tag(const UringApiCompletion *self) {
    if (!self->state) {
        return URING_API_COMPLETION_STATE_NONE;
    }
    return ((const UringApiCompletionStateHeader *)self->state)->tag;
}

static PyObject *UringApiCompletion_get_buf_group(UringApiCompletion *self) {
    UringApiCompletionBufGroupState *buf_group_state;

    if (UringApiCompletion_state_tag(self) != URING_API_COMPLETION_STATE_BUF_GROUP) {
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

static void UringApiCompletion_release_view_sockaddr_state(UringApiCompletionViewSockaddrState *view_sockaddr_state) {
    if (!view_sockaddr_state) {
        return;
    }
    if (view_sockaddr_state->has_view) {
        PyBuffer_Release(&view_sockaddr_state->view);
        view_sockaddr_state->has_view = false;
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
    UringApiCompletionViewSockaddrState *view_sockaddr_state;
    UringApiCompletionMsgState *msg_state;

    if (!self->state) {
        return;
    }

    switch (UringApiCompletion_state_tag(self)) {
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
        PyMem_Free((UringApiCompletionSockaddrState *)self->state);
        break;
    case URING_API_COMPLETION_STATE_VIEW_SOCKADDR:
        view_sockaddr_state = (UringApiCompletionViewSockaddrState *)self->state;
        UringApiCompletion_release_view_sockaddr_state(view_sockaddr_state);
        PyMem_Free(view_sockaddr_state);
        break;
    case URING_API_COMPLETION_STATE_MSG:
        msg_state = (UringApiCompletionMsgState *)self->state;
        UringApiCompletion_release_msg_view(msg_state);
        PyMem_Free(msg_state);
        break;
    case URING_API_COMPLETION_STATE_PATH:
        PyMem_Free(((UringApiCompletionPathState *)self->state)->path);
        PyMem_Free(self->state);
        break;
    case URING_API_COMPLETION_STATE_STATX: {
        UringApiCompletionStatxState *statx_state = (UringApiCompletionStatxState *)self->state;
        if (statx_state->has_view) {
            PyBuffer_Release(&statx_state->view);
            statx_state->has_view = false;
        }
        PyMem_Free(statx_state->path);
        PyMem_Free(statx_state);
        break;
    }
    case URING_API_COMPLETION_STATE_STATX_FDSIZE:
        PyMem_Free((UringApiCompletionStatxFdsizeState *)self->state);
        break;
    case URING_API_COMPLETION_STATE_SCALAR:
        PyMem_Free((UringApiCompletionScalarState *)self->state);
        break;
    case URING_API_COMPLETION_STATE_NONE:
        if (self->state) {
            /* tag was zeroed or corrupt; free orphaned allocation */
            PyMem_Free(self->state);
        }
        break;
    default:
        /* unknown state tag; free to avoid leaking */
        PyMem_Free(self->state);
        break;
    }

    self->state = NULL;
}

UringApiCompletionViewState *UringApiCompletion_get_view_state(UringApiCompletion *self) {
    if (UringApiCompletion_state_tag(self) != URING_API_COMPLETION_STATE_VIEW) {
        return NULL;
    }
    return (UringApiCompletionViewState *)self->state;
}

UringApiCompletionBufGroupState *UringApiCompletion_get_buf_group_state(UringApiCompletion *self) {
    if (UringApiCompletion_state_tag(self) != URING_API_COMPLETION_STATE_BUF_GROUP) {
        return NULL;
    }
    return (UringApiCompletionBufGroupState *)self->state;
}

UringApiCompletionSockaddrState *UringApiCompletion_get_sockaddr_state(UringApiCompletion *self) {
    if (UringApiCompletion_state_tag(self) != URING_API_COMPLETION_STATE_SOCKADDR) {
        return NULL;
    }
    return (UringApiCompletionSockaddrState *)self->state;
}

UringApiCompletionViewSockaddrState *UringApiCompletion_get_view_sockaddr_state(UringApiCompletion *self) {
    if (UringApiCompletion_state_tag(self) != URING_API_COMPLETION_STATE_VIEW_SOCKADDR) {
        return NULL;
    }
    return (UringApiCompletionViewSockaddrState *)self->state;
}

UringApiCompletionMsgState *UringApiCompletion_get_msg_state(UringApiCompletion *self) {
    if (UringApiCompletion_state_tag(self) != URING_API_COMPLETION_STATE_MSG) {
        return NULL;
    }
    return (UringApiCompletionMsgState *)self->state;
}

UringApiCompletionPathState *UringApiCompletion_get_path_state(UringApiCompletion *self) {
    if (UringApiCompletion_state_tag(self) != URING_API_COMPLETION_STATE_PATH) {
        return NULL;
    }
    return (UringApiCompletionPathState *)self->state;
}

UringApiCompletionStatxState *UringApiCompletion_get_statx_state(UringApiCompletion *self) {
    if (UringApiCompletion_state_tag(self) != URING_API_COMPLETION_STATE_STATX) {
        return NULL;
    }
    return (UringApiCompletionStatxState *)self->state;
}

UringApiCompletionStatxFdsizeState *UringApiCompletion_get_statx_fdsize_state(UringApiCompletion *self) {
    if (UringApiCompletion_state_tag(self) != URING_API_COMPLETION_STATE_STATX_FDSIZE) {
        return NULL;
    }
    return (UringApiCompletionStatxFdsizeState *)self->state;
}

UringApiCompletionScalarState *UringApiCompletion_get_scalar_state(UringApiCompletion *self) {
    if (UringApiCompletion_state_tag(self) != URING_API_COMPLETION_STATE_SCALAR) {
        return NULL;
    }
    return (UringApiCompletionScalarState *)self->state;
}

static char *copy_unicode_path(PyObject *path_obj) {
    Py_ssize_t path_len;
    const char *path_utf8;
    char *path_copy;

    if (!PyUnicode_Check(path_obj)) {
        PyErr_SetString(PyExc_TypeError, "path must be a str");
        return NULL;
    }
    path_utf8 = PyUnicode_AsUTF8AndSize(path_obj, &path_len);
    if (!path_utf8) {
        return NULL;
    }
    path_copy = PyMem_Malloc((size_t)path_len + 1);
    if (!path_copy) {
        PyErr_NoMemory();
        return NULL;
    }
    memcpy(path_copy, path_utf8, (size_t)path_len + 1);
    return path_copy;
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
    UringApiCompletionMsgState *msg_state;
    UringApiCompletionStatxState *statx_state;
    UringApiCompletionViewState *view_state;
    UringApiCompletionViewSockaddrState *view_sockaddr_state;
    PyObject *buf_group;

    buf_group = UringApiCompletion_get_buf_group(self);
    Py_VISIT(buf_group);
    Py_VISIT(self->user_data);
    Py_VISIT(self->cancel_target);
    Py_VISIT(self->result);

    switch (UringApiCompletion_state_tag(self)) {
    case URING_API_COMPLETION_STATE_VIEW:
        view_state = (UringApiCompletionViewState *)self->state;
        if (view_state->has_view) {
            Py_VISIT(view_state->view.obj);
        }
        break;
    case URING_API_COMPLETION_STATE_VIEW_SOCKADDR:
        view_sockaddr_state = (UringApiCompletionViewSockaddrState *)self->state;
        if (view_sockaddr_state->has_view) {
            Py_VISIT(view_sockaddr_state->view.obj);
        }
        break;
    case URING_API_COMPLETION_STATE_MSG:
        msg_state = (UringApiCompletionMsgState *)self->state;
        if (msg_state->has_view) {
            Py_VISIT(msg_state->view.obj);
        }
        break;
    case URING_API_COMPLETION_STATE_STATX:
        statx_state = (UringApiCompletionStatxState *)self->state;
        if (statx_state->has_view) {
            Py_VISIT(statx_state->view.obj);
        }
        break;
    default:
        break;
    }

    return 0;
}

static int UringApiCompletion_clear(UringApiCompletion *self) {
    UringApiCompletion_free_state(self);
    Py_CLEAR(self->user_data);
    Py_CLEAR(self->cancel_target);
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
    completion->cancel_target = NULL;
    completion->res = 0;
    completion->flags = 0;
    completion->result = NULL;
    completion->sequence = 0;
    completion->aux_refcount = 0;
    completion->aux_lock = NULL;
    atomic_init(&completion->bits, 0);
    completion->state = NULL;
    PyObject_GC_Track(completion);
    return completion;
}

static bool is_zero_copy_send_kind(UringApiPendingKind kind) {
    return kind == URING_API_PENDING_SEND_ZC || kind == URING_API_PENDING_SENDMSG_ZC;
}

/* bump the outstanding-CQE count for a multi-step armed Completion; sticky aux_decref
 * records that a terminal leg was staged and the in-flight ref should drop once the
 * count returns to zero. */
static void completion_aux_stage_cqe(UringApiRing *ring, UringApiCompletion *completion, bool want_to_decref) {
    uring_api_refcount_mutex_lock(&ring->refcount_mutex);
    completion->aux_refcount++;
    if (want_to_decref) {
        completion_set_bit(completion, URING_API_C_AUX_DECREF);
    }
    uring_api_refcount_mutex_unlock(&ring->refcount_mutex);
}

/* drop the outstanding-CQE count after build; return whether the in-flight ref may
 * leave now (count reached zero and a terminal leg had been staged). apply a
 * pending clear_user_data once no staged leg will still copy the live slot. */
static bool completion_aux_finish_cqe(UringApiRing *ring, UringApiCompletion *completion) {
    bool decref = false;
    PyObject *old = NULL;

    uring_api_refcount_mutex_lock(&ring->refcount_mutex);
    completion->aux_refcount--;
    if (completion->aux_refcount == 0) {
        if (completion_has_bit(completion, URING_API_C_AUX_DECREF)) {
            decref = true;
            completion_clear_bit(completion, URING_API_C_AUX_DECREF);
        }
        if (completion_has_bit(completion, URING_API_C_USER_DATA_CLEAR)) {
            completion_clear_bit(completion, URING_API_C_USER_DATA_CLEAR);
            old = completion->user_data;
            completion->user_data = Py_NewRef(Py_None);
        }
    }
    uring_api_refcount_mutex_unlock(&ring->refcount_mutex);
    Py_XDECREF(old);
    return decref;
}

static int send_all_cqe_is_terminal(UringApiCompletion *completion, int res) {
    UringApiCompletionViewState *view_state;
    Py_ssize_t remaining;

    if (completion_has_bit(completion, URING_API_C_SEND_ALL_ABANDON) || res <= 0) {
        return 1;
    }
    view_state = UringApiCompletion_get_view_state(completion);
    if (view_state == NULL || !view_state->has_view) {
        return 1;
    }
    if (view_state->offset >= (unsigned long long)view_state->view.len) {
        return 1;
    }
    remaining = view_state->view.len - (Py_ssize_t)view_state->offset;
    return res >= remaining;
}

/* called while draining CQEs outside the GIL: track multi-step armed Completions
 * whose in-flight ref must not drop until every staged leg has been built. */
void completion_prep_in_flight_ref(UringApiRing *ring, UringApiCompletion *completion, int res, unsigned int flags) {
    bool multi_step = false;
    bool want_to_decref = true;

    if (completion_has_bit(completion, URING_API_C_MULTISHOT)) {
        multi_step = true;
        want_to_decref = !(flags & IORING_CQE_F_MORE);
    } else if (is_zero_copy_send_kind(completion->kind)) {
        multi_step = true;
        want_to_decref = (flags & IORING_CQE_F_NOTIF) != 0;
    } else if (completion->kind == URING_API_PENDING_SEND_ALL) {
        multi_step = true;
        want_to_decref = send_all_cqe_is_terminal(completion, res) != 0;
    }
    if (multi_step) {
        completion_aux_stage_cqe(ring, completion, want_to_decref);
    }
}

/* called under the GIL after the shell/handle for this staged CQE is built.
 * one-shot ops always return true; multi-step ops may return false when a
 * terminal leg is packaged before an earlier F_MORE leg on another thread. */
bool completion_finish_in_flight_ref(UringApiRing *ring, UringApiCompletion *completion) {
    if (completion_has_bit(completion, URING_API_C_MULTISHOT) || is_zero_copy_send_kind(completion->kind) ||
        completion->kind == URING_API_PENDING_SEND_ALL) {
        return completion_aux_finish_cqe(ring, completion);
    }
    return true;
}

PyObject *UringApiCompletion_new_pending(UringApiPendingKind kind, PyObject *user_data) {
    return (PyObject *)UringApiCompletion_alloc(kind, user_data);
}

PyObject *UringApiCompletion_new_pending_scalar(UringApiPendingKind kind, PyObject *user_data) {
    UringApiCompletion *completion;
    UringApiCompletionScalarState *scalar_state;

    completion = UringApiCompletion_alloc(kind, user_data);
    if (!completion) {
        return NULL;
    }
    scalar_state = PyMem_Malloc(sizeof(UringApiCompletionScalarState));
    if (!scalar_state) {
        Py_DECREF(completion);
        return PyErr_NoMemory();
    }
    memset(scalar_state, 0, sizeof(*scalar_state));
    scalar_state->tag = URING_API_COMPLETION_STATE_SCALAR;
    scalar_state->constructed = false;
    completion->state = scalar_state;
    return (PyObject *)completion;
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
    buf_group_state->tag = URING_API_COMPLETION_STATE_BUF_GROUP;
    buf_group_state->buf_group = Py_NewRef(buf_group);
    buf_group_state->fd = -1;
    buf_group_state->flags = 0;
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
    view_state->tag = URING_API_COMPLETION_STATE_VIEW;
    view_state->view = *view;
    view_state->has_view = true;
    view_state->fd = -1;
    view_state->flags = 0;
    view_state->zc_flags = 0;
    view_state->offset = 0;
    completion->state = view_state;
    return (PyObject *)completion;
}

PyObject *UringApiCompletion_new_pending_view_sockaddr(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view) {
    UringApiCompletion *completion;
    UringApiCompletionViewSockaddrState *view_sockaddr_state;

    completion = UringApiCompletion_alloc(kind, user_data);
    if (!completion) {
        PyBuffer_Release(view);
        return NULL;
    }
    view_sockaddr_state = PyMem_Malloc(sizeof(UringApiCompletionViewSockaddrState));
    if (!view_sockaddr_state) {
        Py_DECREF(completion);
        PyBuffer_Release(view);
        return PyErr_NoMemory();
    }
    memset(view_sockaddr_state, 0, sizeof(*view_sockaddr_state));
    view_sockaddr_state->tag = URING_API_COMPLETION_STATE_VIEW_SOCKADDR;
    view_sockaddr_state->view = *view;
    view_sockaddr_state->has_view = true;
    view_sockaddr_state->addrlen = sizeof(view_sockaddr_state->addr);
    view_sockaddr_state->fd = -1;
    view_sockaddr_state->flags = 0;
    completion->state = view_sockaddr_state;
    return (PyObject *)completion;
}

PyObject *UringApiCompletion_new_pending_sockaddr(UringApiPendingKind kind, PyObject *user_data) {
    UringApiCompletion *completion;
    UringApiCompletionSockaddrState *sockaddr_state;

    completion = UringApiCompletion_alloc(kind, user_data);
    if (!completion) {
        return NULL;
    }
    sockaddr_state = PyMem_Malloc(sizeof(UringApiCompletionSockaddrState));
    if (!sockaddr_state) {
        Py_DECREF(completion);
        return PyErr_NoMemory();
    }
    memset(sockaddr_state, 0, sizeof(*sockaddr_state));
    sockaddr_state->tag = URING_API_COMPLETION_STATE_SOCKADDR;
    sockaddr_state->addrlen = sizeof(sockaddr_state->addr);
    sockaddr_state->fd = -1;
    completion->state = sockaddr_state;
    return (PyObject *)completion;
}

PyObject *UringApiCompletion_new_pending_path(UringApiPendingKind kind, PyObject *user_data, PyObject *path) {
    UringApiCompletion *completion;
    UringApiCompletionPathState *path_state;
    char *path_copy;

    path_copy = copy_unicode_path(path);
    if (!path_copy) {
        return NULL;
    }
    completion = UringApiCompletion_alloc(kind, user_data);
    if (!completion) {
        PyMem_Free(path_copy);
        return NULL;
    }
    path_state = PyMem_Malloc(sizeof(UringApiCompletionPathState));
    if (!path_state) {
        Py_DECREF(completion);
        PyMem_Free(path_copy);
        PyErr_NoMemory();
        return NULL;
    }
    path_state->tag = URING_API_COMPLETION_STATE_PATH;
    path_state->path = path_copy;
    path_state->dfd = 0;
    path_state->flags = 0;
    path_state->mode = 0;
    path_state->constructed = false;
    completion->state = path_state;
    return (PyObject *)completion;
}

PyObject *UringApiCompletion_new_pending_statx(UringApiPendingKind kind, PyObject *user_data, PyObject *path,
                                               Py_buffer *view) {
    UringApiCompletion *completion;
    UringApiCompletionStatxState *statx_state;
    char *path_copy;

    path_copy = copy_unicode_path(path);
    if (!path_copy) {
        PyBuffer_Release(view);
        return NULL;
    }
    completion = UringApiCompletion_alloc(kind, user_data);
    if (!completion) {
        PyMem_Free(path_copy);
        PyBuffer_Release(view);
        return NULL;
    }
    statx_state = PyMem_Malloc(sizeof(UringApiCompletionStatxState));
    if (!statx_state) {
        Py_DECREF(completion);
        PyMem_Free(path_copy);
        PyBuffer_Release(view);
        PyErr_NoMemory();
        return NULL;
    }
    statx_state->tag = URING_API_COMPLETION_STATE_STATX;
    statx_state->path = path_copy;
    statx_state->view = *view;
    statx_state->has_view = true;
    statx_state->dfd = 0;
    statx_state->flags = 0;
    statx_state->mask = 0;
    statx_state->constructed = false;
    completion->state = statx_state;
    return (PyObject *)completion;
}

PyObject *UringApiCompletion_new_pending_statx_fdsize(PyObject *user_data) {
    UringApiCompletion *completion;
    UringApiCompletionStatxFdsizeState *statx_fdsize_state;

    completion = UringApiCompletion_alloc(URING_API_PENDING_STATX_FDSIZE, user_data);
    if (!completion) {
        return NULL;
    }
    statx_fdsize_state = PyMem_Malloc(sizeof(UringApiCompletionStatxFdsizeState));
    if (!statx_fdsize_state) {
        Py_DECREF(completion);
        PyErr_NoMemory();
        return NULL;
    }
    statx_fdsize_state->tag = URING_API_COMPLETION_STATE_STATX_FDSIZE;
    memset(statx_fdsize_state->buf, 0, sizeof(statx_fdsize_state->buf));
    statx_fdsize_state->fd = -1;
    statx_fdsize_state->constructed = false;
    completion->state = statx_fdsize_state;
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
    msg_state->tag = URING_API_COMPLETION_STATE_MSG;
    msg_state->view = *view;
    msg_state->has_view = true;
    msg_state->addrlen = sizeof(msg_state->addr);
    msg_state->fd = -1;
    msg_state->flags = 0;
    msg_state->iov.iov_base = view->buf;
    msg_state->iov.iov_len = (size_t)view->len;
    msg_state->msg.msg_name = &msg_state->addr;
    msg_state->msg.msg_namelen = msg_state->addrlen;
    msg_state->msg.msg_iov = &msg_state->iov;
    msg_state->msg.msg_iovlen = 1;
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
    msg_state->tag = URING_API_COMPLETION_STATE_MSG;
    msg_state->view = *view;
    msg_state->has_view = true;
    msg_state->fd = -1;
    msg_state->flags = 0;
    msg_state->iov.iov_base = view->buf;
    msg_state->iov.iov_len = (size_t)view->len;
    msg_state->msg.msg_iov = &msg_state->iov;
    msg_state->msg.msg_iovlen = 1;
    completion->state = msg_state;
    return (PyObject *)completion;
}

/* Intermediate MORE leg only. Copies live user_data from the armed handle.
 * take_user_data() / clear_user_data() on that handle defer while
 * aux_refcount > 0, so a concurrent !MORE delivery cannot nerf this slot
 * before the copy. Does not replace that handle. Terminal !MORE delivers
 * the source itself. */
PyObject *UringApiCompletion_new_multishot_delivered_shell(UringApiCompletion *source, unsigned long long leg_index) {
    UringApiCompletion *completion;
    UringApiCompletionBufGroupState *source_buf_group_state;
    UringApiCompletionBufGroupState *buf_group_state;

    completion = UringApiCompletion_alloc(source->kind, source->user_data);
    if (!completion) {
        return NULL;
    }
    if (completion_has_bit(source, URING_API_C_MULTISHOT)) {
        completion_set_bit(completion, URING_API_C_MULTISHOT);
    }
    completion->sequence = leg_index;
    if (UringApiCompletion_state_tag(source) == URING_API_COMPLETION_STATE_BUF_GROUP) {
        source_buf_group_state = (UringApiCompletionBufGroupState *)source->state;
        buf_group_state = PyMem_Malloc(sizeof(UringApiCompletionBufGroupState));
        if (!buf_group_state) {
            Py_DECREF(completion);
            return PyErr_NoMemory();
        }
        memset(buf_group_state, 0, sizeof(*buf_group_state));
        buf_group_state->tag = URING_API_COMPLETION_STATE_BUF_GROUP;
        buf_group_state->buf_group = Py_NewRef(source_buf_group_state->buf_group);
        /* do not copy source fd/flags: a MORE shell is not a constructed handle */
        buf_group_state->fd = -1;
        buf_group_state->flags = 0;
        completion->state = buf_group_state;
    }
    return (PyObject *)completion;
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

static PyObject *statx_fdsize_completion_size_payload(const void *buf, Py_ssize_t buflen) {
    unsigned long long file_size;

    if (uring_api_statx_try_read_st_size(buf, buflen, &file_size)) {
        return PyLong_FromUnsignedLongLong(file_size);
    }
    return Py_NewRef(Py_None);
}

int UringApiCompletion_complete(UringApiCompletion *self, int res, unsigned int flags) {
    PyObject *payload;
    UringApiCompletionStatxFdsizeState *statx_fdsize_state;
    UringApiCompletionMsgState *msg_state;
    UringApiCompletionSockaddrState *sockaddr_state;

    if (is_zero_copy_send_kind(self->kind) && (flags & IORING_CQE_F_NOTIF)) {
        return 1;
    }
    self->res = res;
    self->flags = flags;
    if (self->kind == URING_API_PENDING_RECV_MULTISHOT || self->kind == URING_API_PENDING_RECV_BUF) {
        payload = UringApiCompletion_recv_multishot_buf_payload(self, res, flags);
    } else if (res >= 0 && self->kind == URING_API_PENDING_STATX_FDSIZE) {
        statx_fdsize_state = UringApiCompletion_get_statx_fdsize_state(self);
        if (!statx_fdsize_state) {
            PyErr_SetString(PyExc_RuntimeError, "statx_fdsize completion is missing buffer state");
            return -1;
        }
        payload =
            statx_fdsize_completion_size_payload(statx_fdsize_state->buf, (Py_ssize_t)sizeof(statx_fdsize_state->buf));
    } else if (res >= 0 && (self->kind == URING_API_PENDING_RECV || self->kind == URING_API_PENDING_SEND ||
                            self->kind == URING_API_PENDING_SEND_ALL || self->kind == URING_API_PENDING_READ ||
                            self->kind == URING_API_PENDING_WRITE || is_zero_copy_send_kind(self->kind) ||
                            self->kind == URING_API_PENDING_SENDTO || self->kind == URING_API_PENDING_SENDMSG ||
                            self->kind == URING_API_PENDING_SOCKET || self->kind == URING_API_PENDING_POLL ||
                            self->kind == URING_API_PENDING_POLL_MULTISHOT || self->kind == URING_API_PENDING_OPENAT)) {
        payload = PyLong_FromLong(res);
    } else if (res >= 0 && self->kind == URING_API_PENDING_STATX) {
        payload = Py_NewRef(Py_None);
    } else if (res >= 0 && self->kind == URING_API_PENDING_RECVMSG) {
        msg_state = UringApiCompletion_get_msg_state(self);
        if (!msg_state) {
            PyErr_SetString(PyExc_RuntimeError, "recvmsg completion is missing message state");
            return -1;
        }
        msg_state->addrlen = msg_state->msg.msg_namelen;
        payload = sockaddr_to_object(&msg_state->addr, msg_state->addrlen);
    } else if (self->kind == URING_API_PENDING_ACCEPT) {
        if (res >= 0) {
            payload = PyLong_FromLong(res);
        } else {
            payload = Py_NewRef(Py_None);
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
    return 0;
}

static PyObject *UringApiCompletion_get_user_data(UringApiCompletion *self, void *closure) {
    return Py_NewRef(self->user_data);
}

PyObject *UringApiCompletion_take_user_data(UringApiCompletion *self) {
    PyObject *taken;

    /* shells / oneshot / idle: aux is 0 → steal the slot now. armed
     * multishot with staged CQEs: return a new ref and mark USER_DATA_CLEAR
     * so aux_finish drops the live slot after the last shell has copied it.
     * swap the pointer under the lock so the flag and slot stay consistent. */
    if (self->aux_lock != NULL) {
        uring_api_refcount_mutex_lock(self->aux_lock);
        if (self->aux_refcount > 0) {
            completion_set_bit(self, URING_API_C_USER_DATA_CLEAR);
            taken = Py_NewRef(self->user_data);
        } else {
            taken = self->user_data;
            self->user_data = Py_NewRef(Py_None);
        }
        uring_api_refcount_mutex_unlock(self->aux_lock);
        return taken;
    }
    taken = self->user_data;
    self->user_data = Py_NewRef(Py_None);
    return taken;
}

int UringApiCompletion_clear_user_data(UringApiCompletion *self) {
    PyObject *old = UringApiCompletion_take_user_data(self);

    Py_DECREF(old);
    return 0;
}

static PyObject *UringApiCompletion_take_user_data_method(UringApiCompletion *self, PyObject *Py_UNUSED(ignored)) {
    return UringApiCompletion_take_user_data(self);
}

static PyObject *UringApiCompletion_clear_user_data_method(UringApiCompletion *self, PyObject *Py_UNUSED(ignored)) {
    if (UringApiCompletion_clear_user_data(self) < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

int UringApiCompletion_assign_user_data(UringApiCompletion *self, PyObject *value) {
    PyObject *old;

    if (value == NULL || value == Py_None) {
        return UringApiCompletion_clear_user_data(self);
    }
    Py_INCREF(value);
    if (self->aux_lock != NULL) {
        uring_api_refcount_mutex_lock(self->aux_lock);
        completion_clear_bit(self, URING_API_C_USER_DATA_CLEAR);
        old = self->user_data;
        self->user_data = value;
        uring_api_refcount_mutex_unlock(self->aux_lock);
    } else {
        old = self->user_data;
        self->user_data = value;
    }
    Py_DECREF(old);
    return 0;
}

static int UringApiCompletion_set_user_data(UringApiCompletion *self, PyObject *value, void *closure) {
    /* del / None go through clear_user_data so a pending MORE shell can copy. */
    (void)closure;
    return UringApiCompletion_assign_user_data(self, value);
}

static PyObject *UringApiCompletion_get_cancel_target(UringApiCompletion *self, void *closure) {
    if (!self->cancel_target) {
        Py_RETURN_NONE;
    }
    return Py_NewRef(self->cancel_target);
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

static int UringApiCompletion_set_sequence(UringApiCompletion *self, PyObject *value, void *closure) {
    unsigned long long sequence;

    (void)closure;
    if (value == NULL) {
        PyErr_SetString(PyExc_TypeError, "cannot delete sequence");
        return -1;
    }
    sequence = PyLong_AsUnsignedLongLong(value);
    if (sequence == (unsigned long long)-1 && PyErr_Occurred()) {
        return -1;
    }
    self->sequence = sequence;
    return 0;
}

static PyObject *UringApiCompletion_get_multishot(UringApiCompletion *self, void *closure) {
    return PyBool_FromLong(completion_has_bit(self, URING_API_C_MULTISHOT));
}

static PyObject *UringApiCompletion_get_prepared(UringApiCompletion *self, void *closure) {
    return PyBool_FromLong(completion_has_bit(self, URING_API_C_PREPARED));
}

static int completion_kind_allows_nowait(UringApiPendingKind kind) {
    return kind == URING_API_PENDING_CLOSE || kind == URING_API_PENDING_SHUTDOWN || kind == URING_API_PENDING_CANCEL ||
           kind == URING_API_PENDING_POLL_REMOVE || kind == URING_API_PENDING_SEND_ALL;
}

int UringApiCompletion_set_nowait_flag(UringApiCompletion *self, int nowait) {
    if (completion_is_accepted(self)) {
        PyErr_SetString(PyExc_ValueError, "cannot change nowait after prepare");
        return -1;
    }
    if (nowait && !completion_kind_allows_nowait(self->kind)) {
        PyErr_SetString(PyExc_ValueError,
                        "nowait is only valid for close, shutdown, cancel, poll_remove, and send_all");
        return -1;
    }
    if (nowait) {
        completion_set_bit(self, URING_API_C_NOWAIT);
    } else {
        completion_clear_bit(self, URING_API_C_NOWAIT);
    }
    return 0;
}

static PyObject *UringApiCompletion_get_nowait(UringApiCompletion *self, void *closure) {
    (void)closure;
    return PyBool_FromLong(completion_has_bit(self, URING_API_C_NOWAIT));
}

static int UringApiCompletion_set_nowait(UringApiCompletion *self, PyObject *value, void *closure) {
    int nowait;

    (void)closure;
    if (value == NULL) {
        PyErr_SetString(PyExc_TypeError, "cannot delete nowait");
        return -1;
    }
    nowait = PyObject_IsTrue(value);
    if (nowait < 0) {
        return -1;
    }
    return UringApiCompletion_set_nowait_flag(self, nowait);
}

static PyGetSetDef UringApiCompletion_getset[] = {
    {
        "user_data",
        (getter)UringApiCompletion_get_user_data,
        (setter)UringApiCompletion_set_user_data,
        "Client payload. Assigning None is Completion.clear_user_data(). "
        "Callbacks that want possession should use take_user_data(). "
        "Undefined after the Ring object has been deallocated.",
        NULL,
    },
    {"cancel_target", (getter)UringApiCompletion_get_cancel_target, NULL, NULL, NULL},
    {"kind", (getter)UringApiCompletion_get_kind, NULL, NULL, NULL},
    {"res", (getter)UringApiCompletion_get_res, NULL, NULL, NULL},
    {"flags", (getter)UringApiCompletion_get_flags, NULL, NULL, NULL},
    {"result", (getter)UringApiCompletion_get_result, NULL, NULL, NULL},
    {"sequence", (getter)UringApiCompletion_get_sequence, (setter)UringApiCompletion_set_sequence,
     "Multishot leg ordinal. Set after construct to seed the first delivered leg.", NULL},
    {"multishot", (getter)UringApiCompletion_get_multishot, NULL, NULL, NULL},
    {"prepared", (getter)UringApiCompletion_get_prepared, NULL,
     "True after an SQE has been reserved and filled. Conflict-FIFO and fill-wait parks stay false.", NULL},
    {"nowait", (getter)UringApiCompletion_get_nowait, (setter)UringApiCompletion_set_nowait,
     "If true, prepare stamps a tagged nowait SQE and does not deliver this handle.", NULL},
    {NULL, NULL, NULL, NULL, NULL},
};

static PyMethodDef UringApiCompletion_methods[] = {
    {"take_user_data", (PyCFunction)UringApiCompletion_take_user_data_method, METH_NOARGS,
     "Return user_data and drop the slot when no staged MORE shell still needs it.\n"
     "On a shell or idle handle this steals the payload immediately. On an\n"
     "armed multishot handle with staged CQEs it returns a new reference and\n"
     "marks the slot so the live pointer is cleared after the last packaged\n"
     "leg (same window as aux_refcount).\n"
     "Undefined after the Ring object has been deallocated."},
    {"clear_user_data", (PyCFunction)UringApiCompletion_clear_user_data_method, METH_NOARGS,
     "Drop user_data when no staged MORE shell still needs the live slot.\n"
     "On a shell or idle handle this is immediate. On an armed multishot\n"
     "handle with staged CQEs it marks the slot and applies the clear after\n"
     "the last packaged leg (same window as aux_refcount).\n"
     "Same deferred-clear window as take_user_data(); use take when the\n"
     "callback needs the payload.\n"
     "Undefined after the Ring object has been deallocated."},
    {NULL, NULL, 0, NULL},
};

PyTypeObject UringApiCompletion_Type = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_uring_api.Completion",
    .tp_basicsize = sizeof(UringApiCompletion),
    .tp_dealloc = (destructor)UringApiCompletion_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)UringApiCompletion_traverse,
    .tp_clear = (inquiry)UringApiCompletion_clear,
    .tp_doc = "io_uring completion result",
    .tp_methods = UringApiCompletion_methods,
    .tp_getset = UringApiCompletion_getset,
};
