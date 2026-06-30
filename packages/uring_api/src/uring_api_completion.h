#ifndef URING_API_COMPLETION_H
#define URING_API_COMPLETION_H

/* private implementation header; not part of the public C API. */

#include "uring_api_common.h"

typedef struct {
    Py_buffer view;
    bool has_view;
} UringApiCompletionViewState;

typedef struct {
    PyObject *buf_group;
} UringApiCompletionBufGroupState;

typedef struct {
    struct sockaddr_storage addr;
    socklen_t addrlen;
} UringApiCompletionSockaddrState;

typedef struct {
    Py_buffer view;
    bool has_view;
    struct iovec iov;
    struct msghdr msg;
    struct sockaddr_storage addr;
    socklen_t addrlen;
} UringApiCompletionMsgState;

int completion_type_check(PyObject *completion);
PyObject *UringApiCompletion_new_pending(UringApiPendingKind kind, PyObject *user_data);
PyObject *UringApiCompletion_new_pending_buf_group(UringApiPendingKind kind, PyObject *user_data, PyObject *buf_group);
PyObject *UringApiCompletion_new_pending_view(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view);
PyObject *UringApiCompletion_new_pending_recvmsg(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view);
PyObject *UringApiCompletion_new_pending_sendmsg(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view);
bool is_zero_copy_send_kind(UringApiPendingKind kind);
PyObject *UringApiCompletion_new_pending_accept(PyObject *user_data);
PyObject *UringApiCompletion_new_delivered_copy(UringApiCompletion *source);
void UringApiCompletion_clear_pending_state(UringApiCompletion *self);
int UringApiCompletion_complete(UringApiCompletion *self, int res, unsigned int flags);
UringApiCompletionSockaddrState *UringApiCompletion_get_sockaddr_state(UringApiCompletion *self);
UringApiCompletionMsgState *UringApiCompletion_get_msg_state(UringApiCompletion *self);

#endif