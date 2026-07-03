#ifndef URING_API_COMPLETION_H
#define URING_API_COMPLETION_H

/* private implementation header; not part of the public C API. */

#include "uring_api_common.h"

typedef struct {
    UringApiCompletionStateKind tag;
} UringApiCompletionStateHeader;

typedef struct {
    UringApiCompletionStateKind tag;
    Py_buffer view;
    bool has_view;
} UringApiCompletionViewState;

typedef struct {
    UringApiCompletionStateKind tag;
    PyObject *buf_group;
} UringApiCompletionBufGroupState;

typedef struct {
    UringApiCompletionStateKind tag;
    struct sockaddr_storage addr;
    socklen_t addrlen;
} UringApiCompletionSockaddrState;

typedef struct {
    UringApiCompletionStateKind tag;
    Py_buffer view;
    bool has_view;
    struct sockaddr_storage addr;
    socklen_t addrlen;
} UringApiCompletionViewSockaddrState;

typedef struct {
    UringApiCompletionStateKind tag;
    Py_buffer view;
    bool has_view;
    struct iovec iov;
    struct msghdr msg;
    struct sockaddr_storage addr;
    socklen_t addrlen;
} UringApiCompletionMsgState;

typedef struct {
    UringApiCompletionStateKind tag;
    char *path;
} UringApiCompletionPathState;

typedef struct {
    UringApiCompletionStateKind tag;
    char *path;
    Py_buffer view;
    bool has_view;
} UringApiCompletionStatxState;

typedef struct {
    UringApiCompletionStateKind tag;
    unsigned char buf[256];
} UringApiCompletionStatxFdsizeState;

int completion_type_check(PyObject *completion);
PyObject *UringApiCompletion_new_pending(UringApiPendingKind kind, PyObject *user_data);
PyObject *UringApiCompletion_new_pending_buf_group(UringApiPendingKind kind, PyObject *user_data, PyObject *buf_group);
PyObject *UringApiCompletion_new_pending_view(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view);
PyObject *UringApiCompletion_new_pending_view_sockaddr(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view);
PyObject *UringApiCompletion_new_pending_sockaddr(UringApiPendingKind kind, PyObject *user_data);
PyObject *UringApiCompletion_new_pending_path(UringApiPendingKind kind, PyObject *user_data, PyObject *path);
PyObject *UringApiCompletion_new_pending_statx(UringApiPendingKind kind, PyObject *user_data, PyObject *path,
                                               Py_buffer *view);
PyObject *UringApiCompletion_new_pending_statx_fdsize(PyObject *user_data);
UringApiCompletionStatxFdsizeState *UringApiCompletion_get_statx_fdsize_state(UringApiCompletion *self);
PyObject *UringApiCompletion_new_pending_recvmsg(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view);
PyObject *UringApiCompletion_new_pending_sendmsg(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view);
bool is_zero_copy_send_kind(UringApiPendingKind kind);
PyObject *UringApiCompletion_new_pending_accept(PyObject *user_data);
PyObject *UringApiCompletion_new_delivered_copy(UringApiCompletion *source);
void UringApiCompletion_clear_pending_state(UringApiCompletion *self);
int UringApiCompletion_complete(UringApiCompletion *self, int res, unsigned int flags);
UringApiCompletionSockaddrState *UringApiCompletion_get_sockaddr_state(UringApiCompletion *self);
UringApiCompletionViewSockaddrState *UringApiCompletion_get_view_sockaddr_state(UringApiCompletion *self);
UringApiCompletionMsgState *UringApiCompletion_get_msg_state(UringApiCompletion *self);
UringApiCompletionPathState *UringApiCompletion_get_path_state(UringApiCompletion *self);
UringApiCompletionStatxState *UringApiCompletion_get_statx_state(UringApiCompletion *self);

#endif
