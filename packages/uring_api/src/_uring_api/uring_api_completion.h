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
    int fd;
    unsigned int flags;
    unsigned int zc_flags;
    unsigned long long offset;
} UringApiCompletionViewState;

typedef struct {
    UringApiCompletionStateKind tag;
    PyObject *buf_group;
    int fd;
    unsigned int flags;
} UringApiCompletionBufGroupState;

typedef struct {
    UringApiCompletionStateKind tag;
    struct sockaddr_storage addr;
    socklen_t addrlen;
    int fd;
} UringApiCompletionSockaddrState;

typedef struct {
    UringApiCompletionStateKind tag;
    Py_buffer view;
    bool has_view;
    struct sockaddr_storage addr;
    socklen_t addrlen;
    int fd;
    unsigned int flags;
} UringApiCompletionViewSockaddrState;

typedef struct {
    UringApiCompletionStateKind tag;
    Py_buffer view;
    bool has_view;
    struct iovec iov;
    struct msghdr msg;
    struct sockaddr_storage addr;
    socklen_t addrlen;
    int fd;
    unsigned int flags;
} UringApiCompletionMsgState;

typedef struct {
    UringApiCompletionStateKind tag;
    char *path;
    int dfd;
    int flags;
    unsigned int mode;
    bool constructed;
} UringApiCompletionPathState;

typedef struct {
    UringApiCompletionStateKind tag;
    char *path;
    Py_buffer view;
    bool has_view;
    int dfd;
    int flags;
    unsigned int mask;
    bool constructed;
} UringApiCompletionStatxState;

typedef struct {
    UringApiCompletionStateKind tag;
    unsigned char buf[256];
    int fd;
    bool constructed;
} UringApiCompletionStatxFdsizeState;

typedef struct {
    UringApiCompletionStateKind tag;
    int fd;
    unsigned int flags;
    unsigned int poll_mask;
    int how;
    int domain;
    int type;
    int protocol;
    bool constructed;
} UringApiCompletionScalarState;

int completion_type_check(PyObject *completion);
UringApiCompletionViewState *UringApiCompletion_get_view_state(UringApiCompletion *self);
UringApiCompletionBufGroupState *UringApiCompletion_get_buf_group_state(UringApiCompletion *self);
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
PyObject *UringApiCompletion_new_pending_scalar(UringApiPendingKind kind, PyObject *user_data);
UringApiCompletionScalarState *UringApiCompletion_get_scalar_state(UringApiCompletion *self);
int UringApiCompletion_set_nowait_flag(UringApiCompletion *self, int nowait);
PyObject *UringApiCompletion_new_pending_recvmsg(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view);
PyObject *UringApiCompletion_new_pending_sendmsg(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view);
PyObject *UringApiCompletion_new_multishot_delivered_shell(UringApiCompletion *source, unsigned long long leg_index);
void completion_prep_in_flight_ref(UringApiRing *ring, UringApiCompletion *completion, unsigned int flags);
bool completion_finish_in_flight_ref(UringApiRing *ring, UringApiCompletion *completion);
int UringApiCompletion_complete(UringApiCompletion *self, int res, unsigned int flags);
UringApiCompletionSockaddrState *UringApiCompletion_get_sockaddr_state(UringApiCompletion *self);
UringApiCompletionViewSockaddrState *UringApiCompletion_get_view_sockaddr_state(UringApiCompletion *self);
UringApiCompletionMsgState *UringApiCompletion_get_msg_state(UringApiCompletion *self);
UringApiCompletionPathState *UringApiCompletion_get_path_state(UringApiCompletion *self);
UringApiCompletionStatxState *UringApiCompletion_get_statx_state(UringApiCompletion *self);

#endif
