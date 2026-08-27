/*
 * Opt-in SQ prepare/submit tracing for uring-api.
 */

#include "uring_api_sq_log.h"
#include "uring_api_completion.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/syscall.h>
#include <unistd.h>

static int sq_log_enabled;

static long current_tid(void) { return (long)syscall(SYS_gettid); }

static const char *kind_name(unsigned int kind) {
    switch (kind) {
    case URING_API_PENDING_RECV:
        return "recv";
    case URING_API_PENDING_SEND:
        return "send";
    case URING_API_PENDING_WAKE:
        return "wake";
    case URING_API_PENDING_SENDTO:
        return "sendto";
    case URING_API_PENDING_RECVMSG:
        return "recvmsg";
    case URING_API_PENDING_ACCEPT:
        return "accept";
    case URING_API_PENDING_CONNECT:
        return "connect";
    case URING_API_PENDING_CANCEL:
        return "cancel";
    case URING_API_PENDING_SHUTDOWN:
        return "shutdown";
    case URING_API_PENDING_CLOSE:
        return "close";
    case URING_API_PENDING_SENDMSG:
        return "sendmsg";
    case URING_API_PENDING_SOCKET:
        return "socket";
    case URING_API_PENDING_RECV_MULTISHOT:
        return "recv_multishot";
    case URING_API_PENDING_SEND_ZC:
        return "send_zc";
    case URING_API_PENDING_SENDMSG_ZC:
        return "sendmsg_zc";
    case URING_API_PENDING_RECV_BUF:
        return "recv_buf";
    case URING_API_PENDING_POLL:
        return "poll";
    case URING_API_PENDING_POLL_MULTISHOT:
        return "poll_multishot";
    case URING_API_PENDING_POLL_REMOVE:
        return "poll_remove";
    case URING_API_PENDING_READ:
        return "read";
    case URING_API_PENDING_WRITE:
        return "write";
    case URING_API_PENDING_OPENAT:
        return "openat";
    case URING_API_PENDING_STATX:
        return "statx";
    case URING_API_PENDING_STATX_FDSIZE:
        return "statx_fdsize";
    case URING_API_PENDING_SEND_ALL:
        return "send_all";
    default:
        return "?";
    }
}

static int completion_fd(UringApiCompletion *completion) {
    UringApiCompletionViewState *view_state;
    UringApiCompletionBufGroupState *buf_group_state;
    UringApiCompletionScalarState *scalar_state;
    UringApiCompletionSockaddrState *sockaddr_state;
    UringApiCompletionViewSockaddrState *view_sockaddr_state;
    UringApiCompletionMsgState *msg_state;

    switch (completion->kind) {
    case URING_API_PENDING_RECV:
    case URING_API_PENDING_SEND:
    case URING_API_PENDING_SEND_ALL:
    case URING_API_PENDING_SEND_ZC:
    case URING_API_PENDING_READ:
    case URING_API_PENDING_WRITE:
        view_state = UringApiCompletion_get_view_state(completion);
        return view_state != NULL ? view_state->fd : -1;
    case URING_API_PENDING_RECV_MULTISHOT:
    case URING_API_PENDING_RECV_BUF:
        buf_group_state = UringApiCompletion_get_buf_group_state(completion);
        return buf_group_state != NULL ? buf_group_state->fd : -1;
    case URING_API_PENDING_ACCEPT:
    case URING_API_PENDING_CLOSE:
    case URING_API_PENDING_SHUTDOWN:
    case URING_API_PENDING_POLL:
    case URING_API_PENDING_POLL_MULTISHOT:
    case URING_API_PENDING_SOCKET:
        scalar_state = UringApiCompletion_get_scalar_state(completion);
        return scalar_state != NULL ? scalar_state->fd : -1;
    case URING_API_PENDING_CONNECT:
        sockaddr_state = UringApiCompletion_get_sockaddr_state(completion);
        return sockaddr_state != NULL ? sockaddr_state->fd : -1;
    case URING_API_PENDING_SENDTO:
        view_sockaddr_state = UringApiCompletion_get_view_sockaddr_state(completion);
        return view_sockaddr_state != NULL ? view_sockaddr_state->fd : -1;
    case URING_API_PENDING_RECVMSG:
    case URING_API_PENDING_SENDMSG:
    case URING_API_PENDING_SENDMSG_ZC:
        msg_state = UringApiCompletion_get_msg_state(completion);
        return msg_state != NULL ? msg_state->fd : -1;
    default:
        return -1;
    }
}

void uring_api_sq_log_init(void) {
    const char *raw = getenv("URING_API_SQ_LOG");

    sq_log_enabled = 0;
    if (raw == NULL || raw[0] == '\0') {
        return;
    }
    if (strcmp(raw, "1") == 0 || strcasecmp(raw, "true") == 0 || strcasecmp(raw, "yes") == 0) {
        sq_log_enabled = 1;
    }
}

int uring_api_sq_log_enabled(void) { return sq_log_enabled; }

void uring_api_sq_log_prepare(UringApiCompletion *completion, const char *how) {
    if (!sq_log_enabled) {
        return;
    }
    fprintf(stderr, "uring-sq tid=%ld prepare how=%s kind=%s fd=%d c=%p\n", current_tid(), how != NULL ? how : "sqe",
            kind_name((unsigned int)completion->kind), completion_fd(completion), (void *)completion);
    fflush(stderr);
}

void uring_api_sq_log_prepare_nowait(unsigned int kind, int fd) {
    if (!sq_log_enabled) {
        return;
    }
    fprintf(stderr, "uring-sq tid=%ld prepare how=nowait kind=%s fd=%d c=nowait\n", current_tid(), kind_name(kind), fd);
    fflush(stderr);
}

void uring_api_sq_log_prepare_nop(void) {
    if (!sq_log_enabled) {
        return;
    }
    fprintf(stderr, "uring-sq tid=%ld prepare how=sqe kind=nop fd=-1 c=wake\n", current_tid());
    fflush(stderr);
}

void uring_api_sq_log_park(UringApiCompletion *completion, const char *queue) {
    if (!sq_log_enabled) {
        return;
    }
    fprintf(stderr, "uring-sq tid=%ld park queue=%s kind=%s fd=%d c=%p\n", current_tid(), queue != NULL ? queue : "?",
            kind_name((unsigned int)completion->kind), completion_fd(completion), (void *)completion);
    fflush(stderr);
}

void uring_api_sq_log_submit(unsigned int sq_ready, int submitted) {
    if (!sq_log_enabled) {
        return;
    }
    fprintf(stderr, "uring-sq tid=%ld submit sq_ready=%u n=%d\n", current_tid(), sq_ready, submitted);
    fflush(stderr);
}
