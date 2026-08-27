/* uring_api_capi.h - public C API declarations for the _uring_api extension.
 *
 * Client extensions should import this API via PyCapsule_Import() using the
 * capsule name below, then call function pointers from the returned table.
 */

#ifndef URING_API_CAPI_H
#define URING_API_CAPI_H

#include <Python.h>

#include <stdint.h>

#include "uring_api_completion_kinds.h"

/*
 * Pre-release: ABI version stays 1 while the package is unreleased (see
 * packages/uring_api/AGENTS.md). Vtable *signatures* may still change — rebuild
 * every C client after pulling. Notable breaks vs early v1 drafts:
 *   - ring_submit_* / ring_submit_*_nowait removed; C clients construct then
 *     ring_prepare(). Python Ring.prepare_* is construct+prepare sugar.
 *   - ring_set_pre_submit / ring_set_c_pre_submit removed
 *   - ring_construct_recv_multishot / accept_multishot no longer take
 *     base_sequence; set completion.sequence after construct
 *   - ring_construct_recv / recvmsg take flags (POLL_FIRST and friends)
 *   - Python prepare and construct methods: cargo then user_data last
 *     (aligns with C)
 * Clients must check abi_version, struct_size, and null-check pointers they use.
 */
#define URING_API_CAPI_ABI_VERSION 1u
#define URING_API_CAPI_CAPSULE_NAME "_uring_api._C_API"

/* Feature flags published in UringApi_CAPI.feature_flags. */
#define URING_API_CAPI_FEATURE_CORE (1ull << 0)

/*
 * Completion delivery callback invoked from serve_completions() worker threads.
 * completions is a list of Completion objects for one kernel drain batch.
 * user_data is the pointer supplied to ring_set_c_callback(). Return 0 on
 * success; set a Python exception and return -1 so the current serving worker
 * exits with that error.
 *
 * ring_set_callback() and ring_set_c_callback() must not be called while
 * serve_completions() workers are active. ring_set_exception_handler() may be
 * called at any time; delivery threads read the current handler under the ring
 * critical section when reporting callback failures.
 */
typedef int (*UringApi_CCompletionCallback)(PyObject *ring, PyObject *completions, void *user_data);

typedef struct UringApi_CAPI {
    uint32_t abi_version;
    uint32_t struct_size;
    uint64_t feature_flags;
    uint32_t compiled_liburing_major;
    uint32_t compiled_liburing_minor;

    /* Return a new dict matching _uring_api.probe(entries, flags), including capabilities. */
    PyObject *(*probe)(unsigned int entries, unsigned int flags);

    /* Ring lifecycle. Return new references where PyObject * is returned. */
    PyObject *(*ring_new)(unsigned int entries, unsigned int flags);
    int (*ring_check)(PyObject *ring);
    int (*ring_close)(PyObject *ring);

    /* Ring metadata. */
    int (*ring_fd)(PyObject *ring);
    unsigned int (*ring_features)(PyObject *ring);
    unsigned int (*ring_sq_entries)(PyObject *ring);
    unsigned int (*ring_cq_entries)(PyObject *ring);
    int (*ring_closed)(PyObject *ring);
    int (*ring_running)(PyObject *ring);

    /*
     * Construct Completions without reserving an SQE. Returns a new Completion,
     * or NULL with an exception. Arm reverse links, then ring_prepare() to fill
     * SQEs. Dropping an unprepared handle just releases cargo.
     * Provided-buffer construct takes a Python BufGroup*; create groups from
     * Python until BufGroup lifecycle is on the capsule (see ROADMAP.md).
     */
    PyObject *(*ring_construct_recv)(PyObject *ring, int fd, PyObject *buf, unsigned int flags, PyObject *user_data);
    PyObject *(*ring_construct_recv_buf)(PyObject *ring, int fd, PyObject *buf_group, unsigned int flags,
                                         PyObject *user_data);
    PyObject *(*ring_construct_recv_multishot)(PyObject *ring, int fd, PyObject *buf_group, unsigned int flags,
                                               PyObject *user_data);
    PyObject *(*ring_construct_send)(PyObject *ring, int fd, PyObject *data, unsigned int flags, PyObject *user_data);
    PyObject *(*ring_construct_send_zc)(PyObject *ring, int fd, PyObject *data, unsigned int flags,
                                        unsigned int zc_flags, PyObject *user_data);
    PyObject *(*ring_construct_recvmsg)(PyObject *ring, int fd, PyObject *buf, unsigned int flags, PyObject *user_data);
    PyObject *(*ring_construct_sendto)(PyObject *ring, int fd, PyObject *data, PyObject *address, unsigned int flags,
                                       PyObject *user_data);
    PyObject *(*ring_construct_sendmsg)(PyObject *ring, int fd, PyObject *data, PyObject *address, unsigned int flags,
                                        PyObject *user_data);
    PyObject *(*ring_construct_sendmsg_zc)(PyObject *ring, int fd, PyObject *data, PyObject *address,
                                           unsigned int flags, PyObject *user_data);
    PyObject *(*ring_construct_accept)(PyObject *ring, int fd, unsigned int flags, PyObject *user_data);
    PyObject *(*ring_construct_accept_multishot)(PyObject *ring, int fd, unsigned int flags, PyObject *user_data);
    PyObject *(*ring_construct_connect)(PyObject *ring, int fd, PyObject *address, PyObject *user_data);
    PyObject *(*ring_construct_poll)(PyObject *ring, int fd, unsigned int mask, PyObject *user_data);
    PyObject *(*ring_construct_poll_multishot)(PyObject *ring, int fd, unsigned int mask, PyObject *user_data);
    PyObject *(*ring_construct_poll_remove)(PyObject *ring, PyObject *target_completion, PyObject *user_data);
    PyObject *(*ring_construct_cancel)(PyObject *ring, PyObject *target_completion, PyObject *user_data);
    PyObject *(*ring_construct_shutdown)(PyObject *ring, int fd, int how, PyObject *user_data);
    PyObject *(*ring_construct_close)(PyObject *ring, int fd, PyObject *user_data);
    PyObject *(*ring_construct_read)(PyObject *ring, int fd, PyObject *buf, unsigned long long offset,
                                     PyObject *user_data);
    PyObject *(*ring_construct_write)(PyObject *ring, int fd, PyObject *data, unsigned long long offset,
                                      PyObject *user_data);
    PyObject *(*ring_construct_openat)(PyObject *ring, int dfd, PyObject *path, int flags, unsigned int mode,
                                       PyObject *user_data);
    PyObject *(*ring_construct_statx)(PyObject *ring, int dfd, PyObject *path, int flags, unsigned int mask,
                                      PyObject *buf, PyObject *user_data);
    PyObject *(*ring_construct_statx_fdsize)(PyObject *ring, int fd, PyObject *user_data);
    int (*statx_st_size)(PyObject *buf, unsigned long long *value);
    PyObject *(*ring_construct_socket)(PyObject *ring, int domain, int type, int protocol, unsigned int flags,
                                       PyObject *user_data);
    /*
     * Accept constructed Completions: fill SQEs, park on a per-fd conflict FIFO
     * when that fd is send-all-busy (send/close/shutdown/further send-all), or
     * park on the ring-wide fill-wait list when a non-issuer would have to enter.
     * completions is a Completion or a sequence. On success stores the number
     * accepted in *prepared (SQE fills and parks) and returns 0. On error
     * returns -1; the prefix may already be accepted (and may have been flushed).
     * Nowait: set completion_set_nowait first; prepare stamps a tagged SQE
     * (nowait send_all keeps the Completion* until the drain terminals).
     */
    int (*ring_prepare)(PyObject *ring, PyObject *completions, int *prepared);
    /* 1 after an SQE is filled. Conflict-FIFO and fill-wait parks stay 0 until drain copies them. */
    int (*completion_prepared)(PyObject *completion, int *value);
    int (*completion_nowait)(PyObject *completion, int *value);
    int (*completion_set_nowait)(PyObject *completion, int value);

    int (*ring_break_wait)(PyObject *ring);
    /*
     * Wait for ready completions.
     * With no delivery callback: returns a new list reference (empty on timeout
     * or break_wait). With a Python or C delivery callback: delivers non-empty
     * user batches via the callback and returns None; empty batches skip the
     * callback and still return None.
     * The first wait uses the requested timeout; once one completion is ready,
     * additional CQEs are drained with zero wait before return/delivery.
     * timeout < 0 blocks indefinitely, timeout == 0 performs a non-blocking peek,
     * and timeout > 0 waits for at most that many seconds.
     * When auto_submit is on, flushes prepared SQEs first (if this thread may
     * submit). When off, only already-submitted work is visible.
     */
    PyObject *(*ring_wait)(PyObject *ring, double timeout);

    /* Completion service control. C callback is preferred over Python callback when both are set. */
    int (*ring_set_callback)(PyObject *ring, PyObject *callback);
    int (*ring_set_exception_handler)(PyObject *ring, PyObject *handler);
    int (*ring_set_c_callback)(PyObject *ring, UringApi_CCompletionCallback callback, void *user_data);
    int (*ring_serve_completions)(PyObject *ring);
    int (*ring_stop_serving)(PyObject *ring);
    int (*ring_reset_serving)(PyObject *ring);

    /* Completion helpers. Return borrowed scalars via output pointers and new references for PyObject *. */
    int (*completion_check)(PyObject *completion);
    PyObject *(*completion_user_data)(PyObject *completion);
    int (*completion_res)(PyObject *completion, int *value);
    int (*completion_flags)(PyObject *completion, unsigned int *value);
    int (*completion_sequence)(PyObject *completion, unsigned long long *value);
    PyObject *(*completion_result)(PyObject *completion);
    int (*completion_kind)(PyObject *completion, int *value);
    int (*completion_set_user_data)(PyObject *completion, PyObject *value);

    /* Same callable as Ring.nowait_error_handler. */
    int (*ring_set_nowait_error_handler)(PyObject *ring, PyObject *handler);

    /*
     * Flush prepared SQEs to the kernel. Same as Ring.submit(). On success
     * stores the number submitted in *submitted (may be 0) and returns 0.
     */
    int (*ring_submit)(PyObject *ring, int *submitted);

    /* Default true. When false, the issuer's get_sqe raises SubmissionQueueFull
     * instead of flushing, wait/serve do not auto-submit, and a non-issuer
     * prepare that would have to enter parks on fill-wait. */
    int (*ring_auto_submit)(PyObject *ring, int *value);
    int (*ring_set_auto_submit)(PyObject *ring, int value);

    /* Waitable Completions still in flight (same as Ring.pending_count()).
     * Includes waitable conflict-FIFO / fill-wait parks and nowait send_all
     * until terminal; ordinary nowait is excluded. */
    int (*ring_pending_count)(PyObject *ring, unsigned int *value);

    /* Seed completion.sequence (first multishot leg). Same as Completion.sequence = n. */
    int (*completion_set_sequence)(PyObject *completion, unsigned long long value);
    /* Deferred-clear user_data (same as Completion.clear_user_data()). */
    int (*completion_clear_user_data)(PyObject *completion);
    /* Park until break_wait/close. timeout < 0 blocks, 0 polls, > 0 is seconds.
     * Stores 1 if signalled, 0 on timeout. */
    int (*ring_wait_idle)(PyObject *ring, double timeout, int *signaled);
    /* Synthetic drain: kernel sees IORING_OP_SEND legs. Later prepare of
     * send/close/shutdown/send_all on the same fd parks on the conflict FIFO. */
    PyObject *(*ring_construct_send_all)(PyObject *ring, int fd, PyObject *data, unsigned int flags,
                                         PyObject *user_data);
} UringApi_CAPI;

/* Import helper for clients. Returns NULL and sets exception on failure. */
static inline const UringApi_CAPI *UringApi_Import(void) {
    return (const UringApi_CAPI *)PyCapsule_Import(URING_API_CAPI_CAPSULE_NAME, 0);
}

#endif
