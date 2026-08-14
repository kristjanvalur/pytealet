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
 *   - ring_submit_accept_multishot / ring_submit_recv_multishot take base_sequence
 *   - ring_set_pre_submit / ring_set_c_pre_submit removed from the middle of
 *     UringApi_CAPI (between completion_kind and the nowait slots); every later
 *     function pointer offset (nowait submits, ring_submit, …) moved — old
 *     binaries that cached offsetof without rebuild call the wrong slots
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

    /* Submission operations (ordered like Ring.submit_*). */
    int (*ring_submit_recv)(PyObject *ring, int fd, PyObject *buf, PyObject *user_data);
    /*
     * Provided-buffer submits take a Python BufGroup*. The C API does not yet
     * expose BufGroup lifecycle (create, close, release_callback / C release
     * hook, leased_count, …). Create and manage groups from Python, or pass a
     * borrowed BufGroup object. TODO: append ring_create_buf_group,
     * buf_group_close, and optional C release callback when pure-C owners need
     * them (see ROADMAP.md).
     */
    int (*ring_submit_recv_buf)(PyObject *ring, int fd, PyObject *buf_group, unsigned int flags, PyObject *user_data);
    int (*ring_submit_recv_multishot)(PyObject *ring, int fd, PyObject *buf_group, unsigned int flags,
                                      PyObject *user_data, unsigned long long base_sequence);
    int (*ring_submit_send)(PyObject *ring, int fd, PyObject *data, unsigned int flags, PyObject *user_data);
    int (*ring_submit_send_zc)(PyObject *ring, int fd, PyObject *data, unsigned int flags, unsigned int zc_flags,
                               PyObject *user_data);
    int (*ring_submit_recvmsg)(PyObject *ring, int fd, PyObject *buf, PyObject *user_data);
    int (*ring_submit_sendto)(PyObject *ring, int fd, PyObject *data, PyObject *address, unsigned int flags,
                              PyObject *user_data);
    int (*ring_submit_sendmsg)(PyObject *ring, int fd, PyObject *data, PyObject *address, unsigned int flags,
                               PyObject *user_data);
    int (*ring_submit_sendmsg_zc)(PyObject *ring, int fd, PyObject *data, PyObject *address, unsigned int flags,
                                  PyObject *user_data);
    int (*ring_submit_accept)(PyObject *ring, int fd, unsigned int flags, PyObject *user_data);
    int (*ring_submit_accept_multishot)(PyObject *ring, int fd, unsigned int flags, PyObject *user_data,
                                        unsigned long long base_sequence);
    int (*ring_submit_connect)(PyObject *ring, int fd, PyObject *address, PyObject *user_data);
    int (*ring_submit_poll)(PyObject *ring, int fd, unsigned int mask, PyObject *user_data);
    int (*ring_submit_poll_multishot)(PyObject *ring, int fd, unsigned int mask, PyObject *user_data);
    int (*ring_submit_poll_remove)(PyObject *ring, PyObject *target_completion);
    int (*ring_submit_cancel)(PyObject *ring, PyObject *target_completion);
    int (*ring_submit_shutdown)(PyObject *ring, int fd, int how, PyObject *user_data);
    int (*ring_submit_close)(PyObject *ring, int fd, PyObject *user_data);
    int (*ring_submit_read)(PyObject *ring, int fd, PyObject *buf, unsigned long long offset, PyObject *user_data);
    int (*ring_submit_write)(PyObject *ring, int fd, PyObject *data, unsigned long long offset, PyObject *user_data);
    int (*ring_submit_openat)(PyObject *ring, int dfd, PyObject *path, int flags, unsigned int mode,
                              PyObject *user_data);
    int (*ring_submit_statx)(PyObject *ring, int dfd, PyObject *path, int flags, unsigned int mask, PyObject *buf,
                             PyObject *user_data);
    int (*ring_submit_statx_fdsize)(PyObject *ring, int fd, PyObject *user_data);
    int (*statx_st_size)(PyObject *buf, unsigned long long *value);
    int (*ring_submit_socket)(PyObject *ring, int domain, int type, int protocol, unsigned int flags,
                              PyObject *user_data);
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

    /*
     * Nowait prepares (appended; check struct_size / null pointers).
     * No Completion, no client delivery. Return 0 after the SQE is prepared.
     * All nowait paths (including cancel/poll_remove) stay lazy until
     * ring_submit / wait entry flush / SQ-full get_sqe / post-delivery flush.
     * Cancel/remove enqueued after a still-prepared target publish in order on
     * the next flush; no special pre/post flush.
     */
    int (*ring_submit_close_nowait)(PyObject *ring, int fd);
    int (*ring_submit_shutdown_nowait)(PyObject *ring, int fd, int how);
    int (*ring_submit_cancel_nowait)(PyObject *ring, PyObject *target_completion);
    int (*ring_submit_poll_remove_nowait)(PyObject *ring, PyObject *target_completion);

    /* Nowait failure hook (appended; check struct_size / null pointer). Same callable as Ring.nowait_error_handler. */
    int (*ring_set_nowait_error_handler)(PyObject *ring, PyObject *handler);

    /*
     * Flush prepared SQEs to the kernel (appended; check struct_size / null pointer).
     * Same as Ring.submit(). On success stores the number submitted in *submitted
     * (may be 0) and returns 0; on error returns -1 with a Python exception.
     * submit_* / nowait only prepare SQEs; work becomes kernel-visible after this,
     * wait entry flush (when this thread may submit), serve/wait post-delivery
     * flush, or SQ-full get_sqe.
     */
    int (*ring_submit)(PyObject *ring, int *submitted);

    /*
     * Set Completion.user_data (appended; check struct_size / null pointer).
     * value may be NULL for None. Breaks op↔completion cycles after delivery.
     * Does not change the kernel SQE user_data tag (Completion pointer).
     */
    int (*completion_set_user_data)(PyObject *completion, PyObject *value);

    /*
     * Construct a send Completion without reserving an SQE (appended; check
     * struct_size / null pointers). Same arguments as ring_submit_send.
     * Returns a new Completion, or NULL with an exception. Arm reverse links
     * on this object, then ring_prepare() to fill SQEs. Dropping it without
     * prepare just releases the retained buffer.
     */
    PyObject *(*ring_construct_send)(PyObject *ring, int fd, PyObject *data, unsigned int flags, PyObject *user_data);
    /*
     * Reserve and fill SQEs for constructed send Completions (appended).
     * completions is a Completion or a sequence of Completions. On success
     * stores the number prepared in *prepared (may be 0) and returns 0.
     * On error returns -1 with an exception; the prefix may already be
     * prepared (and may have been flushed if get_sqe hit SQ full).
     */
    int (*ring_prepare)(PyObject *ring, PyObject *completions, int *prepared);
    /* 1 if an SQE has been filled for this completion, else 0. */
    int (*completion_prepared)(PyObject *completion, int *value);
    PyObject *(*ring_construct_send_zc)(PyObject *ring, int fd, PyObject *data, unsigned int flags,
                                        unsigned int zc_flags, PyObject *user_data);
    /*
     * Construct recv/read/write Completions without reserving an SQE
     * (appended; check struct_size / null pointers). Same arguments as the
     * matching ring_submit_* slots. Returns a new Completion, or NULL with
     * an exception.
     */
    PyObject *(*ring_construct_recv)(PyObject *ring, int fd, PyObject *buf, PyObject *user_data);
    PyObject *(*ring_construct_read)(PyObject *ring, int fd, PyObject *buf, unsigned long long offset,
                                     PyObject *user_data);
    PyObject *(*ring_construct_write)(PyObject *ring, int fd, PyObject *data, unsigned long long offset,
                                      PyObject *user_data);
} UringApi_CAPI;

/* Import helper for clients. Returns NULL and sets exception on failure. */
static inline const UringApi_CAPI *UringApi_Import(void) {
    return (const UringApi_CAPI *)PyCapsule_Import(URING_API_CAPI_CAPSULE_NAME, 0);
}

#endif
