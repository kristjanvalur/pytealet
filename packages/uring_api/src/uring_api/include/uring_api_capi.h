/* uring_api_capi.h - public C API declarations for the _uring_api extension.
 *
 * Client extensions should import this API via PyCapsule_Import() using the
 * capsule name below, then call function pointers from the returned table.
 */

#ifndef URING_API_CAPI_H
#define URING_API_CAPI_H

#include <Python.h>

#include <stdint.h>

#define URING_API_CAPI_ABI_VERSION 1u
#define URING_API_CAPI_CAPSULE_NAME "_uring_api._C_API"

/* Feature flags published in UringApi_CAPI.feature_flags. */
#define URING_API_CAPI_FEATURE_CORE (1ull << 0)

typedef int (*UringApi_CCompletionCallback)(PyObject *ring, PyObject *completion, void *user_data);

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

    /* Submission and receive operations. */
    int (*ring_submit_recv)(PyObject *ring, int fd, PyObject *buf, PyObject *user_data);
    int (*ring_submit_recv_multishot)(PyObject *ring, int fd, unsigned int buffer_size, unsigned int buffer_count,
                                      unsigned int flags, PyObject *user_data);
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
    int (*ring_submit_accept_multishot)(PyObject *ring, int fd, unsigned int flags, PyObject *user_data);
    int (*ring_submit_connect)(PyObject *ring, int fd, PyObject *address, PyObject *user_data);
    int (*ring_submit_shutdown)(PyObject *ring, int fd, int how, PyObject *user_data);
    int (*ring_submit_close)(PyObject *ring, int fd, PyObject *user_data);
    int (*ring_submit_socket)(PyObject *ring, int domain, int type, int protocol, unsigned int flags,
                              PyObject *user_data);
    int (*ring_break_wait)(PyObject *ring);
    /*
     * Wait for one completion and return a new reference, or Py_None on timeout/no completion.
     * timeout < 0 blocks indefinitely, timeout == 0 performs a non-blocking peek,
     * and timeout > 0 waits for at most that many seconds.
     */
    PyObject *(*ring_wait)(PyObject *ring, double timeout);

    /* Completion service control. C callback is preferred over Python callback when both are set. */
    int (*ring_set_callback)(PyObject *ring, PyObject *callback);
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

    void *reserved[8];
} UringApi_CAPI;

/* Import helper for clients. Returns NULL and sets exception on failure. */
static inline const UringApi_CAPI *UringApi_Import(void) {
    return (const UringApi_CAPI *)PyCapsule_Import(URING_API_CAPI_CAPSULE_NAME, 0);
}

#endif