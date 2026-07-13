/*
 * Availability and capability reporting for the _uring_api extension.
 */

#include "uring_api_probe.h"
#include "uring_api_capi_impl.h"
#include "uring_api_core.h"
#include "uring_api_kernel_version.h"
#include "uring_api_kernel_versions.h"

/*
 * Process-lifetime cache for capability reporting. Capability floors are
 * derived from documented kernel versions via uname(2); they do not depend on
 * the caller's entries/flags. Default availability (entries=2, flags=0) is
 * cached separately because setup-flag probes still need a fresh queue_init
 * attempt each time.
 */
static int capability_cache_ready = 0;
static int capability_accept_multishot = 0;
static int capability_poll_multishot = 0;
static int capability_recv_multishot = 0;
static int capability_socket = 0;
static int capability_send_zc = 0;
static int capability_sendmsg_zc = 0;
static int capability_statx = 0;

static int default_availability_cached = 0;
static int default_availability = 0;

static PyObject *build_capability_dict(void);

static PyObject *build_probe_result(bool available) {
    PyObject *result;

    if (!available) {
        return PyDict_New();
    }

    result = build_capability_dict();
    if (!result) {
        return NULL;
    }
    if (PyDict_SetItemString(result, "available", Py_True) < 0) {
        Py_DECREF(result);
        return NULL;
    }
    return result;
}

static int probe_ring_availability(unsigned int entries, unsigned int flags) {
    struct io_uring ring;
    struct io_uring_params params;
    int ret;

    if (entries == 2 && flags == 0 && default_availability_cached) {
        return default_availability;
    }

    memset(&ring, 0, sizeof(ring));
    memset(&params, 0, sizeof(params));
    params.flags = flags;

    errno = 0;
    Py_BEGIN_ALLOW_THREADS;
    ret = io_uring_queue_init_params(entries, &ring, &params);
    Py_END_ALLOW_THREADS;

    if (ret < 0) {
        return 0;
    }

    io_uring_queue_exit(&ring);
    if (entries == 2 && flags == 0) {
        default_availability_cached = 1;
        default_availability = 1;
    }
    return 1;
}

static PyObject *uring_api_probe_impl(unsigned int entries, unsigned int flags) {
    if (entries == 0) {
        PyErr_SetString(PyExc_ValueError, "entries must be between 1 and UINT_MAX");
        return NULL;
    }

    return build_probe_result(probe_ring_availability(entries, flags) != 0);
}

PyObject *uring_api_probe(PyObject *self, PyObject *args, PyObject *kwargs) {
    unsigned int entries;
    unsigned int flags;

    if (parse_entries_flags(args, kwargs, 2, &entries, &flags) < 0) {
        return NULL;
    }
    return uring_api_probe_impl(entries, flags);
}

static int ensure_capability_cache(void) {
    if (capability_cache_ready) {
        return 0;
    }

    capability_poll_multishot = uring_api_kernel_version_at_least(URING_API_KERNEL_VERSION_POLL_MULTISHOT_MAJOR,
                                                                URING_API_KERNEL_VERSION_POLL_MULTISHOT_MINOR,
                                                                URING_API_KERNEL_VERSION_POLL_MULTISHOT_PATCH);
    capability_accept_multishot = uring_api_kernel_version_at_least(URING_API_KERNEL_VERSION_ACCEPT_MULTISHOT_MAJOR,
                                                                    URING_API_KERNEL_VERSION_ACCEPT_MULTISHOT_MINOR,
                                                                    URING_API_KERNEL_VERSION_ACCEPT_MULTISHOT_PATCH);
    capability_socket = uring_api_kernel_version_at_least(URING_API_KERNEL_VERSION_SOCKET_MAJOR,
                                                          URING_API_KERNEL_VERSION_SOCKET_MINOR,
                                                          URING_API_KERNEL_VERSION_SOCKET_PATCH);
    capability_recv_multishot = uring_api_kernel_version_at_least(URING_API_KERNEL_VERSION_RECV_MULTISHOT_MAJOR,
                                                                  URING_API_KERNEL_VERSION_RECV_MULTISHOT_MINOR,
                                                                  URING_API_KERNEL_VERSION_RECV_MULTISHOT_PATCH);
    capability_send_zc = uring_api_kernel_version_at_least(URING_API_KERNEL_VERSION_SEND_ZC_MAJOR,
                                                           URING_API_KERNEL_VERSION_SEND_ZC_MINOR,
                                                           URING_API_KERNEL_VERSION_SEND_ZC_PATCH);
    capability_sendmsg_zc = uring_api_kernel_version_at_least(URING_API_KERNEL_VERSION_SENDMSG_ZC_MAJOR,
                                                              URING_API_KERNEL_VERSION_SENDMSG_ZC_MINOR,
                                                              URING_API_KERNEL_VERSION_SENDMSG_ZC_PATCH);
    capability_statx = uring_api_kernel_version_at_least(URING_API_KERNEL_VERSION_STATX_MAJOR,
                                                         URING_API_KERNEL_VERSION_STATX_MINOR,
                                                         URING_API_KERNEL_VERSION_STATX_PATCH);
    capability_cache_ready = 1;
    return 0;
}

static int add_cached_bool(PyObject *capabilities, const char *name, int cached_value) {
    return PyDict_SetItemString(capabilities, name, cached_value ? Py_True : Py_False);
}

static PyObject *build_capability_dict(void) {
    PyObject *capabilities;

    if (ensure_capability_cache() < 0) {
        return NULL;
    }

    capabilities = PyDict_New();
    if (!capabilities) {
        return NULL;
    }

    if (add_cached_bool(capabilities, "IORING_ACCEPT_MULTISHOT", capability_accept_multishot) < 0 ||
        add_cached_bool(capabilities, "IORING_POLL_MULTISHOT", capability_poll_multishot) < 0 ||
        add_cached_bool(capabilities, "IORING_RECV_MULTISHOT", capability_recv_multishot) < 0 ||
        add_cached_bool(capabilities, "IORING_OP_SOCKET", capability_socket) < 0 ||
        add_cached_bool(capabilities, "IORING_OP_SEND_ZC", capability_send_zc) < 0 ||
        add_cached_bool(capabilities, "IORING_OP_SENDMSG_ZC", capability_sendmsg_zc) < 0 ||
        add_cached_bool(capabilities, "IORING_OP_STATX", capability_statx) < 0) {
        Py_DECREF(capabilities);
        return NULL;
    }
    return capabilities;
}

static PyObject *UringApiCapi_Probe(unsigned int entries, unsigned int flags) {
    return uring_api_probe_impl(entries, flags);
}

static const UringApi_CAPI uring_api_capi_table = {
    URING_API_CAPI_ABI_VERSION,
    sizeof(UringApi_CAPI),
    URING_API_CAPI_FEATURES,
    IO_URING_VERSION_MAJOR,
    IO_URING_VERSION_MINOR,
    UringApiCapi_Probe,
    UringApiCapi_RingNew,
    UringApiCapi_RingCheck,
    UringApiCapi_RingClose,
    UringApiCapi_RingFd,
    UringApiCapi_RingFeatures,
    UringApiCapi_RingSqEntries,
    UringApiCapi_RingCqEntries,
    UringApiCapi_RingClosed,
    UringApiCapi_RingRunning,
    UringApiCapi_RingSubmitRecv,
    UringApiCapi_RingSubmitRecvBuf,
    UringApiCapi_RingSubmitRecvMultishot,
    UringApiCapi_RingSubmitSend,
    UringApiCapi_RingSubmitSendZc,
    UringApiCapi_RingSubmitRecvmsg,
    UringApiCapi_RingSubmitSendto,
    UringApiCapi_RingSubmitSendmsg,
    UringApiCapi_RingSubmitSendmsgZc,
    UringApiCapi_RingSubmitAccept,
    UringApiCapi_RingSubmitAcceptMultishot,
    UringApiCapi_RingSubmitConnect,
    UringApiCapi_RingSubmitPoll,
    UringApiCapi_RingSubmitPollMultishot,
    UringApiCapi_RingSubmitPollRemove,
    UringApiCapi_RingSubmitCancel,
    UringApiCapi_RingSubmitShutdown,
    UringApiCapi_RingSubmitClose,
    UringApiCapi_RingSubmitRead,
    UringApiCapi_RingSubmitWrite,
    UringApiCapi_RingSubmitOpenat,
    UringApiCapi_RingSubmitStatx,
    UringApiCapi_RingSubmitStatxFdsize,
    UringApiCapi_StatxStSize,
    UringApiCapi_RingSubmitSocket,
    UringApiCapi_RingBreakWait,
    UringApiCapi_RingWait,
    UringApiCapi_RingSetCallback,
    UringApiCapi_RingSetExceptionHandler,
    UringApiCapi_RingSetCCallback,
    UringApiCapi_RingServeCompletions,
    UringApiCapi_RingStopServing,
    UringApiCapi_RingResetServing,
    UringApiCapi_CompletionCheck,
    UringApiCapi_CompletionUserData,
    UringApiCapi_CompletionRes,
    UringApiCapi_CompletionFlags,
    UringApiCapi_CompletionSequence,
    UringApiCapi_CompletionResult,
    UringApiCapi_CompletionKind,
};

int uring_api_export_capi(PyObject *module) {
    PyObject *capsule;

    capsule = PyCapsule_New((void *)&uring_api_capi_table, URING_API_CAPI_CAPSULE_NAME, NULL);
    if (!capsule) {
        return -1;
    }
    if (PyModule_AddObject(module, "_C_API", capsule) < 0) {
        Py_DECREF(capsule);
        return -1;
    }
    if (PyModule_AddIntConstant(module, "C_API_ABI_VERSION", (long)URING_API_CAPI_ABI_VERSION) < 0 ||
        PyModule_AddIntConstant(module, "C_API_STRUCT_SIZE", (long)sizeof(UringApi_CAPI)) < 0) {
        return -1;
    }
    if (module_add_uint64_constant(module, "C_API_FEATURE_CORE", URING_API_CAPI_FEATURE_CORE) < 0) {
        return -1;
    }
    if (module_add_uint64_constant(module, "C_API_FEATURES", URING_API_CAPI_FEATURES) < 0) {
        return -1;
    }
    return 0;
}