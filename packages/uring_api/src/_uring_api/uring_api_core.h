#ifndef URING_API_CORE_H
#define URING_API_CORE_H

/* private implementation header; not part of the public C API. */

#include "uring_api_common.h"

/*
 * SQE/CQE user_data tagging (io_uring user_data is always u64).
 *
 * PyObject* / Completion* values are at least 4-byte aligned, so bits 1:0 are 0.
 * Bit 0 set ⇒ special (not a Completion*). Bit 1 selects the special class:
 *
 *   bits 1:0 == 00  → Completion* (waitable path)
 *   bits 1:0 == 01  → wake NOP (break_wait / neutralize / stop_serving)
 *   bits 1:0 == 10  → reserved
 *   bits 1:0 == 11  → nowait (no Completion; optional CQE_SKIP_SUCCESS)
 *
 * Nowait payload (bits 63:2):
 *   bits 7:2   kind  — COMPLETION_KIND_* (fits in 6 bits today)
 *   bits 39:8  fd    — advisory uint32 (masked; may roll over; no-fd = all ones)
 *   bits 63:40 reserved (0)
 */
#define URING_API_UD_TAG_MASK 0x3ull
#define URING_API_UD_TAG_COMPLETION 0x0ull
#define URING_API_UD_TAG_WAKE 0x1ull
#define URING_API_UD_TAG_RESERVED 0x2ull
#define URING_API_UD_TAG_NOWAIT 0x3ull

#define URING_API_WAKE_USER_DATA URING_API_UD_TAG_WAKE

#define URING_API_NOWAIT_KIND_SHIFT 2
#define URING_API_NOWAIT_KIND_MASK 0x3full
#define URING_API_NOWAIT_FD_SHIFT 8
#define URING_API_NOWAIT_FD_BITS 32
#define URING_API_NOWAIT_FD_MASK 0xffffffffull
/* advisory: no associated fd (cancel / poll_remove acks) */
#define URING_API_NOWAIT_FD_NONE ((unsigned int)0xffffffffu)

/* any non-zero low tag (including reserved 10) is not a Completion* */
static inline int uring_api_ud_is_special(unsigned long long user_data) {
    return (user_data & URING_API_UD_TAG_MASK) != URING_API_UD_TAG_COMPLETION;
}

static inline int uring_api_ud_is_wake(unsigned long long user_data) {
    return (user_data & URING_API_UD_TAG_MASK) == URING_API_UD_TAG_WAKE;
}

static inline int uring_api_ud_is_nowait(unsigned long long user_data) {
    return (user_data & URING_API_UD_TAG_MASK) == URING_API_UD_TAG_NOWAIT;
}

/* kind is COMPLETION_KIND_*; fd is advisory (masked to NOWAIT_FD_BITS). */
static inline unsigned long long uring_api_make_nowait_user_data(unsigned int kind, int fd) {
    unsigned long long k = (unsigned long long)(kind & (unsigned int)URING_API_NOWAIT_KIND_MASK);
    unsigned long long f;

    if (fd < 0) {
        f = URING_API_NOWAIT_FD_NONE;
    } else {
        /* advisory: high bits may be lost if fd is huge */
        f = ((unsigned long long)(unsigned int)fd) & URING_API_NOWAIT_FD_MASK;
    }
    return URING_API_UD_TAG_NOWAIT | (k << URING_API_NOWAIT_KIND_SHIFT) | (f << URING_API_NOWAIT_FD_SHIFT);
}

static inline unsigned int uring_api_nowait_kind(unsigned long long user_data) {
    return (unsigned int)((user_data >> URING_API_NOWAIT_KIND_SHIFT) & URING_API_NOWAIT_KIND_MASK);
}

/* returns 1 and sets *fd_out when an fd was stored; 0 when FD_NONE / absent */
static inline int uring_api_nowait_fd(unsigned long long user_data, int *fd_out) {
    unsigned int raw = (unsigned int)((user_data >> URING_API_NOWAIT_FD_SHIFT) & URING_API_NOWAIT_FD_MASK);

    if (raw == URING_API_NOWAIT_FD_NONE) {
        return 0;
    }
    *fd_out = (int)raw;
    return 1;
}

int ring_type_check(PyObject *ring);
int normalize_ret_errno(int ret);
PyObject *liburing_version_string(void);
PyObject *liburing_version_info(void);
int module_add_uint64_constant(PyObject *module, const char *name, unsigned long long value);
int module_add_setup_flag_constants(PyObject *module);
int module_add_cqe_flag_constants(PyObject *module);
int module_add_recvsend_flag_constants(PyObject *module);
int module_add_completion_kind_constants(PyObject *module);
int module_add_statx_constants(PyObject *module);
void sqe_set_completion(UringApiRing *self, struct io_uring_sqe *sqe, PyObject *completion);
UringApiCompletion *cqe_get_completion(UringApiRing *self, struct io_uring_cqe *cqe);
/* rewrite a reserved SQE as a wake NOP so a later submit cannot run abandoned work */
void neutralize_prepared_sqe(struct io_uring_sqe *sqe);
unsigned int ring_sq_entries(UringApiRing *self);
unsigned int ring_cq_entries(UringApiRing *self);

int parse_entries_flags(PyObject *args, PyObject *kwargs, unsigned int default_entries, unsigned int *entries,
                        unsigned int *flags);
int parse_numeric_sockaddr(int fd, PyObject *address, struct sockaddr_storage *storage, socklen_t *addrlen);
int ring_check_open(UringApiRing *self);
/*
 * Check issuer-thread rules (SINGLE_ISSUER / DEFER_TASKRUN). Returns 0 if this
 * thread may submit, -1 if not. When raise_on_error is non-zero, sets
 * RuntimeError on failure; otherwise fails quietly (no exception).
 */
int ring_check_submit_thread(UringApiRing *self, int raise_on_error);
int ring_check_client_thread(UringApiRing *self);
/* Flush pending SQEs. Allows zero submitted. Returns 0 or -1 with exception.
 * When submitted_out is non-NULL, stores the io_uring_submit return count. */
int ring_flush_pending(UringApiRing *self, int *submitted_out);
/* Flush and require at least one SQE (e.g. after preparing a wake NOP). */
int submit_one(UringApiRing *self);
/*
 * SQE is already prepared with completion as user_data. Does not flush to the
 * kernel (lazy submit). Caller holds the ring critical section.
 */
int submit_one_completion(UringApiRing *self, struct io_uring_sqe *sqe, PyObject *completion);
int receive_wait_begin(UringApiRing *self, bool from_delivery_thread);
void receive_wait_end(UringApiRing *self, bool from_delivery_thread);
bool delivery_is_running_locked(UringApiRing *self);
int delivery_check_not_running(UringApiRing *self);
void delivery_mark_exited(UringApiRing *self);
struct io_uring_sqe *get_sqe(UringApiRing *self);

#endif
