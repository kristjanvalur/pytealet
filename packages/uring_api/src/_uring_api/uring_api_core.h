#ifndef URING_API_CORE_H
#define URING_API_CORE_H

/* private implementation header; not part of the public C API. */

#include "uring_api_common.h"

#include "uring_api_capi.h"

/*
 * SQE/CQE user_data tagging (io_uring user_data is always u64).
 *
 * PyObject* / Completion* values are at least 4-byte aligned, so bits 1:0 are 0.
 * Bit 0 set ⇒ special (not a Completion*). Bit 1 selects the special class:
 *
 *   bits 1:0 == 00  → Completion* (waitable path)
 *   bits 1:0 == 01  → wake NOP (break_wait / neutralize / stop_serving)
 *   bits 1:0 == 10  → link-timeout CQE. bits 63:2 are a timespec* this SQE owns.
 *                     freed when the CQE is consumed. not a Completion, not delivered.
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
#define URING_API_UD_TAG_LINK_TIMEOUT 0x2ull
#define URING_API_UD_TAG_NOWAIT 0x3ull

#define URING_API_WAKE_USER_DATA URING_API_UD_TAG_WAKE

#define URING_API_NOWAIT_KIND_SHIFT 2
#define URING_API_NOWAIT_KIND_MASK 0x3full
#define URING_API_NOWAIT_FD_SHIFT 8
#define URING_API_NOWAIT_FD_BITS 32
#define URING_API_NOWAIT_FD_MASK 0xffffffffull
/* advisory: no associated fd (cancel / poll_remove acks) */
#define URING_API_NOWAIT_FD_NONE ((unsigned int)0xffffffffu)

/* any non-zero low tag is not a Completion* */
static inline int uring_api_ud_is_special(unsigned long long user_data) {
    return (user_data & URING_API_UD_TAG_MASK) != URING_API_UD_TAG_COMPLETION;
}

/* timer CQE. bits 63:2 are the timespec* the SQE owns. */
static inline int uring_api_ud_is_link_timeout(unsigned long long user_data) {
    return (user_data & URING_API_UD_TAG_MASK) == URING_API_UD_TAG_LINK_TIMEOUT;
}

static inline unsigned long long uring_api_link_timeout_user_data(const void *ts) {
    uintptr_t ptr = (uintptr_t)ts;

    assert((ptr & (uintptr_t)URING_API_UD_TAG_MASK) == 0);
    return (unsigned long long)ptr | URING_API_UD_TAG_LINK_TIMEOUT;
}

static inline struct __kernel_timespec *uring_api_ud_link_timeout_timespec(unsigned long long user_data) {
    return (struct __kernel_timespec *)(uintptr_t)(user_data & ~URING_API_UD_TAG_MASK);
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
/* One SQE this thread prepared that the publish rule cares about.
 * waitable is a Completion* fill, a cancel that sets sq_waitable, or a
 * parked close, shutdown, or poll_remove. A plain ring ignores a direct
 * nowait. SQPOLL counts it too: publishing that tail is cheap. Not a
 * link-timeout SQE, not a wake NOP, and not a slot that was rolled back.
 * Monotonic for the life of the thread. Shared by every ring the thread
 * touches. */
void ring_note_thread_sqe(UringApiRing *self, int waitable);
/* One housekeeping SQE this thread filled: a send-all next leg, or a
 * parked copy that submit() had already covered (the leg notes itself).
 * A prepare() that no submit() has covered does not move this count.
 * auto_submit does not cover it. The handler publishes when this thread's
 * housekeeping flag is set. Monotonic for the life of the thread. */
void ring_note_thread_internal(void);
uint64_t ring_thread_internal_sqes(void);
uint64_t ring_thread_sqes_prepared(void);
UringApiCompletion *cqe_get_completion(UringApiRing *self, struct io_uring_cqe *cqe);
unsigned int ring_sq_entries(UringApiRing *self);
unsigned int ring_cq_entries(UringApiRing *self);

int parse_entries_flags(URING_API_PARSE_ARGS, unsigned int default_entries, unsigned int *entries, unsigned int *flags);
int parse_numeric_sockaddr(int fd, PyObject *address, struct sockaddr_storage *storage, socklen_t *addrlen);
void ring_pending_inc(UringApiRing *self);
void ring_pending_dec(UringApiRing *self);
unsigned int ring_pending_count(UringApiRing *self);
int ring_check_open(UringApiRing *self);
/*
 * Returns 0 if this thread may submit, -1 if not. The decision is
 * submission_policy.workers_may_submit, cooked once from the setup flags.
 * The owner may submit either way. When raise_on_error is non-zero, sets
 * RuntimeError on failure; otherwise fails quietly (no exception).
 */
int ring_check_submit_thread(UringApiRing *self, int raise_on_error);
/* Fill submission_policy from auto_submit and the setup flags already
 * stored on the ring. Called from init. workers_may_submit follows the
 * setup flags only. */
void ring_submission_policy_apply(UringApiRing *self, int auto_submit);
/* 1 when this thread's policy flag for that automatic enter is set.
 * The owner thread reads the issuer flag. Any other thread reads the
 * worker flag. Quiet: never sets an exception. Explicit submit() does
 * not use these. */
int ring_policy_on_sq_full(UringApiRing *self);
int ring_policy_before_reap(UringApiRing *self);
int ring_policy_callback_sqes(UringApiRing *self);
/* 1 when this thread publishes a next leg or a submit-owed parked copy.
 * Independent of auto_submit. The owner reads issuer_housekeeping.
 * Any other thread reads worker_housekeeping. */
int ring_policy_housekeeping(UringApiRing *self);
int ring_check_client_thread(UringApiRing *self);
/* Flush pending SQEs. Allows zero submitted. Returns 0 or -1 with exception.
 * When submitted_out is non-NULL, adds the batch. A normal ring adds
 * io_uring_submit's return (unsubmitted SQEs). SQPOLL always calls
 * io_uring_submit and adds how far sqe_tail was ahead of liburing's
 * userspace head. A call that copies nothing adds nothing. */
int ring_flush_pending(UringApiRing *self, int *submitted_out);
/* SQPOLL: wait until the kernel thread has consumed the tail liburing
 * already published. does not call io_uring_submit, so an unpublished
 * tail is left for queue_exit to drop. caller holds the ring CS.
 * non-SQPOLL returns 0. 0 when the published tail is consumed. -1 with
 * an exception if the clock fails or the poller does not catch up. */
int ring_sqpoll_drain_published(UringApiRing *self);
/* Copy Ring.stats() counters. cqe may be a completion ahead of the rest. */
void ring_read_stats(UringApiRing *self, UringApiRingStats *out);
/* Flush and require at least one SQE (e.g. after preparing a wake NOP). */
int submit_one(UringApiRing *self);
int receive_wait_begin(UringApiRing *self);
void receive_wait_end(UringApiRing *self);
/* 1 if the unique CQ waiter role is held (may already be in wait_cqe). */
int cqe_unique_waiter_active(UringApiRing *self);
/* 1 when this handler should publish. prepared_at_entry is
 * ring_thread_sqes_prepared() sampled before the handler filled SQEs.
 * This thread's callback_sqes flag must be set. callback_submit selects
 * the rule. WHEN_WAITER_HELD: the cared count must have moved and the
 * unique waiter role must already be held. On a plain ring that count is
 * waitable SQEs, so a direct nowait does not publish. Once the host has
 * blocked in its loop (wait_idle with a non-zero timeout, or
 * note_driver_waiting(True)), the host must also be inside that park.
 * Before that first block the sleep flag is ignored. END_OF_TURN: the
 * same count moved. Under SQPOLL it includes a direct nowait. The waiter
 * and the host park are not checked. Does not enter by itself. */
int ring_should_we_submit(UringApiRing *self, uint64_t prepared_at_entry);
/* Caller holds the ring CS. When ring_should_we_submit is true, submit
 * whatever is queued. The decision is the cared count. When it is
 * true the whole ordered queue is flushed, so a non-waitable already in
 * that queue rides along. Empty SQ is a no-op. Owning thread counts as
 * main, any other as worker. 0 if nothing was required, -1 on submit error. */
int ring_publish_if_waiter_active(UringApiRing *self, uint64_t prepared_at_entry);
/* Caller holds the ring CS. Publish when this handler filled a housekeeping
 * SQE (ring_note_thread_internal since internal_at) and this thread's
 * housekeeping flag is set. Ignores auto_submit and the waiter gates. A
 * caller prepare does not move the count, so it stays queued unless it
 * was already on the tail. The publish copies the whole unpublished tail.
 * 0 if nothing was required or this thread's flag is off, -1 on submit error. */
int ring_flush_internal_since(UringApiRing *self, uint64_t internal_at);
bool delivery_is_running_locked(UringApiRing *self);
int delivery_check_not_running(UringApiRing *self);
void delivery_mark_exited(UringApiRing *self);
/* Take one SQE. The on_sq_full policy flag off raises SubmissionQueueFull
 * instead of flushing. The wake NOP uses this. Prepare, next-leg, and
 * leftover drain use get_sqe_try. */
struct io_uring_sqe *get_sqe(UringApiRing *self);
/* 0 if the ring has at least need submission entries. -1 with RuntimeError
 * when it does not: flushing cannot create a slot the queue lacks. Does not
 * enter or wait. */
int sq_check_need(UringApiRing *self, unsigned int need);
/* Make room for need free SQ slots. Caller holds the ring CS and has already
 * decided this thread may enter. Does not take a slot. 1 if
 * io_uring_sq_space_left >= need. -1 with the too-small-ring RuntimeError,
 * the stuck RuntimeError, a flush OSError, or the SQPOLL wait error.
 * SubmissionQueueFull is not raised here: that means the caller was not
 * allowed to enter. io_uring_sqring_wait runs only when the SQ is completely
 * full. A partly full queue sleeps until need slots are free. */
int sq_ensure_space(UringApiRing *self, unsigned int need, int *submitted_out);
/* io_uring_get_sqe first (any thread). Enter only when flush_if_full and this
 * thread may submit, or when the on_sq_full policy flag is set. Does not
 * raise SubmissionQueueFull.
 * 1: *sqe_out set. 0: no slot and this thread will not enter (no exception).
 * -1: error (stuck SQ, flush OSError, submit-thread contract). */
int get_sqe_try(UringApiRing *self, int flush_if_full, int *submitted_out, struct io_uring_sqe **sqe_out);

#endif
