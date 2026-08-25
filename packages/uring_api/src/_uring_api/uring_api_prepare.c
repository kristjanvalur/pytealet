/*
 * Submission methods for the _uring_api Ring type.
 */

#include "uring_api_prepare.h"
#include "uring_api_bufgroup.h"
#include "uring_api_completion.h"
#include "uring_api_core.h"
#include "uring_api_fd_table.h"
#include "uring_api_probe.h"
#include "uring_api_staging.h"
#include "uring_api_statx.h"

#ifndef IORING_RECVSEND_POLL_FIRST
#define IORING_RECVSEND_POLL_FIRST (1U << 0)
#endif

/* POLL_FIRST is sqe->ioprio, not MSG_* msg_flags.
 * Bit 0 is also MSG_OOB: that value is poll-first, not OOB. */
static unsigned int recvsend_msg_flags(unsigned int flags) { return flags & ~(unsigned int)IORING_RECVSEND_POLL_FIRST; }

static void recvsend_apply_ioprio(struct io_uring_sqe *sqe, unsigned int flags) {
    if (flags & IORING_RECVSEND_POLL_FIRST) {
        sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
    }
}

static Py_ssize_t send_all_remaining(const UringApiCompletionViewState *view_state) {
    if (view_state->offset >= (unsigned long long)view_state->view.len) {
        return 0;
    }
    return view_state->view.len - (Py_ssize_t)view_state->offset;
}

static void take_in_flight_ref(UringApiRing *self, UringApiCompletion *completion) {
    Py_INCREF(completion);
    completion->aux_lock = &self->refcount_mutex;
    ring_pending_inc(self);
}

static int send_all_park_continuation(UringApiRing *self, UringApiCompletion *completion) {
    UringApiCompletionViewState *view_state;
    UringApiFdSlot *slot;

    if (completion_has_bit(completion, URING_API_C_SEND_ALL_CONT)) {
        return 0;
    }
    view_state = UringApiCompletion_get_view_state(completion);
    assert(view_state != NULL);
    slot = fd_table_get(self, view_state->fd);
    if (!slot) {
        return -1;
    }
    slot->active = completion;
    slot->continuation_pending = 1;
    completion_set_bit(completion, URING_API_C_SEND_ALL_CONT);
    fd_table_mark_drain(self, slot);
    return 0;
}

static int send_all_fill_sqe(UringApiRing *self, UringApiCompletion *completion, struct io_uring_sqe *sqe,
                             int later_leg) {
    UringApiCompletionViewState *view_state;
    unsigned int flags;
    Py_ssize_t remaining;

    view_state = UringApiCompletion_get_view_state(completion);
    assert(view_state != NULL && view_state->has_view);
    if (completion_has_bit(completion, URING_API_C_SEND_ALL_ABANDON)) {
        io_uring_prep_nop(sqe);
        sqe_set_completion(self, sqe, (PyObject *)completion);
        completion_clear_bit(completion, URING_API_C_SEND_ALL_CONT);
        return 0;
    }
    remaining = send_all_remaining(view_state);
    flags = view_state->flags;
    /* later legs set POLL_FIRST only when the 5.19 probe says the ioprio bit exists. */
    if (later_leg && uring_api_recvsend_poll_first_capable()) {
        flags |= IORING_RECVSEND_POLL_FIRST;
    }
    io_uring_prep_send(sqe, view_state->fd, (char *)view_state->view.buf + (Py_ssize_t)view_state->offset,
                       (size_t)remaining, (int)recvsend_msg_flags(flags));
    recvsend_apply_ioprio(sqe, flags);
    sqe_set_completion(self, sqe, (PyObject *)completion);
    completion_clear_bit(completion, URING_API_C_SEND_ALL_CONT);
    return 0;
}

static int completion_kind_conflicts(UringApiPendingKind kind) {
    return kind == URING_API_PENDING_SEND || kind == URING_API_PENDING_SEND_ALL || kind == URING_API_PENDING_SEND_ZC ||
           kind == URING_API_PENDING_SENDMSG || kind == URING_API_PENDING_SENDMSG_ZC ||
           kind == URING_API_PENDING_CLOSE || kind == URING_API_PENDING_SHUTDOWN;
}

static int completion_conflict_fd(UringApiCompletion *completion, int *fd_out) {
    UringApiCompletionViewState *view_state;
    UringApiCompletionViewSockaddrState *view_sockaddr_state;
    UringApiCompletionMsgState *msg_state;
    UringApiCompletionScalarState *scalar_state;

    switch (completion->kind) {
    case URING_API_PENDING_SEND:
    case URING_API_PENDING_SEND_ALL:
    case URING_API_PENDING_SEND_ZC:
    case URING_API_PENDING_RECV:
        view_state = UringApiCompletion_get_view_state(completion);
        assert(view_state != NULL);
        *fd_out = view_state->fd;
        return 0;
    case URING_API_PENDING_SENDTO:
        view_sockaddr_state = UringApiCompletion_get_view_sockaddr_state(completion);
        assert(view_sockaddr_state != NULL);
        *fd_out = view_sockaddr_state->fd;
        return 0;
    case URING_API_PENDING_SENDMSG:
    case URING_API_PENDING_SENDMSG_ZC:
    case URING_API_PENDING_RECVMSG:
        msg_state = UringApiCompletion_get_msg_state(completion);
        assert(msg_state != NULL);
        *fd_out = msg_state->fd;
        return 0;
    case URING_API_PENDING_CLOSE:
    case URING_API_PENDING_SHUTDOWN:
        scalar_state = UringApiCompletion_get_scalar_state(completion);
        assert(scalar_state != NULL);
        *fd_out = scalar_state->fd;
        return 0;
    case URING_API_PENDING_CANCEL:
    case URING_API_PENDING_POLL_REMOVE:
        if (completion->cancel_target == NULL ||
            !PyObject_TypeCheck(completion->cancel_target, &UringApiCompletion_Type)) {
            return -1;
        }
        return completion_conflict_fd((UringApiCompletion *)completion->cancel_target, fd_out);
    default:
        return -1;
    }
}

static int is_cancel_of_active(UringApiCompletion *completion, UringApiFdSlot *slot) {
    return completion->kind == URING_API_PENDING_CANCEL && slot->active != NULL &&
           (UringApiCompletion *)completion->cancel_target == slot->active;
}

static int should_enqueue_conflict(UringApiRing *self, UringApiCompletion *completion, UringApiFdSlot **slot_out) {
    int fd;
    UringApiFdSlot *slot;
    UringApiCompletion *target;

    if (completion_conflict_fd(completion, &fd) < 0) {
        return 0;
    }
    slot = fd_table_lookup(self, fd);
    if (slot_out) {
        *slot_out = slot;
    }
    if (slot == NULL || slot->active == NULL) {
        return 0;
    }
    if (is_cancel_of_active(completion, slot)) {
        return 0;
    }
    if (completion_kind_conflicts(completion->kind)) {
        return 1;
    }
    if (completion->kind != URING_API_PENDING_CANCEL || completion->cancel_target == NULL) {
        return 0;
    }
    target = (UringApiCompletion *)completion->cancel_target;
    return completion_has_bit(target, URING_API_C_CONFLICT_QUEUED);
}

static int enqueue_conflict(UringApiRing *self, UringApiCompletion *completion) {
    int fd;
    UringApiFdSlot *slot;

    if (completion_conflict_fd(completion, &fd) < 0) {
        assert(0 && "conflict enqueue is missing an fd");
        return -1;
    }
    slot = fd_table_get(self, fd);
    if (!slot) {
        return -1;
    }
    if (completion->kind == URING_API_PENDING_SEND_ALL) {
        take_in_flight_ref(self, completion);
    }
    completion_set_bit(completion, URING_API_C_CONFLICT_QUEUED);
    if (fd_table_fifo_push(slot, completion) < 0) {
        completion_clear_bit(completion, URING_API_C_CONFLICT_QUEUED);
        if (completion->kind == URING_API_PENDING_SEND_ALL) {
            ring_pending_dec(self);
            Py_DECREF(completion);
        }
        return -1;
    }
    fd_table_mark_drain(self, slot);
    return 0;
}

static int mark_send_all_active(UringApiRing *self, UringApiCompletion *completion) {
    UringApiCompletionViewState *view_state;
    UringApiFdSlot *slot;

    view_state = UringApiCompletion_get_view_state(completion);
    assert(view_state != NULL);
    slot = fd_table_get(self, view_state->fd);
    if (!slot) {
        return -1;
    }
    slot->active = completion;
    slot->continuation_pending = 0;
    return 0;
}

static int drain_fd_slot(UringApiRing *self, UringApiFdSlot *slot, int flush_if_full, int *submitted_out);

int send_all_flush_continuations(UringApiRing *self, int flush_if_full, int *submitted_out) {
    while (self->fd_drain_head) {
        UringApiFdSlot *slot = self->fd_drain_head;

        /* flush_if_full: submit() makes SQ room. prepare respects auto_submit. */
        if (drain_fd_slot(self, slot, flush_if_full, submitted_out) < 0) {
            return -1;
        }
    }
    return 0;
}

void send_all_clear_continuations(UringApiRing *self) { fd_table_clear(self); }

static int prepare_one_constructed_ex(UringApiRing *self, UringApiCompletion *completion, int from_fifo,
                                      int flush_if_full, int *submitted_out);

static int fill_queued_completion(UringApiRing *self, UringApiCompletion *completion, int flush_if_full,
                                  int *submitted_out) {
    return prepare_one_constructed_ex(self, completion, 1, flush_if_full, submitted_out);
}

static int drain_fd_slot(UringApiRing *self, UringApiFdSlot *slot, int flush_if_full, int *submitted_out) {
    fd_table_unlink_drain(self, slot);
    if (slot->continuation_pending) {
        struct io_uring_sqe *sqe;

        sqe = get_sqe_ex(self, flush_if_full, submitted_out);
        if (!sqe) {
            fd_table_mark_drain(self, slot);
            return -1;
        }
        if (send_all_fill_sqe(self, slot->active, sqe, 1) < 0) {
            fd_table_mark_drain(self, slot);
            return -1;
        }
        slot->continuation_pending = 0;
        if (self->auto_submit && self->experimental_send_all_submit_next && ring_flush_pending(self, NULL) < 0) {
            fd_table_mark_drain(self, slot);
            return -1;
        }
    }
    while (slot->fifo.count) {
        UringApiCompletion *completion = fd_table_fifo_peek(slot);

        if (slot->active != NULL && !is_cancel_of_active(completion, slot)) {
            break;
        }
        completion = fd_table_fifo_pop(slot);
        if (fill_queued_completion(self, completion, flush_if_full, submitted_out) < 0) {
            if (fd_table_fifo_push_front(slot, completion) < 0) {
                Py_DECREF(completion);
                return -1;
            }
            fd_table_mark_drain(self, slot);
            return -1;
        }
        completion_clear_bit(completion, URING_API_C_CONFLICT_QUEUED);
        Py_DECREF(completion);
    }
    if (slot->continuation_pending) {
        fd_table_mark_drain(self, slot);
    } else if (slot->fifo.count > 0 && slot->active == NULL) {
        fd_table_mark_drain(self, slot);
    } else if (slot->fifo.count > 0 && is_cancel_of_active(fd_table_fifo_peek(slot), slot)) {
        fd_table_mark_drain(self, slot);
    } else {
        fd_table_try_free(self, slot);
    }
    return 0;
}

static int send_all_release_active(UringApiRing *self, UringApiCompletion *completion) {
    UringApiCompletionViewState *view_state;
    UringApiFdSlot *slot;
    int failed = 0;

    view_state = UringApiCompletion_get_view_state(completion);
    assert(view_state != NULL);
    Py_BEGIN_CRITICAL_SECTION(self);
    slot = fd_table_lookup(self, view_state->fd);
    if (slot != NULL && slot->active == completion) {
        slot->active = NULL;
        slot->continuation_pending = 0;
        completion_clear_bit(completion, URING_API_C_SEND_ALL_CONT);
        if (slot->fifo.count > 0) {
            fd_table_mark_drain(self, slot);
            if (ring_check_submit_thread(self, 0) == 0) {
                if (drain_fd_slot(self, slot, 0, NULL) < 0) {
                    failed = 1;
                } else if (self->auto_submit && ring_flush_pending(self, NULL) < 0) {
                    failed = 1;
                }
            }
        } else {
            fd_table_try_free(self, slot);
        }
    }
    Py_END_CRITICAL_SECTION();
    return failed ? -1 : 0;
}

static int send_all_try_next_leg(UringApiRing *self, UringApiCompletion *completion) {
    struct io_uring_sqe *sqe;
    int failed = 0;

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else if (ring_check_submit_thread(self, 0) < 0) {
        if (send_all_park_continuation(self, completion) < 0) {
            failed = 1;
        }
    } else {
        sqe = io_uring_get_sqe(&self->ring);
        if (!sqe && self->auto_submit) {
            if (ring_flush_pending(self, NULL) < 0) {
                failed = 1;
            } else {
                sqe = io_uring_get_sqe(&self->ring);
            }
        }
        if (!failed && !sqe) {
            if (send_all_park_continuation(self, completion) < 0) {
                failed = 1;
            }
        } else if (!failed) {
            if (send_all_fill_sqe(self, completion, sqe, 1) < 0) {
                failed = 1;
            } else if (self->auto_submit && self->experimental_send_all_submit_next &&
                       ring_flush_pending(self, NULL) < 0) {
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();
    return failed ? -1 : 0;
}

int send_all_on_cqe(UringApiRing *self, UringApiCompletion *completion, int res, unsigned int flags) {
    UringApiCompletionViewState *view_state;
    Py_ssize_t remaining;
    int complete_res;
    int status;

    view_state = UringApiCompletion_get_view_state(completion);
    assert(view_state != NULL && view_state->has_view);
    remaining = send_all_remaining(view_state);
    if (res > 0) {
        assert((Py_ssize_t)res <= remaining);
        view_state->offset += (unsigned long long)res;
        remaining = send_all_remaining(view_state);
    }
    if (res < 0) {
        complete_res = res;
    } else if (remaining == 0) {
        /* Completion.res is CQE-shaped int; clamp. result holds the full offset. */
        if (view_state->offset > (unsigned long long)INT_MAX) {
            complete_res = INT_MAX;
        } else {
            complete_res = (int)view_state->offset;
        }
    } else if (completion_has_bit(completion, URING_API_C_SEND_ALL_ABANDON)) {
        complete_res = -ECANCELED;
        /* a partial success CQE was staged as non-terminal if abandon was set after drain. */
        if (res > 0) {
            uring_api_refcount_mutex_lock(&self->refcount_mutex);
            completion_set_bit(completion, URING_API_C_AUX_DECREF);
            uring_api_refcount_mutex_unlock(&self->refcount_mutex);
        }
    } else if (res == 0) {
        complete_res = -EAGAIN;
    } else {
        if (send_all_try_next_leg(self, completion) < 0) {
            return -1;
        }
        return 1;
    }
    status = UringApiCompletion_complete(completion, complete_res, flags);
    if (status < 0) {
        return -1;
    }
    if (complete_res >= 0) {
        PyObject *payload = PyLong_FromUnsignedLongLong(view_state->offset);
        if (!payload) {
            return -1;
        }
        Py_XSETREF(completion->result, payload);
    }
    if (send_all_release_active(self, completion) < 0) {
        return -1;
    }
    if (completion_has_bit(completion, URING_API_C_NOWAIT)) {
        if (complete_res < 0) {
            staging_report_nowait_error(self, complete_res, flags, (unsigned int)completion->kind, 1, view_state->fd);
        }
        return 1;
    }
    return 0;
}

static int parse_socket_fd(PyObject *obj, int *fd_out) {
    long value = PyLong_AsLong(obj);

    if (value == -1 && PyErr_Occurred()) {
        return -1;
    }
    if (value < 0) {
        PyErr_SetString(PyExc_ValueError, "fd must be non-negative");
        return -1;
    }
    if (value > INT_MAX) {
        PyErr_SetString(PyExc_OverflowError, "fd out of range");
        return -1;
    }
    *fd_out = (int)value;
    return 0;
}

/* signed int for how/flags-like args (SHUT_RD etc.); no non-negative check */
static int parse_int_arg(PyObject *obj, int *value_out) {
    long value = PyLong_AsLong(obj);

    if (value == -1 && PyErr_Occurred()) {
        return -1;
    }
    if (value < INT_MIN || value > INT_MAX) {
        PyErr_SetString(PyExc_OverflowError, "integer out of range");
        return -1;
    }
    *value_out = (int)value;
    return 0;
}

static int parse_uint_arg(PyObject *obj, unsigned int *value_out) {
    unsigned long value = PyLong_AsUnsignedLong(obj);

    if (value == (unsigned long)-1 && PyErr_Occurred()) {
        return -1;
    }
    if (value > UINT_MAX) {
        PyErr_SetString(PyExc_OverflowError, "integer out of range");
        return -1;
    }
    *value_out = (unsigned int)value;
    return 0;
}

static int parse_ull_arg(PyObject *obj, unsigned long long *value_out) {
    unsigned long long value = PyLong_AsUnsignedLongLong(obj);

    if (value == (unsigned long long)-1 && PyErr_Occurred()) {
        return -1;
    }
    *value_out = value;
    return 0;
}

static int parse_recv_multishot_args(const char *name, PyObject *const *args, Py_ssize_t nargs, int *fd_out,
                                     PyObject **buf_group_out, unsigned int *flags_out, PyObject **user_data_out) {
    if (nargs < 2) {
        PyErr_Format(PyExc_TypeError, "%s() missing required arguments 'fd' and 'buf_group'", name);
        return -1;
    }
    if (nargs > 4) {
        PyErr_Format(PyExc_TypeError, "%s() takes at most 4 positional arguments (%zd given)", name, nargs);
        return -1;
    }

    if (parse_socket_fd(args[0], fd_out) < 0) {
        return -1;
    }
    if (!PyObject_TypeCheck(args[1], &UringApiBufGroup_Type)) {
        PyErr_SetString(PyExc_TypeError, "buf_group must be a BufGroup");
        return -1;
    }
    *buf_group_out = args[1];
    if (nargs > 2) {
        if (parse_uint_arg(args[2], flags_out) < 0) {
            return -1;
        }
    }
    if (nargs > 3) {
        *user_data_out = args[3];
    }
    return 0;
}

static int parse_send_args(const char *name, PyObject *const *args, Py_ssize_t nargs, Py_ssize_t max_nargs, int *fd_out,
                           Py_buffer *view_out, PyObject **user_data_out, unsigned int *flags_out,
                           unsigned int *zc_flags_out, int parse_zc_flags) {
    if (nargs < 2) {
        PyErr_Format(PyExc_TypeError, "%s() missing required arguments 'fd' and 'data'", name);
        return -1;
    }
    if (nargs > max_nargs) {
        PyErr_Format(PyExc_TypeError, "%s() takes at most %zd positional arguments (%zd given)", name, max_nargs,
                     nargs);
        return -1;
    }
    if (parse_socket_fd(args[0], fd_out) < 0) {
        return -1;
    }
    if (PyObject_GetBuffer(args[1], view_out, PyBUF_STRIDED_RO) < 0) {
        return -1;
    }
    /* fd, data, [flags], [zc_flags], [user_data] */
    if (nargs > 2) {
        if (parse_uint_arg(args[2], flags_out) < 0) {
            PyBuffer_Release(view_out);
            return -1;
        }
    }
    if (parse_zc_flags) {
        if (nargs > 3) {
            if (parse_uint_arg(args[3], zc_flags_out) < 0) {
                PyBuffer_Release(view_out);
                return -1;
            }
        }
        if (nargs > 4) {
            *user_data_out = args[4];
        }
    } else if (nargs > 3) {
        *user_data_out = args[3];
    }
    return 0;
}

static int parse_accept_listener_args(const char *name, PyObject *const *args, Py_ssize_t nargs, int *fd_out,
                                      unsigned int *flags_out, PyObject **user_data_out) {
    if (nargs < 1) {
        PyErr_Format(PyExc_TypeError, "%s() missing required argument 'fd'", name);
        return -1;
    }
    if (nargs > 3) {
        PyErr_Format(PyExc_TypeError, "%s() takes at most 3 positional arguments (%zd given)", name, nargs);
        return -1;
    }
    if (parse_socket_fd(args[0], fd_out) < 0) {
        return -1;
    }
    if (nargs > 1) {
        if (parse_uint_arg(args[1], flags_out) < 0) {
            return -1;
        }
    }
    if (nargs > 2) {
        *user_data_out = args[2];
    }
    return 0;
}

static int validate_file_io_buffer_length(Py_buffer *view) {
    if (view->len < 0 || (unsigned long long)view->len > UINT_MAX) {
        PyErr_SetString(PyExc_ValueError, "buffer length must fit in uint32_t");
        return -1;
    }
    return 0;
}

static int validate_statx_buffer(Py_buffer *view) {
    if (view->len < URING_API_STATX_BUFFER_SIZE) {
        PyErr_SetString(PyExc_ValueError, "statx buffer must be at least 256 bytes");
        return -1;
    }
    return 0;
}

static PyObject *prepare_after_construct(UringApiRing *self, PyObject *completion);

PyObject *UringApiRing_prepare_recv_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                         PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_recv_impl(self, fd, view, flags, user_data));
}

static PyObject *construct_pending_buf_group(UringApiRing *self, UringApiPendingKind kind, int fd,
                                             PyObject *buf_group_obj, unsigned int flags, PyObject *user_data,
                                             int multishot) {
    UringApiBufGroup *buf_group;
    PyObject *completion;
    UringApiCompletionBufGroupState *buf_group_state;

    if (!buf_group_obj || !PyObject_TypeCheck(buf_group_obj, &UringApiBufGroup_Type)) {
        PyErr_SetString(PyExc_TypeError, "buf_group must be a BufGroup");
        return NULL;
    }
    buf_group = (UringApiBufGroup *)buf_group_obj;
    if (buf_group->ring != self) {
        PyErr_SetString(PyExc_ValueError, "buf_group was not created by this ring");
        return NULL;
    }
    if (ring_check_open(self) < 0) {
        return NULL;
    }
    completion = UringApiCompletion_new_pending_buf_group(kind, user_data, buf_group_obj);
    if (!completion) {
        return NULL;
    }
    if (multishot) {
        completion_set_bit((UringApiCompletion *)completion, URING_API_C_MULTISHOT);
    }
    buf_group_state = UringApiCompletion_get_buf_group_state((UringApiCompletion *)completion);
    assert(buf_group_state != NULL);
    buf_group_state->fd = fd;
    buf_group_state->flags = flags;
    return completion;
}

PyObject *UringApiRing_construct_recv_buf_impl(UringApiRing *self, int fd, PyObject *buf_group_obj, unsigned int flags,
                                               PyObject *user_data) {
    return construct_pending_buf_group(self, URING_API_PENDING_RECV_BUF, fd, buf_group_obj, flags, user_data, 0);
}

PyObject *UringApiRing_construct_recv_multishot_impl(UringApiRing *self, int fd, PyObject *buf_group_obj,
                                                     unsigned int flags, PyObject *user_data) {
    return construct_pending_buf_group(self, URING_API_PENDING_RECV_MULTISHOT, fd, buf_group_obj, flags, user_data, 1);
}

PyObject *UringApiRing_prepare_recv_buf_impl(UringApiRing *self, int fd, PyObject *buf_group_obj, unsigned int flags,
                                             PyObject *user_data) {
    return prepare_after_construct(self,
                                   UringApiRing_construct_recv_buf_impl(self, fd, buf_group_obj, flags, user_data));
}

PyObject *UringApiRing_prepare_recv_buf(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "buf_group", "flags", "user_data", NULL};
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;
    PyObject *buf_group_obj;

    if (!URING_API_PARSE_KEYWORDS("iO!|IO", keywords, &fd, &UringApiBufGroup_Type, &buf_group_obj, &flags,
                                  &user_data)) {
        return NULL;
    }
    return UringApiRing_prepare_recv_buf_impl(self, fd, buf_group_obj, flags, user_data);
}

PyObject *UringApiRing_prepare_recv_multishot_impl(UringApiRing *self, int fd, PyObject *buf_group_obj,
                                                   unsigned int flags, PyObject *user_data) {
    return prepare_after_construct(
        self, UringApiRing_construct_recv_multishot_impl(self, fd, buf_group_obj, flags, user_data));
}

static PyObject *construct_pending_view(UringApiRing *self, UringApiPendingKind kind, int fd, Py_buffer *view,
                                        unsigned int flags, unsigned int zc_flags, unsigned long long offset,
                                        PyObject *user_data) {
    PyObject *completion;
    UringApiCompletionViewState *view_state;

    if (ring_check_open(self) < 0) {
        PyBuffer_Release(view);
        return NULL;
    }
    completion = UringApiCompletion_new_pending_view(kind, user_data, view);
    if (!completion) {
        return NULL;
    }
    view_state = UringApiCompletion_get_view_state((UringApiCompletion *)completion);
    assert(view_state != NULL);
    view_state->fd = fd;
    view_state->flags = flags;
    view_state->zc_flags = zc_flags;
    view_state->offset = offset;
    return completion;
}

PyObject *UringApiRing_construct_send_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                           PyObject *user_data) {
    return construct_pending_view(self, URING_API_PENDING_SEND, fd, view, flags, 0, 0, user_data);
}

PyObject *UringApiRing_construct_send_all_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                               PyObject *user_data) {
    return construct_pending_view(self, URING_API_PENDING_SEND_ALL, fd, view, flags, 0, 0, user_data);
}

PyObject *UringApiRing_construct_send_zc_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                              unsigned int zc_flags, PyObject *user_data) {
    return construct_pending_view(self, URING_API_PENDING_SEND_ZC, fd, view, flags, zc_flags, 0, user_data);
}

PyObject *UringApiRing_construct_recv_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                           PyObject *user_data) {
    return construct_pending_view(self, URING_API_PENDING_RECV, fd, view, flags, 0, 0, user_data);
}

PyObject *UringApiRing_construct_read_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned long long offset,
                                           PyObject *user_data) {
    if (validate_file_io_buffer_length(view) < 0) {
        PyBuffer_Release(view);
        return NULL;
    }
    return construct_pending_view(self, URING_API_PENDING_READ, fd, view, 0, 0, offset, user_data);
}

PyObject *UringApiRing_construct_write_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned long long offset,
                                            PyObject *user_data) {
    if (validate_file_io_buffer_length(view) < 0) {
        PyBuffer_Release(view);
        return NULL;
    }
    return construct_pending_view(self, URING_API_PENDING_WRITE, fd, view, 0, 0, offset, user_data);
}

PyObject *UringApiRing_construct_sendto_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                             unsigned int flags, PyObject *user_data) {
    PyObject *completion;
    UringApiCompletionViewSockaddrState *sendto_state;

    if (ring_check_open(self) < 0) {
        PyBuffer_Release(view);
        return NULL;
    }
    completion = UringApiCompletion_new_pending_view_sockaddr(URING_API_PENDING_SENDTO, user_data, view);
    if (!completion) {
        return NULL;
    }
    sendto_state = UringApiCompletion_get_view_sockaddr_state((UringApiCompletion *)completion);
    assert(sendto_state != NULL);
    if (parse_numeric_sockaddr(fd, address, &sendto_state->addr, &sendto_state->addrlen) < 0) {
        Py_DECREF(completion);
        return NULL;
    }
    sendto_state->fd = fd;
    sendto_state->flags = flags;
    return completion;
}

static PyObject *construct_pending_msg(UringApiRing *self, UringApiPendingKind kind, int fd, Py_buffer *view,
                                       PyObject *address, unsigned int flags, PyObject *user_data) {
    PyObject *completion;
    UringApiCompletionMsgState *msg_state;

    if (ring_check_open(self) < 0) {
        PyBuffer_Release(view);
        return NULL;
    }
    if (kind == URING_API_PENDING_RECVMSG) {
        completion = UringApiCompletion_new_pending_recvmsg(kind, user_data, view);
    } else {
        completion = UringApiCompletion_new_pending_sendmsg(kind, user_data, view);
    }
    if (!completion) {
        return NULL;
    }
    msg_state = UringApiCompletion_get_msg_state((UringApiCompletion *)completion);
    assert(msg_state != NULL);
    if (address != NULL && address != Py_None) {
        if (parse_numeric_sockaddr(fd, address, &msg_state->addr, &msg_state->addrlen) < 0) {
            Py_DECREF(completion);
            return NULL;
        }
        msg_state->msg.msg_name = &msg_state->addr;
        msg_state->msg.msg_namelen = msg_state->addrlen;
    }
    msg_state->fd = fd;
    msg_state->flags = flags;
    return completion;
}

PyObject *UringApiRing_construct_recvmsg_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                              PyObject *user_data) {
    return construct_pending_msg(self, URING_API_PENDING_RECVMSG, fd, view, NULL, flags, user_data);
}

PyObject *UringApiRing_construct_sendmsg_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                              unsigned int flags, PyObject *user_data) {
    return construct_pending_msg(self, URING_API_PENDING_SENDMSG, fd, view, address, flags, user_data);
}

PyObject *UringApiRing_construct_sendmsg_zc_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                                 unsigned int flags, PyObject *user_data) {
    return construct_pending_msg(self, URING_API_PENDING_SENDMSG_ZC, fd, view, address, flags, user_data);
}

PyObject *UringApiRing_construct_connect_impl(UringApiRing *self, int fd, PyObject *address, PyObject *user_data) {
    PyObject *completion;
    UringApiCompletionSockaddrState *sockaddr_state;

    if (ring_check_open(self) < 0) {
        return NULL;
    }
    completion = UringApiCompletion_new_pending_sockaddr(URING_API_PENDING_CONNECT, user_data);
    if (!completion) {
        return NULL;
    }
    sockaddr_state = UringApiCompletion_get_sockaddr_state((UringApiCompletion *)completion);
    assert(sockaddr_state != NULL);
    if (parse_numeric_sockaddr(fd, address, &sockaddr_state->addr, &sockaddr_state->addrlen) < 0) {
        Py_DECREF(completion);
        return NULL;
    }
    sockaddr_state->fd = fd;
    return completion;
}

static int constructed_kind_ready(UringApiCompletion *completion) {
    UringApiCompletionViewState *view_state;
    UringApiCompletionViewSockaddrState *view_sockaddr_state;
    UringApiCompletionMsgState *msg_state;
    UringApiCompletionSockaddrState *sockaddr_state;

    switch (completion->kind) {
    case URING_API_PENDING_SEND:
    case URING_API_PENDING_SEND_ZC:
    case URING_API_PENDING_SEND_ALL:
    case URING_API_PENDING_RECV:
    case URING_API_PENDING_READ:
    case URING_API_PENDING_WRITE:
        view_state = UringApiCompletion_get_view_state(completion);
        return view_state != NULL && view_state->fd >= 0;
    case URING_API_PENDING_SENDTO:
        view_sockaddr_state = UringApiCompletion_get_view_sockaddr_state(completion);
        return view_sockaddr_state != NULL && view_sockaddr_state->fd >= 0;
    case URING_API_PENDING_RECVMSG:
    case URING_API_PENDING_SENDMSG:
    case URING_API_PENDING_SENDMSG_ZC:
        msg_state = UringApiCompletion_get_msg_state(completion);
        return msg_state != NULL && msg_state->fd >= 0;
    case URING_API_PENDING_CONNECT:
        sockaddr_state = UringApiCompletion_get_sockaddr_state(completion);
        return sockaddr_state != NULL && sockaddr_state->fd >= 0;
    case URING_API_PENDING_RECV_BUF:
    case URING_API_PENDING_RECV_MULTISHOT: {
        UringApiCompletionBufGroupState *buf_group_state = UringApiCompletion_get_buf_group_state(completion);
        return buf_group_state != NULL && buf_group_state->fd >= 0;
    }
    case URING_API_PENDING_OPENAT: {
        UringApiCompletionPathState *path_state = UringApiCompletion_get_path_state(completion);
        return path_state != NULL && path_state->constructed;
    }
    case URING_API_PENDING_STATX: {
        UringApiCompletionStatxState *statx_state = UringApiCompletion_get_statx_state(completion);
        return statx_state != NULL && statx_state->constructed;
    }
    case URING_API_PENDING_STATX_FDSIZE: {
        UringApiCompletionStatxFdsizeState *statx_fdsize_state = UringApiCompletion_get_statx_fdsize_state(completion);
        return statx_fdsize_state != NULL && statx_fdsize_state->constructed;
    }
    case URING_API_PENDING_ACCEPT:
    case URING_API_PENDING_POLL:
    case URING_API_PENDING_POLL_MULTISHOT:
    case URING_API_PENDING_CLOSE:
    case URING_API_PENDING_SHUTDOWN:
    case URING_API_PENDING_SOCKET: {
        UringApiCompletionScalarState *scalar_state = UringApiCompletion_get_scalar_state(completion);
        return scalar_state != NULL && scalar_state->constructed;
    }
    case URING_API_PENDING_CANCEL:
    case URING_API_PENDING_POLL_REMOVE:
        return completion->cancel_target != NULL;
    default:
        return 0;
    }
}

/* Nowait SQE identity: tagged token, optional CQE_SKIP_SUCCESS. No Completion*. */
static int stamp_nowait_sqe(UringApiRing *self, struct io_uring_sqe *sqe, unsigned int kind, int fd) {
    io_uring_sqe_set_data64(sqe, uring_api_make_nowait_user_data(kind, fd));
    if (self->ring.features & IORING_FEAT_CQE_SKIP) {
        sqe->flags |= IOSQE_CQE_SKIP_SUCCESS;
    }
    return 0;
}

static int nowait_kind_ok(UringApiPendingKind kind) {
    return kind == URING_API_PENDING_CLOSE || kind == URING_API_PENDING_SHUTDOWN || kind == URING_API_PENDING_CANCEL ||
           kind == URING_API_PENDING_POLL_REMOVE || kind == URING_API_PENDING_SEND_ALL;
}

static int nowait_advisory_fd(UringApiCompletion *completion) {
    UringApiCompletionScalarState *scalar_state;

    if (completion->kind != URING_API_PENDING_CLOSE && completion->kind != URING_API_PENDING_SHUTDOWN) {
        return -1;
    }
    scalar_state = UringApiCompletion_get_scalar_state(completion);
    assert(scalar_state != NULL);
    return scalar_state->fd;
}

/* Caller holds the ring critical section. On success the completion is prepared
 * or parked on that fd's conflict FIFO. Waitable ops hold an in-flight ref until
 * CQE delivery. Ordinary nowait ops stamp a tagged SQE and drop the Completion;
 * nowait send_all keeps the Completion* SQE and the in-flight ref until the
 * drain terminals. Kind is checked before prepared so a non-constructed handle
 * reports "not constructed", not "already prepared". */
static int prepare_one_constructed_ex(UringApiRing *self, UringApiCompletion *completion, int from_fifo,
                                      int flush_if_full, int *submitted_out) {
    UringApiCompletionViewState *view_state;
    UringApiCompletionViewSockaddrState *view_sockaddr_state;
    UringApiCompletionMsgState *msg_state;
    UringApiCompletionSockaddrState *sockaddr_state;
    struct io_uring_sqe *sqe;
    int already_in_flight = from_fifo && completion->kind == URING_API_PENDING_SEND_ALL;

    if (!constructed_kind_ready(completion)) {
        PyErr_SetString(PyExc_ValueError, "prepare() only accepts constructed completions");
        return -1;
    }
    if (!from_fifo && completion_is_accepted(completion)) {
        PyErr_SetString(PyExc_ValueError, "completion is already prepared");
        return -1;
    }
    if (completion_has_bit(completion, URING_API_C_NOWAIT) && !nowait_kind_ok(completion->kind)) {
        PyErr_SetString(PyExc_ValueError,
                        "nowait is only valid for close, shutdown, cancel, poll_remove, and send_all");
        return -1;
    }

    /* abandon before parked-continuation flush so a next-leg is a NOP, not another send. */
    if (!from_fifo && completion->kind == URING_API_PENDING_CANCEL && completion->cancel_target != NULL &&
        ((UringApiCompletion *)completion->cancel_target)->kind == URING_API_PENDING_SEND_ALL) {
        completion_set_bit((UringApiCompletion *)completion->cancel_target, URING_API_C_SEND_ALL_ABANDON);
    }

    if (!from_fifo && should_enqueue_conflict(self, completion, NULL)) {
        return enqueue_conflict(self, completion);
    }

    /* user prepare, not FIFO drain: fill parked next-legs first so they get the
     * next SQ slot. from_fifo skips this — drain_fd_slot is already walking the
     * list; calling back would recurse. */
    if (!from_fifo && send_all_flush_continuations(self, 0, NULL) < 0) {
        return -1;
    }

    sqe = get_sqe_ex(self, flush_if_full, submitted_out);
    if (!sqe) {
        return -1;
    }
    switch (completion->kind) {
    case URING_API_PENDING_SEND:
        view_state = UringApiCompletion_get_view_state(completion);
        assert(view_state != NULL && view_state->has_view);
        io_uring_prep_send(sqe, view_state->fd, view_state->view.buf, (size_t)view_state->view.len,
                           (int)recvsend_msg_flags(view_state->flags));
        recvsend_apply_ioprio(sqe, view_state->flags);
        break;
    case URING_API_PENDING_SEND_ALL:
        if (send_all_fill_sqe(self, completion, sqe, 0) < 0) {
            return -1;
        }
        break;
    case URING_API_PENDING_SEND_ZC:
        view_state = UringApiCompletion_get_view_state(completion);
        assert(view_state != NULL && view_state->has_view);
        io_uring_prep_send_zc(sqe, view_state->fd, view_state->view.buf, (size_t)view_state->view.len,
                              (int)recvsend_msg_flags(view_state->flags), view_state->zc_flags);
        recvsend_apply_ioprio(sqe, view_state->flags);
        break;
    case URING_API_PENDING_RECV:
        view_state = UringApiCompletion_get_view_state(completion);
        assert(view_state != NULL && view_state->has_view);
        io_uring_prep_recv(sqe, view_state->fd, view_state->view.buf, (size_t)view_state->view.len,
                           (int)recvsend_msg_flags(view_state->flags));
        recvsend_apply_ioprio(sqe, view_state->flags);
        break;
    case URING_API_PENDING_READ:
        view_state = UringApiCompletion_get_view_state(completion);
        assert(view_state != NULL && view_state->has_view);
        io_uring_prep_read(sqe, view_state->fd, view_state->view.buf, (unsigned)view_state->view.len,
                           (__u64)view_state->offset);
        break;
    case URING_API_PENDING_WRITE:
        view_state = UringApiCompletion_get_view_state(completion);
        assert(view_state != NULL && view_state->has_view);
        io_uring_prep_write(sqe, view_state->fd, view_state->view.buf, (unsigned)view_state->view.len,
                            (__u64)view_state->offset);
        break;
    case URING_API_PENDING_SENDTO:
        view_sockaddr_state = UringApiCompletion_get_view_sockaddr_state(completion);
        assert(view_sockaddr_state != NULL && view_sockaddr_state->has_view);
        io_uring_prep_sendto(sqe, view_sockaddr_state->fd, view_sockaddr_state->view.buf,
                             (size_t)view_sockaddr_state->view.len, (int)recvsend_msg_flags(view_sockaddr_state->flags),
                             (struct sockaddr *)&view_sockaddr_state->addr, view_sockaddr_state->addrlen);
        recvsend_apply_ioprio(sqe, view_sockaddr_state->flags);
        break;
    case URING_API_PENDING_RECVMSG:
        msg_state = UringApiCompletion_get_msg_state(completion);
        assert(msg_state != NULL && msg_state->has_view);
        io_uring_prep_recvmsg(sqe, msg_state->fd, &msg_state->msg, (int)recvsend_msg_flags(msg_state->flags));
        recvsend_apply_ioprio(sqe, msg_state->flags);
        break;
    case URING_API_PENDING_SENDMSG:
        msg_state = UringApiCompletion_get_msg_state(completion);
        assert(msg_state != NULL && msg_state->has_view);
        io_uring_prep_sendmsg(sqe, msg_state->fd, &msg_state->msg, recvsend_msg_flags(msg_state->flags));
        recvsend_apply_ioprio(sqe, msg_state->flags);
        break;
    case URING_API_PENDING_SENDMSG_ZC:
        msg_state = UringApiCompletion_get_msg_state(completion);
        assert(msg_state != NULL && msg_state->has_view);
        io_uring_prep_sendmsg_zc(sqe, msg_state->fd, &msg_state->msg, recvsend_msg_flags(msg_state->flags));
        recvsend_apply_ioprio(sqe, msg_state->flags);
        break;
    case URING_API_PENDING_CONNECT:
        sockaddr_state = UringApiCompletion_get_sockaddr_state(completion);
        assert(sockaddr_state != NULL);
        io_uring_prep_connect(sqe, sockaddr_state->fd, (struct sockaddr *)&sockaddr_state->addr,
                              sockaddr_state->addrlen);
        break;
    case URING_API_PENDING_RECV_BUF: {
        UringApiCompletionBufGroupState *buf_group_state = UringApiCompletion_get_buf_group_state(completion);
        UringApiBufGroup *buf_group;

        assert(buf_group_state != NULL && buf_group_state->buf_group != NULL);
        buf_group = (UringApiBufGroup *)buf_group_state->buf_group;
        io_uring_prep_recv(sqe, buf_group_state->fd, NULL, (size_t)buf_group->buffer_size,
                           (int)recvsend_msg_flags(buf_group_state->flags));
        recvsend_apply_ioprio(sqe, buf_group_state->flags);
        sqe->flags |= IOSQE_BUFFER_SELECT;
        sqe->buf_group = buf_group->group_id;
        break;
    }
    case URING_API_PENDING_RECV_MULTISHOT: {
        UringApiCompletionBufGroupState *buf_group_state = UringApiCompletion_get_buf_group_state(completion);
        UringApiBufGroup *buf_group;

        assert(buf_group_state != NULL && buf_group_state->buf_group != NULL);
        buf_group = (UringApiBufGroup *)buf_group_state->buf_group;
        io_uring_prep_recv_multishot(sqe, buf_group_state->fd, NULL, 0,
                                     (int)recvsend_msg_flags(buf_group_state->flags));
        /* kernel can strand MORE with no EOF CQE; do not set POLL_FIRST */
        sqe->flags |= IOSQE_BUFFER_SELECT;
        sqe->buf_group = buf_group->group_id;
        break;
    }
    case URING_API_PENDING_OPENAT: {
        UringApiCompletionPathState *path_state = UringApiCompletion_get_path_state(completion);

        assert(path_state != NULL && path_state->path != NULL);
        io_uring_prep_openat(sqe, path_state->dfd, path_state->path, path_state->flags, path_state->mode);
        break;
    }
    case URING_API_PENDING_STATX: {
        UringApiCompletionStatxState *statx_state = UringApiCompletion_get_statx_state(completion);

        assert(statx_state != NULL && statx_state->path != NULL && statx_state->has_view);
        io_uring_prep_statx(sqe, statx_state->dfd, statx_state->path, statx_state->flags, statx_state->mask,
                            (struct statx *)statx_state->view.buf);
        break;
    }
    case URING_API_PENDING_STATX_FDSIZE: {
        UringApiCompletionStatxFdsizeState *statx_fdsize_state = UringApiCompletion_get_statx_fdsize_state(completion);

        assert(statx_fdsize_state != NULL);
        io_uring_prep_statx(sqe, statx_fdsize_state->fd, "", URING_API_AT_EMPTY_PATH, URING_API_STATX_SIZE_MASK,
                            (struct statx *)statx_fdsize_state->buf);
        break;
    }
    case URING_API_PENDING_ACCEPT: {
        UringApiCompletionScalarState *scalar_state = UringApiCompletion_get_scalar_state(completion);

        assert(scalar_state != NULL);
        if (completion_has_bit(completion, URING_API_C_MULTISHOT)) {
            io_uring_prep_multishot_accept(sqe, scalar_state->fd, NULL, NULL, scalar_state->flags);
        } else {
            io_uring_prep_accept(sqe, scalar_state->fd, NULL, NULL, scalar_state->flags);
        }
        break;
    }
    case URING_API_PENDING_POLL: {
        UringApiCompletionScalarState *scalar_state = UringApiCompletion_get_scalar_state(completion);

        assert(scalar_state != NULL);
        io_uring_prep_poll_add(sqe, scalar_state->fd, scalar_state->poll_mask);
        break;
    }
    case URING_API_PENDING_POLL_MULTISHOT: {
        UringApiCompletionScalarState *scalar_state = UringApiCompletion_get_scalar_state(completion);

        assert(scalar_state != NULL);
        io_uring_prep_poll_multishot(sqe, scalar_state->fd, scalar_state->poll_mask);
        break;
    }
    case URING_API_PENDING_CLOSE: {
        UringApiCompletionScalarState *scalar_state = UringApiCompletion_get_scalar_state(completion);

        assert(scalar_state != NULL);
        io_uring_prep_close(sqe, scalar_state->fd);
        break;
    }
    case URING_API_PENDING_SHUTDOWN: {
        UringApiCompletionScalarState *scalar_state = UringApiCompletion_get_scalar_state(completion);

        assert(scalar_state != NULL);
        io_uring_prep_shutdown(sqe, scalar_state->fd, scalar_state->how);
        break;
    }
    case URING_API_PENDING_SOCKET: {
        UringApiCompletionScalarState *scalar_state = UringApiCompletion_get_scalar_state(completion);

        assert(scalar_state != NULL);
        io_uring_prep_socket(sqe, scalar_state->domain, scalar_state->type, scalar_state->protocol,
                             scalar_state->flags);
        break;
    }
    case URING_API_PENDING_CANCEL: {
        UringApiCompletion *cancel_target;

        assert(completion->cancel_target != NULL);
        io_uring_prep_cancel(sqe, completion->cancel_target, 0);
        cancel_target = (UringApiCompletion *)completion->cancel_target;
        if (cancel_target->kind == URING_API_PENDING_SEND_ALL) {
            completion_set_bit(cancel_target, URING_API_C_SEND_ALL_ABANDON);
        }
        break;
    }
    case URING_API_PENDING_POLL_REMOVE:
        assert(completion->cancel_target != NULL);
        io_uring_prep_poll_remove(sqe, (unsigned long long)(uintptr_t)completion->cancel_target);
        break;
    default:
        /* kind already validated */
        break;
    }
    if (completion_has_bit(completion, URING_API_C_NOWAIT) && completion->kind != URING_API_PENDING_SEND_ALL) {
        if (stamp_nowait_sqe(self, sqe, (unsigned int)completion->kind, nowait_advisory_fd(completion)) < 0) {
            return -1;
        }
        completion_set_bit(completion, URING_API_C_PREPARED);
        return 0;
    }
    sqe_set_completion(self, sqe, (PyObject *)completion);
    if (!already_in_flight) {
        /* in-flight ref: matches the leftover alloc ref on prepare_* paths */
        take_in_flight_ref(self, completion);
    }
    if (completion->kind == URING_API_PENDING_SEND_ALL && mark_send_all_active(self, completion) < 0) {
        return -1;
    }
    return 0;
}

static int prepare_one_constructed(UringApiRing *self, UringApiCompletion *completion) {
    return prepare_one_constructed_ex(self, completion, 0, 0, NULL);
}

static PyObject *prepare_after_construct(UringApiRing *self, PyObject *completion) {
    int failed = 0;

    if (!completion) {
        return NULL;
    }
    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else if (prepare_one_constructed(self, (UringApiCompletion *)completion) < 0) {
        failed = 1;
    }
    Py_END_CRITICAL_SECTION();
    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return completion;
}

static int prepare_constructed(UringApiRing *self, PyObject *const *items, Py_ssize_t count) {
    Py_ssize_t i;

    for (i = 0; i < count; i++) {
        PyObject *item = items[i];

        if (!PyObject_TypeCheck(item, &UringApiCompletion_Type)) {
            PyErr_SetString(PyExc_TypeError, "prepare() items must be Completion objects");
            return -1;
        }
        if (prepare_one_constructed(self, (UringApiCompletion *)item) < 0) {
            return -1;
        }
    }
    return 0;
}

int UringApiRing_prepare_impl(UringApiRing *self, PyObject *completions, int *prepared_out) {
    PyObject *seq = NULL;
    PyObject *single[1];
    PyObject *const *items;
    Py_ssize_t count;
    int failed = 0;
    int prepared = 0;

    if (PyObject_TypeCheck(completions, &UringApiCompletion_Type)) {
        single[0] = completions;
        items = single;
        count = 1;
    } else {
        seq = PySequence_Fast(completions, "completions must be a Completion or a sequence of Completions");
        if (!seq) {
            return -1;
        }
        items = PySequence_Fast_ITEMS(seq);
        count = PySequence_Fast_GET_SIZE(seq);
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else if (prepare_constructed(self, items, count) < 0) {
        failed = 1;
        /* count how many in the prefix are now prepared */
        {
            Py_ssize_t i;
            for (i = 0; i < count; i++) {
                if (PyObject_TypeCheck(items[i], &UringApiCompletion_Type) &&
                    (completion_has_bit((UringApiCompletion *)items[i], URING_API_C_PREPARED) ||
                     completion_has_bit((UringApiCompletion *)items[i], URING_API_C_CONFLICT_QUEUED))) {
                    prepared++;
                }
            }
        }
    } else {
        prepared = (int)count;
    }
    Py_END_CRITICAL_SECTION();

    Py_XDECREF(seq);
    if (prepared_out) {
        *prepared_out = prepared;
    }
    return failed ? -1 : 0;
}

PyObject *UringApiRing_prepare_send_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                         PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_send_impl(self, fd, view, flags, user_data));
}

PyObject *UringApiRing_prepare_send_all_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                             PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_send_all_impl(self, fd, view, flags, user_data));
}

PyObject *UringApiRing_prepare_read_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned long long offset,
                                         PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_read_impl(self, fd, view, offset, user_data));
}

PyObject *UringApiRing_prepare_write_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned long long offset,
                                          PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_write_impl(self, fd, view, offset, user_data));
}

PyObject *UringApiRing_construct_openat_impl(UringApiRing *self, int dfd, PyObject *path, int flags, unsigned int mode,
                                             PyObject *user_data) {
    PyObject *completion;
    UringApiCompletionPathState *path_state;

    if (ring_check_open(self) < 0) {
        return NULL;
    }
    completion = UringApiCompletion_new_pending_path(URING_API_PENDING_OPENAT, user_data, path);
    if (!completion) {
        return NULL;
    }
    path_state = UringApiCompletion_get_path_state((UringApiCompletion *)completion);
    assert(path_state != NULL && path_state->path != NULL);
    path_state->dfd = dfd;
    path_state->flags = flags;
    path_state->mode = mode;
    path_state->constructed = true;
    return completion;
}

PyObject *UringApiRing_construct_statx_impl(UringApiRing *self, int dfd, PyObject *path, int flags, unsigned int mask,
                                            Py_buffer *view, PyObject *user_data) {
    PyObject *completion;
    UringApiCompletionStatxState *statx_state;

    if (validate_statx_buffer(view) < 0) {
        PyBuffer_Release(view);
        return NULL;
    }
    if (ring_check_open(self) < 0) {
        PyBuffer_Release(view);
        return NULL;
    }
    completion = UringApiCompletion_new_pending_statx(URING_API_PENDING_STATX, user_data, path, view);
    if (!completion) {
        return NULL;
    }
    statx_state = UringApiCompletion_get_statx_state((UringApiCompletion *)completion);
    assert(statx_state != NULL && statx_state->path != NULL);
    statx_state->dfd = dfd;
    statx_state->flags = flags;
    statx_state->mask = mask;
    statx_state->constructed = true;
    return completion;
}

PyObject *UringApiRing_construct_statx_fdsize_impl(UringApiRing *self, int fd, PyObject *user_data) {
    PyObject *completion;
    UringApiCompletionStatxFdsizeState *statx_fdsize_state;

    if (ring_check_open(self) < 0) {
        return NULL;
    }
    completion = UringApiCompletion_new_pending_statx_fdsize(user_data);
    if (!completion) {
        return NULL;
    }
    statx_fdsize_state = UringApiCompletion_get_statx_fdsize_state((UringApiCompletion *)completion);
    assert(statx_fdsize_state != NULL);
    statx_fdsize_state->fd = fd;
    statx_fdsize_state->constructed = true;
    return completion;
}

PyObject *UringApiRing_prepare_openat_impl(UringApiRing *self, int dfd, PyObject *path, int flags, unsigned int mode,
                                           PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_openat_impl(self, dfd, path, flags, mode, user_data));
}

PyObject *UringApiRing_prepare_statx_impl(UringApiRing *self, int dfd, PyObject *path, int flags, unsigned int mask,
                                          Py_buffer *view, PyObject *user_data) {
    return prepare_after_construct(self,
                                   UringApiRing_construct_statx_impl(self, dfd, path, flags, mask, view, user_data));
}

PyObject *UringApiRing_prepare_send_zc_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                            unsigned int zc_flags, PyObject *user_data) {
    return prepare_after_construct(self,
                                   UringApiRing_construct_send_zc_impl(self, fd, view, flags, zc_flags, user_data));
}

PyObject *UringApiRing_prepare_sendto_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                           unsigned int flags, PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_sendto_impl(self, fd, view, address, flags, user_data));
}

PyObject *UringApiRing_prepare_recvmsg_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                            PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_recvmsg_impl(self, fd, view, flags, user_data));
}

PyObject *UringApiRing_prepare_sendmsg_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                            unsigned int flags, PyObject *user_data) {
    return prepare_after_construct(self,
                                   UringApiRing_construct_sendmsg_impl(self, fd, view, address, flags, user_data));
}

PyObject *UringApiRing_prepare_sendmsg_zc_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                               unsigned int flags, PyObject *user_data) {
    return prepare_after_construct(self,
                                   UringApiRing_construct_sendmsg_zc_impl(self, fd, view, address, flags, user_data));
}

static PyObject *construct_pending_scalar(UringApiRing *self, UringApiPendingKind kind, PyObject *user_data,
                                          int multishot) {
    PyObject *completion;
    UringApiCompletionScalarState *scalar_state;

    if (ring_check_open(self) < 0) {
        return NULL;
    }
    completion = UringApiCompletion_new_pending_scalar(kind, user_data);
    if (!completion) {
        return NULL;
    }
    if (multishot) {
        completion_set_bit((UringApiCompletion *)completion, URING_API_C_MULTISHOT);
    }
    scalar_state = UringApiCompletion_get_scalar_state((UringApiCompletion *)completion);
    assert(scalar_state != NULL);
    scalar_state->constructed = true;
    return completion;
}

PyObject *UringApiRing_construct_accept_impl(UringApiRing *self, int fd, unsigned int flags, PyObject *user_data) {
    PyObject *completion = construct_pending_scalar(self, URING_API_PENDING_ACCEPT, user_data, 0);
    UringApiCompletionScalarState *scalar_state;

    if (!completion) {
        return NULL;
    }
    scalar_state = UringApiCompletion_get_scalar_state((UringApiCompletion *)completion);
    scalar_state->fd = fd;
    scalar_state->flags = flags;
    return completion;
}

PyObject *UringApiRing_construct_accept_multishot_impl(UringApiRing *self, int fd, unsigned int flags,
                                                       PyObject *user_data) {
    PyObject *completion = construct_pending_scalar(self, URING_API_PENDING_ACCEPT, user_data, 1);
    UringApiCompletionScalarState *scalar_state;

    if (!completion) {
        return NULL;
    }
    scalar_state = UringApiCompletion_get_scalar_state((UringApiCompletion *)completion);
    scalar_state->fd = fd;
    scalar_state->flags = flags;
    return completion;
}

PyObject *UringApiRing_construct_poll_impl(UringApiRing *self, int fd, unsigned int poll_mask, PyObject *user_data) {
    PyObject *completion = construct_pending_scalar(self, URING_API_PENDING_POLL, user_data, 0);
    UringApiCompletionScalarState *scalar_state;

    if (!completion) {
        return NULL;
    }
    scalar_state = UringApiCompletion_get_scalar_state((UringApiCompletion *)completion);
    scalar_state->fd = fd;
    scalar_state->poll_mask = poll_mask;
    return completion;
}

PyObject *UringApiRing_construct_poll_multishot_impl(UringApiRing *self, int fd, unsigned int poll_mask,
                                                     PyObject *user_data) {
    PyObject *completion = construct_pending_scalar(self, URING_API_PENDING_POLL_MULTISHOT, user_data, 1);
    UringApiCompletionScalarState *scalar_state;

    if (!completion) {
        return NULL;
    }
    scalar_state = UringApiCompletion_get_scalar_state((UringApiCompletion *)completion);
    scalar_state->fd = fd;
    scalar_state->poll_mask = poll_mask;
    return completion;
}

PyObject *UringApiRing_construct_shutdown_impl(UringApiRing *self, int fd, int how, PyObject *user_data) {
    PyObject *completion = construct_pending_scalar(self, URING_API_PENDING_SHUTDOWN, user_data, 0);
    UringApiCompletionScalarState *scalar_state;

    if (!completion) {
        return NULL;
    }
    scalar_state = UringApiCompletion_get_scalar_state((UringApiCompletion *)completion);
    scalar_state->fd = fd;
    scalar_state->how = how;
    return completion;
}

PyObject *UringApiRing_construct_close_impl(UringApiRing *self, int fd, PyObject *user_data) {
    PyObject *completion = construct_pending_scalar(self, URING_API_PENDING_CLOSE, user_data, 0);
    UringApiCompletionScalarState *scalar_state;

    if (!completion) {
        return NULL;
    }
    scalar_state = UringApiCompletion_get_scalar_state((UringApiCompletion *)completion);
    scalar_state->fd = fd;
    return completion;
}

PyObject *UringApiRing_construct_socket_impl(UringApiRing *self, int domain, int type, int protocol, unsigned int flags,
                                             PyObject *user_data) {
    PyObject *completion = construct_pending_scalar(self, URING_API_PENDING_SOCKET, user_data, 0);
    UringApiCompletionScalarState *scalar_state;

    if (!completion) {
        return NULL;
    }
    scalar_state = UringApiCompletion_get_scalar_state((UringApiCompletion *)completion);
    scalar_state->domain = domain;
    scalar_state->type = type;
    scalar_state->protocol = protocol;
    scalar_state->flags = flags;
    return completion;
}

PyObject *UringApiRing_prepare_accept_impl(UringApiRing *self, int fd, unsigned int flags, PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_accept_impl(self, fd, flags, user_data));
}

PyObject *UringApiRing_prepare_accept_multishot_impl(UringApiRing *self, int fd, unsigned int flags,
                                                     PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_accept_multishot_impl(self, fd, flags, user_data));
}

PyObject *UringApiRing_prepare_connect_impl(UringApiRing *self, int fd, PyObject *address, PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_connect_impl(self, fd, address, user_data));
}

PyObject *UringApiRing_prepare_poll_impl(UringApiRing *self, int fd, unsigned int poll_mask, PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_poll_impl(self, fd, poll_mask, user_data));
}

PyObject *UringApiRing_prepare_poll_multishot_impl(UringApiRing *self, int fd, unsigned int poll_mask,
                                                   PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_poll_multishot_impl(self, fd, poll_mask, user_data));
}

static int poll_remove_target_is_valid(UringApiCompletion *target) {
    if (target->kind != URING_API_PENDING_POLL && target->kind != URING_API_PENDING_POLL_MULTISHOT) {
        PyErr_SetString(PyExc_ValueError,
                        "poll_remove target must be a pending poll or poll_multishot completion handle");
        return 0;
    }
    if (target->result != NULL) {
        PyErr_SetString(PyExc_ValueError,
                        "poll_remove target must be the original submit handle, not a delivered completion");
        return 0;
    }
    return 1;
}

static PyObject *construct_pending_cancel(UringApiRing *self, UringApiPendingKind kind, PyObject *target_completion,
                                          PyObject *user_data) {
    UringApiCompletion *completion;

    if (!PyObject_TypeCheck(target_completion, &UringApiCompletion_Type)) {
        PyErr_SetString(PyExc_TypeError, "completion must be a Completion");
        return NULL;
    }
    if (kind == URING_API_PENDING_POLL_REMOVE &&
        !poll_remove_target_is_valid((UringApiCompletion *)target_completion)) {
        return NULL;
    }
    if (ring_check_open(self) < 0) {
        return NULL;
    }
    if (user_data == NULL || user_data == Py_None) {
        user_data = target_completion;
    }
    completion = (UringApiCompletion *)UringApiCompletion_new_pending(kind, user_data);
    if (!completion) {
        return NULL;
    }
    completion->cancel_target = Py_NewRef(target_completion);
    return (PyObject *)completion;
}

PyObject *UringApiRing_construct_poll_remove_impl(UringApiRing *self, PyObject *target_completion,
                                                  PyObject *user_data) {
    return construct_pending_cancel(self, URING_API_PENDING_POLL_REMOVE, target_completion, user_data);
}

PyObject *UringApiRing_construct_cancel_impl(UringApiRing *self, PyObject *target_completion, PyObject *user_data) {
    return construct_pending_cancel(self, URING_API_PENDING_CANCEL, target_completion, user_data);
}

PyObject *UringApiRing_prepare_poll_remove_impl(UringApiRing *self, PyObject *target_completion, PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_poll_remove_impl(self, target_completion, user_data));
}

PyObject *UringApiRing_prepare_cancel_impl(UringApiRing *self, PyObject *target_completion, PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_cancel_impl(self, target_completion, user_data));
}

PyObject *UringApiRing_prepare_shutdown_impl(UringApiRing *self, int fd, int how, PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_shutdown_impl(self, fd, how, user_data));
}

PyObject *UringApiRing_prepare_close_impl(UringApiRing *self, int fd, PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_close_impl(self, fd, user_data));
}

static PyObject *mark_constructed_nowait(PyObject *completion) {
    if (!completion) {
        return NULL;
    }
    if (UringApiCompletion_set_nowait_flag((UringApiCompletion *)completion, 1) < 0) {
        Py_DECREF(completion);
        return NULL;
    }
    return completion;
}

static PyObject *prepare_nowait_after_construct(UringApiRing *self, PyObject *completion) {
    completion = prepare_after_construct(self, completion);
    if (!completion) {
        return NULL;
    }
    Py_DECREF(completion);
    Py_RETURN_NONE;
}

PyObject *UringApiRing_construct_close_nowait_impl(UringApiRing *self, int fd) {
    return mark_constructed_nowait(UringApiRing_construct_close_impl(self, fd, Py_None));
}

PyObject *UringApiRing_construct_shutdown_nowait_impl(UringApiRing *self, int fd, int how) {
    return mark_constructed_nowait(UringApiRing_construct_shutdown_impl(self, fd, how, Py_None));
}

PyObject *UringApiRing_construct_cancel_nowait_impl(UringApiRing *self, PyObject *target_completion) {
    return mark_constructed_nowait(UringApiRing_construct_cancel_impl(self, target_completion, Py_None));
}

PyObject *UringApiRing_construct_poll_remove_nowait_impl(UringApiRing *self, PyObject *target_completion) {
    return mark_constructed_nowait(UringApiRing_construct_poll_remove_impl(self, target_completion, Py_None));
}

PyObject *UringApiRing_prepare_close_nowait_impl(UringApiRing *self, int fd) {
    return prepare_nowait_after_construct(self, UringApiRing_construct_close_nowait_impl(self, fd));
}

PyObject *UringApiRing_prepare_shutdown_nowait_impl(UringApiRing *self, int fd, int how) {
    return prepare_nowait_after_construct(self, UringApiRing_construct_shutdown_nowait_impl(self, fd, how));
}

PyObject *UringApiRing_prepare_cancel_nowait_impl(UringApiRing *self, PyObject *target_completion) {
    return prepare_nowait_after_construct(self, UringApiRing_construct_cancel_nowait_impl(self, target_completion));
}

PyObject *UringApiRing_prepare_poll_remove_nowait_impl(UringApiRing *self, PyObject *target_completion) {
    return prepare_nowait_after_construct(self,
                                          UringApiRing_construct_poll_remove_nowait_impl(self, target_completion));
}

PyObject *UringApiRing_prepare_socket_impl(UringApiRing *self, int domain, int type, int protocol, unsigned int flags,
                                           PyObject *user_data) {
    return prepare_after_construct(self,
                                   UringApiRing_construct_socket_impl(self, domain, type, protocol, flags, user_data));
}

PyObject *UringApiRing_prepare_read(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "buf", "offset", "user_data", NULL};
    Py_buffer view;
    int fd;
    long long offset;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iw*L|O", keywords, &fd, &view, &offset, &user_data)) {
        return NULL;
    }
    if (offset < 0) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "offset must be non-negative");
        return NULL;
    }
    return UringApiRing_prepare_read_impl(self, fd, &view, (unsigned long long)offset, user_data);
}

PyObject *UringApiRing_prepare_write(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "data", "offset", "user_data", NULL};
    Py_buffer view;
    int fd;
    long long offset;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iy*L|O", keywords, &fd, &view, &offset, &user_data)) {
        return NULL;
    }
    if (offset < 0) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "offset must be non-negative");
        return NULL;
    }
    return UringApiRing_prepare_write_impl(self, fd, &view, (unsigned long long)offset, user_data);
}

PyObject *UringApiRing_prepare_openat(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"dfd", "path", "flags", "mode", "user_data", NULL};
    PyObject *path;
    int flags;
    unsigned int mode = 0;
    int dfd;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iOi|IO", keywords, &dfd, &path, &flags, &mode, &user_data)) {
        return NULL;
    }
    return UringApiRing_prepare_openat_impl(self, dfd, path, flags, mode, user_data);
}

PyObject *UringApiRing_prepare_statx(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"dfd", "path", "flags", "mask", "buf", "user_data", NULL};
    Py_buffer view;
    PyObject *path;
    int dfd;
    int flags;
    unsigned int mask;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iOIIw*|O", keywords, &dfd, &path, &flags, &mask, &view, &user_data)) {
        return NULL;
    }
    return UringApiRing_prepare_statx_impl(self, dfd, path, flags, mask, &view, user_data);
}

PyObject *UringApiRing_prepare_statx_fdsize_impl(UringApiRing *self, int fd, PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_statx_fdsize_impl(self, fd, user_data));
}

PyObject *UringApiRing_prepare_statx_fdsize(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "user_data", NULL};
    int fd;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("i|O", keywords, &fd, &user_data)) {
        return NULL;
    }
    return UringApiRing_prepare_statx_fdsize_impl(self, fd, user_data);
}

PyObject *UringApiRing_prepare_recv(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "buf", "flags", "user_data", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iw*|IO", keywords, &fd, &view, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_prepare_recv_impl(self, fd, &view, flags, user_data);
}

PyObject *UringApiRing_prepare_recv_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;
    PyObject *buf_group_obj;

    if (parse_recv_multishot_args("prepare_recv_multishot", args, nargs, &fd, &buf_group_obj, &flags, &user_data) < 0) {
        return NULL;
    }

    return UringApiRing_prepare_recv_multishot_impl(self, fd, buf_group_obj, flags, user_data);
}

PyObject *UringApiRing_construct_send(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd = -1;
    Py_buffer view;
    PyObject *user_data = Py_None;
    unsigned int flags = 0;

    if (parse_send_args("construct_send", args, nargs, 4, &fd, &view, &user_data, &flags, NULL, 0) < 0) {
        return NULL;
    }
    return UringApiRing_construct_send_impl(self, fd, &view, flags, user_data);
}

PyObject *UringApiRing_construct_send_all(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd = -1;
    Py_buffer view;
    PyObject *user_data = Py_None;
    unsigned int flags = 0;

    if (parse_send_args("construct_send_all", args, nargs, 4, &fd, &view, &user_data, &flags, NULL, 0) < 0) {
        return NULL;
    }
    return UringApiRing_construct_send_all_impl(self, fd, &view, flags, user_data);
}

PyObject *UringApiRing_construct_send_zc(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd = -1;
    Py_buffer view;
    PyObject *user_data = Py_None;
    unsigned int flags = 0;
    unsigned int zc_flags = 0;

    if (parse_send_args("construct_send_zc", args, nargs, 5, &fd, &view, &user_data, &flags, &zc_flags, 1) < 0) {
        return NULL;
    }
    return UringApiRing_construct_send_zc_impl(self, fd, &view, flags, zc_flags, user_data);
}

PyObject *UringApiRing_construct_recv(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "buf", "flags", "user_data", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iw*|IO", keywords, &fd, &view, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_recv_impl(self, fd, &view, flags, user_data);
}

PyObject *UringApiRing_construct_recv_buf(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "buf_group", "flags", "user_data", NULL};
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;
    PyObject *buf_group_obj;

    if (!URING_API_PARSE_KEYWORDS("iO!|IO", keywords, &fd, &UringApiBufGroup_Type, &buf_group_obj, &flags,
                                  &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_recv_buf_impl(self, fd, buf_group_obj, flags, user_data);
}

PyObject *UringApiRing_construct_recv_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;
    PyObject *buf_group_obj;

    if (parse_recv_multishot_args("construct_recv_multishot", args, nargs, &fd, &buf_group_obj, &flags, &user_data) <
        0) {
        return NULL;
    }
    return UringApiRing_construct_recv_multishot_impl(self, fd, buf_group_obj, flags, user_data);
}

PyObject *UringApiRing_construct_read(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "buf", "offset", "user_data", NULL};
    Py_buffer view;
    int fd;
    long long offset;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iw*L|O", keywords, &fd, &view, &offset, &user_data)) {
        return NULL;
    }
    if (offset < 0) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "offset must be non-negative");
        return NULL;
    }
    return UringApiRing_construct_read_impl(self, fd, &view, (unsigned long long)offset, user_data);
}

PyObject *UringApiRing_construct_write(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "data", "offset", "user_data", NULL};
    Py_buffer view;
    int fd;
    long long offset;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iy*L|O", keywords, &fd, &view, &offset, &user_data)) {
        return NULL;
    }
    if (offset < 0) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "offset must be non-negative");
        return NULL;
    }
    return UringApiRing_construct_write_impl(self, fd, &view, (unsigned long long)offset, user_data);
}

PyObject *UringApiRing_construct_openat(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"dfd", "path", "flags", "mode", "user_data", NULL};
    PyObject *path;
    int flags;
    unsigned int mode = 0;
    int dfd;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iOi|IO", keywords, &dfd, &path, &flags, &mode, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_openat_impl(self, dfd, path, flags, mode, user_data);
}

PyObject *UringApiRing_construct_statx(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"dfd", "path", "flags", "mask", "buf", "user_data", NULL};
    Py_buffer view;
    PyObject *path;
    int dfd;
    int flags;
    unsigned int mask;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iOIIw*|O", keywords, &dfd, &path, &flags, &mask, &view, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_statx_impl(self, dfd, path, flags, mask, &view, user_data);
}

PyObject *UringApiRing_construct_statx_fdsize(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "user_data", NULL};
    int fd;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("i|O", keywords, &fd, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_statx_fdsize_impl(self, fd, user_data);
}

PyObject *UringApiRing_construct_sendto(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "data", "address", "flags", "user_data", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *address;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iy*O|IO", keywords, &fd, &view, &address, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_sendto_impl(self, fd, &view, address, flags, user_data);
}

PyObject *UringApiRing_construct_recvmsg(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "buf", "flags", "user_data", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iw*|IO", keywords, &fd, &view, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_recvmsg_impl(self, fd, &view, flags, user_data);
}

PyObject *UringApiRing_construct_sendmsg(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "data", "address", "flags", "user_data", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *address = Py_None;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iy*|OIO", keywords, &fd, &view, &address, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_sendmsg_impl(self, fd, &view, address, flags, user_data);
}

PyObject *UringApiRing_construct_sendmsg_zc(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "data", "address", "flags", "user_data", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *address = Py_None;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iy*|OIO", keywords, &fd, &view, &address, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_sendmsg_zc_impl(self, fd, &view, address, flags, user_data);
}

PyObject *UringApiRing_construct_connect(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "address", "user_data", NULL};
    int fd;
    PyObject *address;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iO|O", keywords, &fd, &address, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_connect_impl(self, fd, address, user_data);
}

PyObject *UringApiRing_construct_accept(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (parse_accept_listener_args("construct_accept", args, nargs, &fd, &flags, &user_data) < 0) {
        return NULL;
    }
    return UringApiRing_construct_accept_impl(self, fd, flags, user_data);
}

PyObject *UringApiRing_construct_accept_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (parse_accept_listener_args("construct_accept_multishot", args, nargs, &fd, &flags, &user_data) < 0) {
        return NULL;
    }
    return UringApiRing_construct_accept_multishot_impl(self, fd, flags, user_data);
}

PyObject *UringApiRing_construct_poll(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int poll_mask;
    PyObject *user_data = Py_None;

    if (nargs < 2) {
        PyErr_SetString(PyExc_TypeError, "construct_poll() missing required arguments 'fd' and 'mask'");
        return NULL;
    }
    if (nargs > 3) {
        PyErr_Format(PyExc_TypeError, "construct_poll() takes at most 3 positional arguments (%zd given)", nargs);
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    if (parse_uint_arg(args[1], &poll_mask) < 0) {
        return NULL;
    }
    if (nargs > 2) {
        user_data = args[2];
    }
    return UringApiRing_construct_poll_impl(self, fd, poll_mask, user_data);
}

PyObject *UringApiRing_construct_poll_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int poll_mask;
    PyObject *user_data = Py_None;

    if (nargs < 2) {
        PyErr_SetString(PyExc_TypeError, "construct_poll_multishot() missing required arguments 'fd' and 'mask'");
        return NULL;
    }
    if (nargs > 3) {
        PyErr_Format(PyExc_TypeError, "construct_poll_multishot() takes at most 3 positional arguments (%zd given)",
                     nargs);
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    if (parse_uint_arg(args[1], &poll_mask) < 0) {
        return NULL;
    }
    if (nargs > 2) {
        user_data = args[2];
    }
    return UringApiRing_construct_poll_multishot_impl(self, fd, poll_mask, user_data);
}

PyObject *UringApiRing_construct_shutdown(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    int how;
    PyObject *user_data = Py_None;

    if (nargs < 2) {
        PyErr_SetString(PyExc_TypeError, "construct_shutdown() missing required arguments 'fd' and 'how'");
        return NULL;
    }
    if (nargs > 3) {
        PyErr_Format(PyExc_TypeError, "construct_shutdown() takes at most 3 positional arguments (%zd given)", nargs);
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    if (parse_int_arg(args[1], &how) < 0) {
        return NULL;
    }
    if (nargs > 2) {
        user_data = args[2];
    }
    return UringApiRing_construct_shutdown_impl(self, fd, how, user_data);
}

PyObject *UringApiRing_construct_close(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    PyObject *user_data = Py_None;

    if (nargs < 1) {
        PyErr_SetString(PyExc_TypeError, "construct_close() missing required argument 'fd'");
        return NULL;
    }
    if (nargs > 2) {
        PyErr_Format(PyExc_TypeError, "construct_close() takes at most 2 positional arguments (%zd given)", nargs);
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    if (nargs > 1) {
        user_data = args[1];
    }
    return UringApiRing_construct_close_impl(self, fd, user_data);
}

PyObject *UringApiRing_construct_socket(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"domain", "type", "protocol", "flags", "user_data", NULL};
    int domain;
    int type;
    int protocol = 0;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("ii|iIO", keywords, &domain, &type, &protocol, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_socket_impl(self, domain, type, protocol, flags, user_data);
}

PyObject *UringApiRing_construct_poll_remove(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    PyObject *user_data = Py_None;

    if (nargs < 1) {
        PyErr_SetString(PyExc_TypeError, "construct_poll_remove() missing required argument 'completion'");
        return NULL;
    }
    if (nargs > 2) {
        PyErr_Format(PyExc_TypeError, "construct_poll_remove() takes at most 2 positional arguments (%zd given)",
                     nargs);
        return NULL;
    }
    if (nargs > 1) {
        user_data = args[1];
    }
    return UringApiRing_construct_poll_remove_impl(self, args[0], user_data);
}

PyObject *UringApiRing_construct_cancel(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    PyObject *user_data = Py_None;

    if (nargs < 1) {
        PyErr_SetString(PyExc_TypeError, "construct_cancel() missing required argument 'completion'");
        return NULL;
    }
    if (nargs > 2) {
        PyErr_Format(PyExc_TypeError, "construct_cancel() takes at most 2 positional arguments (%zd given)", nargs);
        return NULL;
    }
    if (nargs > 1) {
        user_data = args[1];
    }
    return UringApiRing_construct_cancel_impl(self, args[0], user_data);
}

PyObject *UringApiRing_construct_close_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;

    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "construct_close_nowait() takes exactly 1 positional argument");
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    return UringApiRing_construct_close_nowait_impl(self, fd);
}

PyObject *UringApiRing_construct_shutdown_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    int how;

    if (nargs != 2) {
        PyErr_SetString(PyExc_TypeError, "construct_shutdown_nowait() takes exactly 2 positional arguments");
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    if (parse_int_arg(args[1], &how) < 0) {
        return NULL;
    }
    return UringApiRing_construct_shutdown_nowait_impl(self, fd, how);
}

PyObject *UringApiRing_construct_cancel_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "construct_cancel_nowait() takes exactly 1 positional argument");
        return NULL;
    }
    return UringApiRing_construct_cancel_nowait_impl(self, args[0]);
}

PyObject *UringApiRing_construct_poll_remove_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "construct_poll_remove_nowait() takes exactly 1 positional argument");
        return NULL;
    }
    return UringApiRing_construct_poll_remove_nowait_impl(self, args[0]);
}

PyObject *UringApiRing_prepare(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int prepared = 0;

    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "prepare() takes exactly 1 positional argument");
        return NULL;
    }
    if (UringApiRing_prepare_impl(self, args[0], &prepared) < 0) {
        return NULL;
    }
    return PyLong_FromLong(prepared);
}

PyObject *UringApiRing_prepare_send(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (parse_send_args("prepare_send", args, nargs, 4, &fd, &view, &user_data, &flags, NULL, 0) < 0) {
        return NULL;
    }
    return UringApiRing_prepare_send_impl(self, fd, &view, flags, user_data);
}

PyObject *UringApiRing_prepare_send_all(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (parse_send_args("prepare_send_all", args, nargs, 4, &fd, &view, &user_data, &flags, NULL, 0) < 0) {
        return NULL;
    }
    return UringApiRing_prepare_send_all_impl(self, fd, &view, flags, user_data);
}

PyObject *UringApiRing_prepare_send_zc(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    unsigned int zc_flags = 0;
    PyObject *user_data = Py_None;

    if (parse_send_args("prepare_send_zc", args, nargs, 5, &fd, &view, &user_data, &flags, &zc_flags, 1) < 0) {
        return NULL;
    }
    return UringApiRing_prepare_send_zc_impl(self, fd, &view, flags, zc_flags, user_data);
}

PyObject *UringApiRing_prepare_sendto(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "data", "address", "flags", "user_data", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *address;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iy*O|IO", keywords, &fd, &view, &address, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_prepare_sendto_impl(self, fd, &view, address, flags, user_data);
}

PyObject *UringApiRing_prepare_recvmsg(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "buf", "flags", "user_data", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iw*|IO", keywords, &fd, &view, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_prepare_recvmsg_impl(self, fd, &view, flags, user_data);
}

PyObject *UringApiRing_prepare_sendmsg(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "data", "address", "flags", "user_data", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *address = Py_None;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iy*|OIO", keywords, &fd, &view, &address, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_prepare_sendmsg_impl(self, fd, &view, address, flags, user_data);
}

PyObject *UringApiRing_prepare_sendmsg_zc(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "data", "address", "flags", "user_data", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *address = Py_None;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iy*|OIO", keywords, &fd, &view, &address, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_prepare_sendmsg_zc_impl(self, fd, &view, address, flags, user_data);
}

PyObject *UringApiRing_prepare_accept(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (parse_accept_listener_args("prepare_accept", args, nargs, &fd, &flags, &user_data) < 0) {
        return NULL;
    }
    return UringApiRing_prepare_accept_impl(self, fd, flags, user_data);
}

PyObject *UringApiRing_prepare_accept_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (parse_accept_listener_args("prepare_accept_multishot", args, nargs, &fd, &flags, &user_data) < 0) {
        return NULL;
    }
    return UringApiRing_prepare_accept_multishot_impl(self, fd, flags, user_data);
}

PyObject *UringApiRing_prepare_connect(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "address", "user_data", NULL};
    int fd;
    PyObject *address;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iO|O", keywords, &fd, &address, &user_data)) {
        return NULL;
    }
    return UringApiRing_prepare_connect_impl(self, fd, address, user_data);
}

PyObject *UringApiRing_prepare_poll(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int poll_mask;
    PyObject *user_data = Py_None;

    if (nargs < 2) {
        PyErr_SetString(PyExc_TypeError, "prepare_poll() missing required arguments 'fd' and 'mask'");
        return NULL;
    }
    if (nargs > 3) {
        PyErr_Format(PyExc_TypeError, "prepare_poll() takes at most 3 positional arguments (%zd given)", nargs);
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    if (parse_uint_arg(args[1], &poll_mask) < 0) {
        return NULL;
    }
    if (nargs > 2) {
        user_data = args[2];
    }
    return UringApiRing_prepare_poll_impl(self, fd, poll_mask, user_data);
}

PyObject *UringApiRing_prepare_poll_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int poll_mask;
    PyObject *user_data = Py_None;

    if (nargs < 2) {
        PyErr_SetString(PyExc_TypeError, "prepare_poll_multishot() missing required arguments 'fd' and 'mask'");
        return NULL;
    }
    if (nargs > 3) {
        PyErr_Format(PyExc_TypeError, "prepare_poll_multishot() takes at most 3 positional arguments (%zd given)",
                     nargs);
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    if (parse_uint_arg(args[1], &poll_mask) < 0) {
        return NULL;
    }
    if (nargs > 2) {
        user_data = args[2];
    }
    return UringApiRing_prepare_poll_multishot_impl(self, fd, poll_mask, user_data);
}

PyObject *UringApiRing_prepare_poll_remove(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    PyObject *user_data = Py_None;

    if (nargs < 1) {
        PyErr_SetString(PyExc_TypeError, "prepare_poll_remove() missing required argument 'completion'");
        return NULL;
    }
    if (nargs > 2) {
        PyErr_Format(PyExc_TypeError, "prepare_poll_remove() takes at most 2 positional arguments (%zd given)", nargs);
        return NULL;
    }
    if (!PyObject_TypeCheck(args[0], &UringApiCompletion_Type)) {
        PyErr_SetString(PyExc_TypeError, "completion must be a Completion");
        return NULL;
    }
    if (nargs > 1) {
        user_data = args[1];
    }
    return UringApiRing_prepare_poll_remove_impl(self, args[0], user_data);
}

PyObject *UringApiRing_prepare_poll_remove_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "prepare_poll_remove_nowait() takes exactly 1 positional argument");
        return NULL;
    }
    if (!PyObject_TypeCheck(args[0], &UringApiCompletion_Type)) {
        PyErr_SetString(PyExc_TypeError, "completion must be a Completion");
        return NULL;
    }
    return UringApiRing_prepare_poll_remove_nowait_impl(self, args[0]);
}

PyObject *UringApiRing_prepare_cancel(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    PyObject *user_data = Py_None;

    if (nargs < 1) {
        PyErr_SetString(PyExc_TypeError, "prepare_cancel() missing required argument 'completion'");
        return NULL;
    }
    if (nargs > 2) {
        PyErr_Format(PyExc_TypeError, "prepare_cancel() takes at most 2 positional arguments (%zd given)", nargs);
        return NULL;
    }
    if (!PyObject_TypeCheck(args[0], &UringApiCompletion_Type)) {
        PyErr_SetString(PyExc_TypeError, "completion must be a Completion");
        return NULL;
    }
    if (nargs > 1) {
        user_data = args[1];
    }
    return UringApiRing_prepare_cancel_impl(self, args[0], user_data);
}

PyObject *UringApiRing_prepare_cancel_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "prepare_cancel_nowait() takes exactly 1 positional argument");
        return NULL;
    }
    if (!PyObject_TypeCheck(args[0], &UringApiCompletion_Type)) {
        PyErr_SetString(PyExc_TypeError, "completion must be a Completion");
        return NULL;
    }
    return UringApiRing_prepare_cancel_nowait_impl(self, args[0]);
}

PyObject *UringApiRing_prepare_shutdown(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    int how;
    PyObject *user_data = Py_None;

    if (nargs < 2) {
        PyErr_SetString(PyExc_TypeError, "prepare_shutdown() missing required arguments 'fd' and 'how'");
        return NULL;
    }
    if (nargs > 3) {
        PyErr_Format(PyExc_TypeError, "prepare_shutdown() takes at most 3 positional arguments (%zd given)", nargs);
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    if (parse_int_arg(args[1], &how) < 0) {
        return NULL;
    }
    if (nargs > 2) {
        user_data = args[2];
    }
    return UringApiRing_prepare_shutdown_impl(self, fd, how, user_data);
}

PyObject *UringApiRing_prepare_shutdown_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    int how;

    if (nargs != 2) {
        PyErr_SetString(PyExc_TypeError, "prepare_shutdown_nowait() takes exactly 2 positional arguments");
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    if (parse_int_arg(args[1], &how) < 0) {
        return NULL;
    }
    return UringApiRing_prepare_shutdown_nowait_impl(self, fd, how);
}

PyObject *UringApiRing_prepare_close(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    PyObject *user_data = Py_None;

    if (nargs < 1) {
        PyErr_SetString(PyExc_TypeError, "prepare_close() missing required argument 'fd'");
        return NULL;
    }
    if (nargs > 2) {
        PyErr_Format(PyExc_TypeError, "prepare_close() takes at most 2 positional arguments (%zd given)", nargs);
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    if (nargs > 1) {
        user_data = args[1];
    }
    return UringApiRing_prepare_close_impl(self, fd, user_data);
}

PyObject *UringApiRing_prepare_close_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;

    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "prepare_close_nowait() takes exactly 1 positional argument");
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    return UringApiRing_prepare_close_nowait_impl(self, fd);
}

PyObject *UringApiRing_prepare_socket(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"domain", "type", "protocol", "flags", "user_data", NULL};
    int domain;
    int type;
    int protocol = 0;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("ii|iIO", keywords, &domain, &type, &protocol, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_prepare_socket_impl(self, domain, type, protocol, flags, user_data);
}
