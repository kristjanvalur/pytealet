/*
 * Opt-in Ring.wait timers: URING_API_WAIT_TIMING=1 dumps totals on ring close.
 */

#include "uring_api_wait_timing.h"
#include "uring_api_common.h"
#include "uring_api_dispatch.h"

#include <errno.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

static int wait_timing_enabled;

static atomic_ullong waits;
static atomic_ullong waits_peek;
static atomic_ullong waits_timeout;
static atomic_ullong waits_blocking;
static atomic_ullong flush_ns;
static atomic_ullong first_reap_ready_n;
static atomic_ullong first_reap_ready_ns;
static atomic_ullong first_reap_empty_n;
static atomic_ullong first_reap_empty_ns;
static atomic_ullong first_reap_timeout_n;
static atomic_ullong extra_peek_n;
static atomic_ullong extra_peek_ns;
static atomic_ullong build_ns;
static atomic_ullong staged_n;
static atomic_ullong delivery_ns;

static unsigned long long load_u(atomic_ullong *v) {
    return (unsigned long long)atomic_load_explicit(v, memory_order_relaxed);
}

static void add_u(atomic_ullong *v, unsigned long long n) { atomic_fetch_add_explicit(v, n, memory_order_relaxed); }

void uring_api_wait_timing_init(void) {
    const char *raw = getenv("URING_API_WAIT_TIMING");

    wait_timing_enabled = 0;
    if (raw == NULL || raw[0] == '\0') {
        return;
    }
    if (strcmp(raw, "1") == 0 || strcasecmp(raw, "true") == 0 || strcasecmp(raw, "yes") == 0) {
        wait_timing_enabled = 1;
    }
}

int uring_api_wait_timing_enabled(void) { return wait_timing_enabled; }

unsigned long long uring_api_wait_timing_now_ns(void) {
    struct timespec ts;

    if (!wait_timing_enabled) {
        return 0;
    }
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (unsigned long long)ts.tv_sec * 1000000000ull + (unsigned long long)ts.tv_nsec;
}

void uring_api_wait_timing_note_wait(int timeout_kind) {
    if (!wait_timing_enabled) {
        return;
    }
    add_u(&waits, 1);
    if (timeout_kind == URING_API_WAIT_PEEK) {
        add_u(&waits_peek, 1);
    } else if (timeout_kind == URING_API_WAIT_TIMEOUT) {
        add_u(&waits_timeout, 1);
    } else {
        add_u(&waits_blocking, 1);
    }
}

void uring_api_wait_timing_add_flush(unsigned long long ns) {
    if (!wait_timing_enabled) {
        return;
    }
    add_u(&flush_ns, ns);
}

void uring_api_wait_timing_add_first_reap(unsigned cq_ready, int timeout_kind, int reap_ret, unsigned long long ns) {
    if (!wait_timing_enabled) {
        return;
    }
    (void)timeout_kind;
    if (reap_ret < 0 &&
        (reap_ret == -ETIME || reap_ret == -ETIMEDOUT || reap_ret == -EAGAIN || reap_ret == -EWOULDBLOCK)) {
        add_u(&first_reap_timeout_n, 1);
    }
    if (cq_ready > 0) {
        add_u(&first_reap_ready_n, 1);
        add_u(&first_reap_ready_ns, ns);
        return;
    }
    add_u(&first_reap_empty_n, 1);
    add_u(&first_reap_empty_ns, ns);
}

void uring_api_wait_timing_add_extra_peek(unsigned n, unsigned long long ns) {
    if (!wait_timing_enabled || n == 0) {
        return;
    }
    add_u(&extra_peek_n, n);
    add_u(&extra_peek_ns, ns);
}

void uring_api_wait_timing_add_build(unsigned long long ns, size_t staged) {
    if (!wait_timing_enabled) {
        return;
    }
    add_u(&build_ns, ns);
    add_u(&staged_n, (unsigned long long)staged);
}

void uring_api_wait_timing_add_delivery(unsigned long long ns) {
    if (!wait_timing_enabled) {
        return;
    }
    add_u(&delivery_ns, ns);
}

void uring_api_wait_timing_dump(void) {
    unsigned long long n;
    unsigned long long empty_n;
    unsigned long long ready_n;

    if (!wait_timing_enabled) {
        return;
    }
    n = load_u(&waits);
    if (n == 0) {
        return;
    }
    empty_n = load_u(&first_reap_empty_n);
    ready_n = load_u(&first_reap_ready_n);
    fprintf(stderr,
            "[uring-wait-timing] waits=%llu peek=%llu timeout=%llu blocking=%llu "
            "flush_us=%.1f "
            "first_reap cq_ready=%llu/%.1fus cq_empty=%llu/%.1fus timeout_or_empty=%llu "
            "extra_peek=%llu/%.1fus staged=%llu build_us=%.1f delivery_us=%.1f\n",
            (unsigned long long)n, (unsigned long long)load_u(&waits_peek), (unsigned long long)load_u(&waits_timeout),
            (unsigned long long)load_u(&waits_blocking), (double)load_u(&flush_ns) / 1000.0,
            (unsigned long long)ready_n, (double)load_u(&first_reap_ready_ns) / 1000.0, (unsigned long long)empty_n,
            (double)load_u(&first_reap_empty_ns) / 1000.0, (unsigned long long)load_u(&first_reap_timeout_n),
            (unsigned long long)load_u(&extra_peek_n), (double)load_u(&extra_peek_ns) / 1000.0,
            (unsigned long long)load_u(&staged_n), (double)load_u(&build_ns) / 1000.0,
            (double)load_u(&delivery_ns) / 1000.0);
    fflush(stderr);
}
