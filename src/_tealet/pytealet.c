
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "Python.h"
#include "structmember.h"
#include "frameobject.h"
#include "pythread.h"

#include "tealet.h"

/* Debug logging - set to 1 to enable, 0 to disable */
#define TEALET_DEBUG 0

#if TEALET_DEBUG
#define LOG(...) fprintf(stderr, "[TEALET] " __VA_ARGS__)
#else
#define LOG(...) do {} while(0)
#endif

/* Python minor-version helpers for readable version-specific conditionals. */
#if PY_VERSION_HEX >= 0x030A0000 && PY_VERSION_HEX < 0x030B0000
#define PY310 1
#endif

#if PY_VERSION_HEX >= 0x030B0000 && PY_VERSION_HEX < 0x030C0000
#define PY311 1
#endif

#if PY_VERSION_HEX >= 0x030C0000 && PY_VERSION_HEX < 0x030D0000
#define PY312 1
#endif

#if PY_VERSION_HEX >= 0x030D0000 && PY_VERSION_HEX < 0x030E0000
#define PY313 1
#endif

#if PY_VERSION_HEX >= 0x030E0000 && PY_VERSION_HEX < 0x030F0000
#define PY314 1
#endif

#if PY_VERSION_HEX >= 0x030F0000 && PY_VERSION_HEX < 0x03100000
#define PY315 1
#endif

#if defined(PY310) || defined(PY311) || defined(PY312)
#define PY_HAS_CFRAME
#endif


/****************************************************************
 *Implement copyable stubs by using a trampoline
 */
struct stub_arg
{
    tealet_t *current;
    tealet_run_t run;
    void *runarg;
};

static tealet_t *
stub_main(tealet_t *current, void *arg)
{
    void *myarg = 0;
    void *my_stack_far;
    ptrdiff_t boundary_check;
    
    fprintf(stderr, "[stub_main] ENTERED - current=%p, arg=%p\n", (void*)current, arg);
    
    /* DEBUG: Verify that stack_far boundary includes our local variables */
    my_stack_far = tealet_get_far(current);
    boundary_check = tealet_stack_diff(my_stack_far, (void*)&myarg);
    fprintf(stderr, "[stub_main] &myarg=%p, stack_far=%p, diff=%td bytes\n", 
            (void*)&myarg, my_stack_far, boundary_check);
    
    if (boundary_check < 0) {
        fprintf(stderr, "\n*** STACK_FAR BOUNDARY TOO CLOSE! ***\n");
        fprintf(stderr, "  stub_main local variable &myarg: %p\n", (void*)&myarg);
        fprintf(stderr, "  tealet stack_far boundary:       %p\n", my_stack_far);
        fprintf(stderr, "  Difference (should be positive): %td bytes\n", boundary_check);
        fprintf(stderr, "  This means stack_far does NOT include stub_main's local variables!\n");
        fprintf(stderr, "  When this tealet's stack is saved, local variables will be lost.\n\n");
    }

    /* the caller is in arg, return right back to him */
    tealet_switch((tealet_t*)arg, &myarg);
    /* now we are back, myarg should contain the arg to the run function.
     * We were possibly duplicated, so can't trust the original function args.
     */
    {
        struct stub_arg sarg = *(struct stub_arg*)myarg;
        tealet_free(sarg.current, myarg);
        return (sarg.run)(sarg.current, sarg.runarg);
    }
}

/* create a stub and return it */
static tealet_t *
stub_new(tealet_t *t) {
    void *arg = (void*)tealet_current(t);
	return tealet_new(t, stub_main, &arg, NULL);
}

/* run a stub */
static int
stub_run(tealet_t *stub, tealet_run_t run, void **parg)
{
    int result;
    void *myarg;
    /* we cannot pass arguments to a different tealet on the stack */
    struct stub_arg *psarg = (struct stub_arg*)tealet_malloc(stub, sizeof(struct stub_arg));
    if (!psarg)
        return TEALET_ERR_MEM;
    psarg->current = stub;
    psarg->run = run;
    psarg->runarg = parg ? *parg : NULL;
    myarg = (void*)psarg;
    result = tealet_switch(stub, &myarg);
    if (result) {
        /* failure */
        tealet_free(stub, psarg);
        return result;
    }
    /* pass back the arg value from the switch */
    if (parg)
        *parg = myarg;
    return 0;
}
/***************************************************************/


#define STATE_NEW 0
#define STATE_STUB 1
#define STATE_RUN 2
#define STATE_EXIT 3

static PyTypeObject PyTealetType;
#define PyTealet_Check(op) PyObject_TypeCheck(op, &PyTealetType)
#define PyTealet_CheckExact(op) (Py_TYPE(op) == &PyTealetType)

static int tls_key;

/* Forward declaration */
typedef struct PyTealetObject PyTealetObject;

typedef struct tealet_new_arg {
	int stub;
	PyTealetObject *dest;
	PyObject *func;
	PyObject *arg;
} tealet_new_arg;

/* the structure we associate with the main tealet */
typedef struct main_data
{
	long tid;
	tealet_new_arg new_arg;
	PyObject *dustbin[3];  /* make this a list eventually */
} main_data;
#if defined(PY_HAS_CFRAME)
static void dbg_capture_saved_main_window(const char *phase, PyTealetObject *owner);
static void dbg_compare_saved_main_window(const char *phase, PyTealetObject *owner);
#endif

/* Extra data stored with each tealet for the Python binding.
 * This structure is stored in tealet->extra and provides type-safe
 * access to the associated PyTealetObject.
 */
typedef struct tealet_extra_t {
	PyTealetObject *pytealet;
} tealet_extra_t;

/* Helper macros for type-safe access to the tealet extra data */
#define TEALET_EXTRA(t) ((tealet_extra_t*)(t)->extra)
#define TEALET_PYOBJECT(t) (TEALET_EXTRA(t)->pytealet)
#define TEALET_SET_PYOBJECT(t, obj) (TEALET_EXTRA(t)->pytealet = (obj))


/* a structure that captures the tstate of a tealet.  The fields stored
 * and their semantics may change from python version to version.
 */
struct PyTealetTstate {
	PyFrameObject *frame;
	PyObject *exc_type;
	PyObject *exc_val;
	PyObject *exc_tb;
	_PyErr_StackItem *exc_info;
	_PyErr_StackItem exc_state;
	int recursion_depth;
	int trash_delete_nesting;
	PyObject *context; /* Python 3.7+ contextvars */
	int has_state; /* Debug helper: 1 when this struct currently stores a saved tstate */
	int own_refs; /* has ownership of tstate references been claimed? */
	void *stack_near_saved; /* Debug: stack-near marker captured with this tstate */
	void *stack_far_saved;  /* Debug: far boundary captured with this tstate */
	/* Python 3.10-3.12: cframe tracks C-level call frames (removed in 3.13)
	 * Stack-slicing preserves the CFrame struct itself; we just save the pointer */
#if defined(PY_HAS_CFRAME)
	CFrame* cframe;
	CFrame* cframe_owned; /* CFrame pointer owned by tealet stack while running */
#endif
};

typedef struct PyTealetTstate PyTealetTstate;

#if defined(PY_HAS_CFRAME)
#if TEALET_PYTEALET_ENABLE_STACK_DIAGNOSTICS
static void
stack_bounds(void *a, void *b, void **lo, void **hi)
{
	if (!a || !b) {
		*lo = NULL;
		*hi = NULL;
		return;
	}
	if ((char *)a <= (char *)b) {
		*lo = a;
		*hi = b;
	} else {
		*lo = b;
		*hi = a;
	}
}

static int
stack_contains_ptr(void *near_sp, void *far_sp, void *ptr)
{
	void *lo;
	void *hi;
	if (!near_sp || !far_sp || !ptr)
		return -1;
	stack_bounds(near_sp, far_sp, &lo, &hi);
	return ((char *)ptr >= (char *)lo && (char *)ptr <= (char *)hi) ? 1 : 0;
}

static int
stack_contains_obj(void *near_sp, void *far_sp, void *obj, size_t obj_size)
{
	void *lo;
	void *hi;
	char *obj_lo;
	char *obj_hi;
	if (!near_sp || !far_sp || !obj || obj_size == 0)
		return -1;
	stack_bounds(near_sp, far_sp, &lo, &hi);
	obj_lo = (char *)obj;
	obj_hi = obj_lo + obj_size - 1;
	return (obj_lo >= (char *)lo && obj_hi <= (char *)hi) ? 1 : 0;
}

static void *
diff_to_ptr(ptrdiff_t diff)
{
	return (void *)(uintptr_t)diff;
}

static uint64_t
fnv1a64_update(uint64_t hash, const void *data, size_t size)
{
	const unsigned char *p = (const unsigned char *)data;
	size_t i;
	for (i = 0; i < size; i++) {
		hash ^= (uint64_t)p[i];
		hash *= 1099511628211ULL;
	}
	return hash;
}

enum {
	CFRAME_BAD_NONE = 0,
	CFRAME_BAD_OUT_OF_BOUNDS_OBJ = 1,
	CFRAME_BAD_NEAR_JUMP = 2,
	CFRAME_BAD_USE_TRACING = 3,
	CFRAME_BAD_SELF_LOOP = 4,
	CFRAME_BAD_MAX_DEPTH = 5,
	CFRAME_BAD_ANCHOR_NOT_REACHED = 6,
};

static const char *
cframe_bad_reason_name(int reason)
{
	switch (reason) {
	case CFRAME_BAD_NONE:
		return "none";
	case CFRAME_BAD_OUT_OF_BOUNDS_OBJ:
		return "out_of_bounds_obj";
	case CFRAME_BAD_NEAR_JUMP:
		return "near_jump";
	case CFRAME_BAD_USE_TRACING:
		return "bad_use_tracing";
	case CFRAME_BAD_SELF_LOOP:
		return "self_loop";
	case CFRAME_BAD_MAX_DEPTH:
		return "max_depth";
	case CFRAME_BAD_ANCHOR_NOT_REACHED:
		return "anchor_not_reached";
	default:
		return "unknown";
	}
}

static uint64_t
cframe_chain_hash(CFrame *head,
			 void *near_sp,
			 void *far_sp,
			 CFrame *stop_at,
			 CFrame *root_cframe,
			 int max_depth,
			 int *out_depth,
			 int *out_valid,
			 int *out_reached_stop,
			 int *out_first_bad_reason,
			 CFrame **out_first_bad_frame,
			 CFrame **out_stop)
{
	uint64_t hash = 1469598103934665603ULL;
	int depth = 0;
	int valid = 1;
	int reached_stop = 0;
	int first_bad_reason = CFRAME_BAD_NONE;
	CFrame *first_bad_frame = NULL;
	CFrame *frame = head;

	while (frame && depth < max_depth) {
		if (root_cframe && frame == root_cframe) {
			hash = fnv1a64_update(hash, frame, sizeof(*frame));
			depth++;
			if (stop_at && frame == stop_at)
				reached_stop = 1;
			break;
		}
		ptrdiff_t d_near = tealet_stack_diff(near_sp, (void *)frame);
		int in_obj = stack_contains_obj(near_sp, far_sp, (void *)frame, sizeof(*frame));
		if (in_obj != 1) {
			valid = 0;
			first_bad_reason = CFRAME_BAD_OUT_OF_BOUNDS_OBJ;
			first_bad_frame = frame;
			break;
		}
		if (depth > 0 && (d_near > 65536 || d_near < -(8 * 1024 * 1024))) {
			valid = 0;
			first_bad_reason = CFRAME_BAD_NEAR_JUMP;
			first_bad_frame = frame;
			break;
		}
		if (frame->use_tracing < 0 || frame->use_tracing > 2) {
			valid = 0;
			first_bad_reason = CFRAME_BAD_USE_TRACING;
			first_bad_frame = frame;
			break;
		}
		hash = fnv1a64_update(hash, frame, sizeof(*frame));
		depth++;
		if (stop_at && frame == stop_at) {
			reached_stop = 1;
			break;
		}
		if (frame->previous == NULL)
			break;
		if (frame->previous == frame) {
			valid = 0;
			first_bad_reason = CFRAME_BAD_SELF_LOOP;
			first_bad_frame = frame;
			break;
		}
		frame = frame->previous;
	}
	if (frame && depth >= max_depth) {
		valid = 0;
		first_bad_reason = CFRAME_BAD_MAX_DEPTH;
		first_bad_frame = frame;
	}
	if (stop_at && !reached_stop) {
		valid = 0;
		if (first_bad_reason == CFRAME_BAD_NONE) {
			first_bad_reason = CFRAME_BAD_ANCHOR_NOT_REACHED;
			first_bad_frame = frame;
		}
	}
	if (out_depth)
		*out_depth = depth;
	if (out_valid)
		*out_valid = valid;
	if (out_reached_stop)
		*out_reached_stop = reached_stop;
	if (out_first_bad_reason)
		*out_first_bad_reason = first_bad_reason;
	if (out_first_bad_frame)
		*out_first_bad_frame = first_bad_frame;
	if (out_stop)
		*out_stop = frame;
	return hash;
}

static void
log_cframe_chain(const char *tag, void *owner, void *far_sp, PyThreadState *tstate)
{
	int depth = 0;
	char stack_marker;
	void *near_sp = (void *)&stack_marker;
	CFrame *frame = tstate ? tstate->cframe : NULL;

	fprintf(stderr,
		"[CFRAME] %s owner=%p tstate=%p cframe=%p near=%p far=%p\n",
		tag,
		(void *)owner,
		(void *)tstate,
		(void *)(tstate ? tstate->cframe : NULL),
		near_sp,
		far_sp);

	while (frame && depth < 64) {
		ptrdiff_t d_near = tealet_stack_diff(near_sp, (void *)frame);
		ptrdiff_t d_far = far_sp ? tealet_stack_diff(far_sp, (void *)frame) : 0;
		int in_live = stack_contains_ptr(near_sp, far_sp, (void *)frame);
		int in_live_obj = stack_contains_obj(near_sp, far_sp, (void *)frame, sizeof(*frame));
		if (depth > 0 && (d_near > 65536 || d_near < -(8 * 1024 * 1024))) {
			fprintf(stderr,
				"[CFRAME]   #%02d frame=%p d_near=%p d_far=%p in_live=%d in_live_obj=%d OUT_OF_RANGE (stop)\n",
				depth,
				(void *)frame,
				diff_to_ptr(d_near),
				diff_to_ptr(d_far),
				in_live,
				in_live_obj);
			break;
		}
		int is_root = (tstate && frame == &tstate->root_cframe);
		fprintf(stderr,
			"[CFRAME]   #%02d frame=%p prev=%p use_tracing=%d d_near=%p d_far=%p in_live=%d in_live_obj=%d%s\n",
			depth,
			(void *)frame,
			(void *)frame->previous,
			frame->use_tracing,
			diff_to_ptr(d_near),
			diff_to_ptr(d_far),
			in_live,
			in_live_obj,
			is_root ? " ROOT" : "");
		if (is_root)
			break;
		frame = frame->previous;
		depth++;
	}
	if (frame) {
		fprintf(stderr,
			"[CFRAME]   ... truncated after %d links, next=%p\n",
			depth,
			(void *)frame);
	}
}
#endif
#endif

#if defined(PY_HAS_CFRAME)
#if !TEALET_PYTEALET_ENABLE_STACK_DIAGNOSTICS
static void
stack_bounds(void *a, void *b, void **lo, void **hi)
{
	if (!a || !b) {
		*lo = NULL;
		*hi = NULL;
		return;
	}
	if ((char *)a <= (char *)b) {
		*lo = a;
		*hi = b;
	} else {
		*lo = b;
		*hi = a;
	}
}

static uint64_t
fnv1a64_update(uint64_t hash, const void *data, size_t size)
{
	const unsigned char *p = (const unsigned char *)data;
	size_t i;
	for (i = 0; i < size; i++) {
		hash ^= (uint64_t)p[i];
		hash *= 1099511628211ULL;
	}
	return hash;
}
#endif
#endif






/* The python tealet object */
struct PyTealetObject {
    PyObject_HEAD
    int state;
	tealet_t *tealet;
	PyObject *weakreflist; /* List of weak references */
	long dbg_id;
	struct PyTealetObject *dbg_prev;
	struct PyTealetObject *dbg_next;
	void *dbg_last_head;
	void *dbg_anchor_cframe;
	void *dbg_anchor_near;
	void *dbg_anchor_far;
	int dbg_has_anchor;
	void *dbg_last_saved_cframe;
	void *dbg_last_saved_near;
	void *dbg_last_saved_far;
	void *dbg_last_live_far;
	uint64_t dbg_last_chain_hash;
	int dbg_last_chain_depth;
	int dbg_last_chain_valid;
	int dbg_last_state;
	int dbg_last_has_state;
	int dbg_last_own_refs;
	int dbg_has_last_snapshot;
	int dbg_saved_window_has_snapshot;
	void *dbg_saved_window_addr;
	size_t dbg_saved_window_len;
	uint64_t dbg_saved_window_hash;
	int dbg_saved_heap_has_snapshot;
	void *dbg_saved_heap_addr;
	size_t dbg_saved_heap_len;
	uint64_t dbg_saved_heap_hash;

	/* thread state information */
	PyTealetTstate tstate;
};

static PyTealetObject *dbg_tealet_head = NULL;
static long dbg_tealet_counter = 0;
static int dbg_cframe_failfast_enabled = -1;
static int dbg_cframe_failfast_any_enabled = -1;
static int dbg_main_window_watch_enabled = -1;
static int dbg_main_window_abort_enabled = -1;
static ptrdiff_t dbg_main_window_radius = 512;
static int dbg_saved_heap_watch_enabled = -1;
static int dbg_saved_heap_abort_enabled = -1;

#define DBG_HEAP_MAGIC_HEAD 0xC0FFEEBAD5EED001ULL
#define DBG_HEAP_MAGIC_TAIL 0xB16B00B5DEADC0DEULL

typedef struct dbg_heap_block {
	uint64_t magic_head;
	size_t payload_size;
	struct dbg_heap_block *prev;
	struct dbg_heap_block *next;
} dbg_heap_block;

static dbg_heap_block *dbg_heap_blocks_head = NULL;

static int
dbg_heap_range_in_single_block(void *range_lo,
			       void *range_hi,
			       void **out_payload_lo,
			       size_t *out_payload_size)
{
	dbg_heap_block *iter;
	if (!range_lo || !range_hi)
		return 0;
	for (iter = dbg_heap_blocks_head; iter; iter = iter->next) {
		char *payload_lo = (char *)(iter + 1);
		char *payload_hi = payload_lo + iter->payload_size - 1;
		if ((char *)range_lo >= payload_lo && (char *)range_hi <= payload_hi) {
			if (out_payload_lo)
				*out_payload_lo = (void *)payload_lo;
			if (out_payload_size)
				*out_payload_size = iter->payload_size;
			return 1;
		}
	}
	return 0;
}

static void
dbg_register_tealet(PyTealetObject *obj)
{
	if (!obj || obj->dbg_id != 0)
		return;
	obj->dbg_id = ++dbg_tealet_counter;
	obj->dbg_prev = NULL;
	obj->dbg_next = dbg_tealet_head;
	obj->dbg_last_head = NULL;
	obj->dbg_anchor_cframe = NULL;
	obj->dbg_anchor_near = NULL;
	obj->dbg_anchor_far = NULL;
	obj->dbg_has_anchor = 0;
	obj->dbg_last_saved_cframe = NULL;
	obj->dbg_last_saved_near = NULL;
	obj->dbg_last_saved_far = NULL;
	obj->dbg_last_live_far = NULL;
	obj->dbg_last_chain_hash = 0;
	obj->dbg_last_chain_depth = -1;
	obj->dbg_last_chain_valid = -1;
	obj->dbg_last_state = -1;
	obj->dbg_last_has_state = -1;
	obj->dbg_last_own_refs = -1;
	obj->dbg_has_last_snapshot = 0;
	obj->dbg_saved_window_has_snapshot = 0;
	obj->dbg_saved_window_addr = NULL;
	obj->dbg_saved_window_len = 0;
	obj->dbg_saved_window_hash = 0;
	obj->dbg_saved_heap_has_snapshot = 0;
	obj->dbg_saved_heap_addr = NULL;
	obj->dbg_saved_heap_len = 0;
	obj->dbg_saved_heap_hash = 0;
	if (dbg_tealet_head)
		dbg_tealet_head->dbg_prev = obj;
	dbg_tealet_head = obj;
}

static void
dbg_unregister_tealet(PyTealetObject *obj)
{
	if (!obj || obj->dbg_id == 0)
		return;
	if (obj->dbg_prev)
		obj->dbg_prev->dbg_next = obj->dbg_next;
	else if (dbg_tealet_head == obj)
		dbg_tealet_head = obj->dbg_next;
	if (obj->dbg_next)
		obj->dbg_next->dbg_prev = obj->dbg_prev;
	obj->dbg_prev = obj->dbg_next = NULL;
	obj->dbg_last_head = NULL;
	obj->dbg_anchor_cframe = NULL;
	obj->dbg_anchor_near = NULL;
	obj->dbg_anchor_far = NULL;
	obj->dbg_has_anchor = 0;
	obj->dbg_last_saved_cframe = NULL;
	obj->dbg_last_saved_near = NULL;
	obj->dbg_last_saved_far = NULL;
	obj->dbg_last_live_far = NULL;
	obj->dbg_last_chain_hash = 0;
	obj->dbg_last_chain_depth = -1;
	obj->dbg_last_chain_valid = -1;
	obj->dbg_last_state = -1;
	obj->dbg_last_has_state = -1;
	obj->dbg_last_own_refs = -1;
	obj->dbg_has_last_snapshot = 0;
	obj->dbg_saved_window_has_snapshot = 0;
	obj->dbg_saved_window_addr = NULL;
	obj->dbg_saved_window_len = 0;
	obj->dbg_saved_window_hash = 0;
	obj->dbg_saved_heap_has_snapshot = 0;
	obj->dbg_saved_heap_addr = NULL;
	obj->dbg_saved_heap_len = 0;
	obj->dbg_saved_heap_hash = 0;
	obj->dbg_id = 0;
}

static PyTealetObject *
dbg_find_tealet_obj(tealet_t *tealet)
{
	PyTealetObject *iter;
	if (!tealet)
		return NULL;
	for (iter = dbg_tealet_head; iter; iter = iter->dbg_next) {
		if (iter->tealet == tealet)
			return iter;
	}
	return NULL;
}

static void
dbg_init_main_window_watch_config(void)
{
	if (dbg_main_window_watch_enabled < 0) {
		const char *env = getenv("PYTEALET_MAIN_WINDOW_WATCH");
		dbg_main_window_watch_enabled = (!env || !*env || *env != '0') ? 1 : 0;
	}
	if (dbg_main_window_abort_enabled < 0) {
		const char *env = getenv("PYTEALET_MAIN_WINDOW_ABORT");
		dbg_main_window_abort_enabled = (!env || !*env || *env != '0') ? 1 : 0;
	}
	if (dbg_main_window_radius == 512) {
		const char *env = getenv("PYTEALET_MAIN_WINDOW_RADIUS");
		if (env && *env) {
			char *endp;
			long long parsed = strtoll(env, &endp, 10);
			if (endp != env && (!endp || *endp == '\0') && parsed >= 0)
				dbg_main_window_radius = (ptrdiff_t)parsed;
		}
	}
}

static void
dbg_init_saved_heap_watch_config(void)
{
	if (dbg_saved_heap_watch_enabled < 0) {
		const char *env = getenv("PYTEALET_HEAP_SAVED_WATCH");
		dbg_saved_heap_watch_enabled = (!env || !*env || *env != '0') ? 1 : 0;
	}
	if (dbg_saved_heap_abort_enabled < 0) {
		const char *env = getenv("PYTEALET_HEAP_SAVED_ABORT");
		dbg_saved_heap_abort_enabled = (!env || !*env || *env != '0') ? 1 : 0;
	}
}

static void
dbg_maybe_capture_anchor(const char *phase, PyTealetObject *owner, PyThreadState *tstate)
{
	char stack_marker;
	void *near_sp = (void *)&stack_marker;
	void *far_sp;
	int in_live_obj = -1;
	if (!owner || !tstate || !owner->tealet)
		return;
	if (owner->dbg_has_anchor)
		return;
	far_sp = tealet_get_far(owner->tealet);
	owner->dbg_anchor_cframe = (void *)tstate->cframe;
	owner->dbg_anchor_near = near_sp;
	owner->dbg_anchor_far = far_sp;
	owner->dbg_has_anchor = 1;
	#if TEALET_PYTEALET_ENABLE_STACK_DIAGNOSTICS
	in_live_obj = stack_contains_obj(near_sp,
					 far_sp,
					 owner->dbg_anchor_cframe,
					 sizeof(CFrame));
	#endif
	fprintf(stderr,
		"[TEALET_ANCHOR] phase=%s id=%ld obj=%p tealet=%p anchor=%p near=%p far=%p in_live_obj=%d\n",
		phase,
		owner->dbg_id,
		(void *)owner,
		(void *)owner->tealet,
		owner->dbg_anchor_cframe,
		owner->dbg_anchor_near,
		owner->dbg_anchor_far,
		in_live_obj);
}

static void
dbg_failfast_validate_active_cframe(const char *phase, PyTealetObject *owner, PyThreadState *tstate)
{
	#if defined(PY_HAS_CFRAME) && TEALET_PYTEALET_ENABLE_STACK_DIAGNOSTICS
	char stack_marker;
	void *near_sp = (void *)&stack_marker;
	void *far_sp = (owner && owner->tealet) ? tealet_get_far(owner->tealet) : NULL;
	CFrame *head;
	CFrame *anchor;
	CFrame *chain_first_bad_frame = NULL;
	CFrame *chain_stop = NULL;
	int chain_depth = 0;
	int chain_valid = 1;
	int chain_reached_anchor = 0;
	int chain_first_bad_reason = CFRAME_BAD_NONE;
	uint64_t chain_hash;

	if (!owner || !tstate)
		return;
	if (dbg_cframe_failfast_enabled < 0) {
		const char *env = getenv("PYTEALET_CFRAME_FAILFAST");
		dbg_cframe_failfast_enabled = (env && *env && *env != '0') ? 1 : 0;
	}
	if (!dbg_cframe_failfast_enabled)
		return;
	if (!phase || strstr(phase, "after-restore") == NULL)
		return;

	head = tstate->cframe;
	if (!head)
		return;
	anchor = owner->dbg_has_anchor ? (CFrame *)owner->dbg_anchor_cframe : NULL;
	chain_hash = cframe_chain_hash(head,
				       near_sp,
				       far_sp,
				       anchor,
				       &tstate->root_cframe,
				       64,
				       &chain_depth,
				       &chain_valid,
				       &chain_reached_anchor,
				       &chain_first_bad_reason,
				       &chain_first_bad_frame,
				       &chain_stop);

	if (!chain_valid && chain_first_bad_reason != CFRAME_BAD_ANCHOR_NOT_REACHED && chain_first_bad_reason != CFRAME_BAD_MAX_DEPTH) {
		fprintf(stderr,
			"[CFRAME_FAILFAST] phase=%s owner_id=%ld owner=%p tealet=%p is_main=%d state=%d current=%p tstate=%p head=%p near=%p far=%p anchor=%p chain_hash=0x%016llx chain_depth=%d chain_valid=%d chain_reached_anchor=%d bad_reason=%s bad_frame=%p chain_stop=%p action=abort\n",
			phase ? phase : "unknown",
			owner->dbg_id,
			(void *)owner,
			(void *)owner->tealet,
			(owner->tealet && TEALET_IS_MAIN(owner->tealet)) ? 1 : 0,
			owner->state,
			(void *)(owner->tealet && owner->tealet->main ? tealet_current(owner->tealet->main) : NULL),
			(void *)tstate,
			(void *)head,
			near_sp,
			far_sp,
			(void *)anchor,
			(unsigned long long)chain_hash,
			chain_depth,
			chain_valid,
			chain_reached_anchor,
			cframe_bad_reason_name(chain_first_bad_reason),
			(void *)chain_first_bad_frame,
			(void *)chain_stop);
		abort();
	}
	#else
	(void)phase;
	(void)owner;
	(void)tstate;
	#endif
}

#if defined(PY_HAS_CFRAME)
#if TEALET_PYTEALET_ENABLE_STACK_DIAGNOSTICS
static void
log_cframe_chain_short(const char *phase,
			      const char *role,
			      PyTealetObject *owner,
			      CFrame *head,
			      void *near_sp)
{
	int depth = 0;
	int max_depth = 10;
	CFrame *orig_head = head;
	void *far_sp = (owner && owner->tealet) ? tealet_get_far(owner->tealet) : NULL;
	void *live_lo;
	void *live_hi;
	void *saved_lo;
	void *saved_hi;
	stack_bounds(near_sp, far_sp, &live_lo, &live_hi);
	stack_bounds(owner ? owner->tstate.stack_near_saved : NULL,
			 owner ? owner->tstate.stack_far_saved : NULL,
			 &saved_lo,
			 &saved_hi);
	if (strcmp(role, "next-saved") == 0) {
		fprintf(stderr,
			"[CFRAME_SW] phase=%s role=%s owner=%p tealet=%p state=%d has_state=%d saved_cframe=%p head=%p near=%p far=%p live_lo=%p live_hi=%p saved_near=%p saved_far=%p saved_lo=%p saved_hi=%p chain_walk=skipped reason=not_active\n",
			phase,
			role,
			(void *)owner,
			(void *)(owner ? owner->tealet : NULL),
			owner ? owner->state : -1,
			owner ? owner->tstate.has_state : -1,
			(void *)(owner ? owner->tstate.cframe : NULL),
			(void *)head,
			near_sp,
			far_sp,
			live_lo,
			live_hi,
			owner ? owner->tstate.stack_near_saved : NULL,
			owner ? owner->tstate.stack_far_saved : NULL,
			saved_lo,
			saved_hi);
		return;
	}

	fprintf(stderr,
		"[CFRAME_SW] phase=%s role=%s owner=%p tealet=%p state=%d has_state=%d saved_cframe=%p head=%p near=%p far=%p live_lo=%p live_hi=%p saved_near=%p saved_far=%p saved_lo=%p saved_hi=%p\n",
		phase,
		role,
		(void *)owner,
		(void *)(owner ? owner->tealet : NULL),
		owner ? owner->state : -1,
		owner ? owner->tstate.has_state : -1,
		(void *)(owner ? owner->tstate.cframe : NULL),
		(void *)head,
		near_sp,
		far_sp,
		live_lo,
		live_hi,
		owner ? owner->tstate.stack_near_saved : NULL,
		owner ? owner->tstate.stack_far_saved : NULL,
		saved_lo,
		saved_hi);

	while (head && depth < max_depth) {
		ptrdiff_t d_near = tealet_stack_diff(near_sp, (void *)head);
		ptrdiff_t d_far = far_sp ? tealet_stack_diff(far_sp, (void *)head) : 0;
		ptrdiff_t d_saved_near = (owner && owner->tstate.stack_near_saved)
			? tealet_stack_diff(owner->tstate.stack_near_saved, (void *)head)
			: 0;
		ptrdiff_t d_saved_far = (owner && owner->tstate.stack_far_saved)
			? tealet_stack_diff(owner->tstate.stack_far_saved, (void *)head)
			: 0;
		int in_live = stack_contains_ptr(near_sp, far_sp, (void *)head);
		int in_live_obj = stack_contains_obj(near_sp, far_sp, (void *)head, sizeof(*head));
		int in_saved = stack_contains_ptr(owner ? owner->tstate.stack_near_saved : NULL,
						 owner ? owner->tstate.stack_far_saved : NULL,
						 (void *)head);
		int in_saved_obj = stack_contains_obj(owner ? owner->tstate.stack_near_saved : NULL,
						     owner ? owner->tstate.stack_far_saved : NULL,
						     (void *)head,
						     sizeof(*head));
		int bogus = 0;

		if (depth > 0 && (d_near > 65536 || d_near < -(8 * 1024 * 1024)))
			bogus = 1;
		if (head->use_tracing < 0 || head->use_tracing > 2)
			bogus = 1;

		fprintf(stderr,
			"[CFRAME_SW]   #%02d frame=%p prev=%p use_tracing=%d d_near=%p d_far=%p d_saved_near=%p d_saved_far=%p in_live=%d in_live_obj=%d in_saved=%d in_saved_obj=%d%s\n",
			depth,
			(void *)head,
			(void *)head->previous,
			head->use_tracing,
			diff_to_ptr(d_near),
			diff_to_ptr(d_far),
			diff_to_ptr(d_saved_near),
			diff_to_ptr(d_saved_far),
			in_live,
			in_live_obj,
			in_saved,
			in_saved_obj,
			bogus ? " BOGUS" : "");

		if (bogus || head->previous == NULL || head->previous == head)
			break;

		head = head->previous;
		depth++;
	}

	if (head && depth >= max_depth) {
		fprintf(stderr,
			"[CFRAME_SW]   ... truncated after %d links, next=%p\n",
			max_depth,
			(void *)head);
	}
	{
		int chain_depth = 0;
		int chain_valid = -1;
		int chain_reached_anchor = -1;
		int chain_first_bad_reason = CFRAME_BAD_NONE;
		CFrame *chain_stop = NULL;
		CFrame *chain_first_bad_frame = NULL;
		CFrame *anchor = (owner && owner->dbg_has_anchor)
			? (CFrame *)owner->dbg_anchor_cframe
			: NULL;
		uint64_t chain_hash = cframe_chain_hash(orig_head,
						      near_sp,
						      far_sp,
						      anchor,
						      NULL,
						      64,
						      &chain_depth,
						      &chain_valid,
						      &chain_reached_anchor,
						      &chain_first_bad_reason,
						      &chain_first_bad_frame,
						      &chain_stop);
		fprintf(stderr,
			"[CFRAME_SW]   chain_hash=0x%016llx chain_depth=%d chain_valid=%d chain_reached_anchor=%d anchor=%p chain_first_bad_frame=%p chain_first_bad_reason=%s chain_stop=%p\n",
			(unsigned long long)chain_hash,
			chain_depth,
			chain_valid,
			chain_reached_anchor,
			(void *)anchor,
			(void *)chain_first_bad_frame,
			cframe_bad_reason_name(chain_first_bad_reason),
			(void *)chain_stop);
	}
}

static void
log_switch_cframes(const char *phase,
		   PyTealetObject *prev_owner,
		   PyTealetObject *next_owner,
		   PyThreadState *tstate)
{
	char stack_marker;
	void *near_sp = (void *)&stack_marker;
	void *saved_lo;
	void *saved_hi;
	CFrame *active_head = tstate ? tstate->cframe : NULL;
	CFrame *next_saved_head = (next_owner && next_owner->tstate.has_state)
		? next_owner->tstate.cframe
		: NULL;
	tealet_t *active_tealet = NULL;
	PyTealetObject *active_owner = NULL;
	PyTealetObject *iter;

	dbg_init_saved_heap_watch_config();
	if (prev_owner && prev_owner->tealet && prev_owner->tealet->main)
		active_tealet = tealet_current(prev_owner->tealet->main);
	active_owner = dbg_find_tealet_obj(active_tealet);

	fprintf(stderr,
		"[TEALET_DUMP] phase=%s total_head=%p prev_owner=%p next_owner=%p active_owner=%p active_tealet=%p active_cframe=%p\n",
		phase,
		(void *)dbg_tealet_head,
		(void *)prev_owner,
		(void *)next_owner,
		(void *)active_owner,
		(void *)active_tealet,
		(void *)active_head);
	for (iter = dbg_tealet_head; iter; iter = iter->dbg_next) {
		void *live_far = iter->tealet ? tealet_get_far(iter->tealet) : NULL;
		CFrame *chosen_head = NULL;
		const char *head_source = "none";
		int is_uninvolved_parked;
		int has_saved_blob = 0;
		int saved_blob_is_heap = 0;
		void *saved_blob_addr = NULL;
		size_t saved_blob_len = 0;
		void *saved_payload_lo = NULL;
		size_t saved_payload_size = 0;
		uint64_t saved_blob_hash = 0;
		int saved_blob_mutated = 0;
		ptrdiff_t d_saved_near = 0;
		ptrdiff_t d_saved_far = 0;
		int in_live = -1;
		int in_live_obj = -1;
		int in_saved = -1;
		int in_saved_obj = -1;
		int chain_depth = -1;
		int chain_valid = -1;
		int chain_reached_anchor = -1;
		int chain_first_bad_reason = CFRAME_BAD_NONE;
		CFrame *chain_stop = NULL;
		CFrame *chain_first_bad_frame = NULL;
		CFrame *anchor = (iter->dbg_has_anchor)
			? (CFrame *)iter->dbg_anchor_cframe
			: NULL;
		uint64_t chain_hash = 0;
		int likely_bogus = 0;

		if (iter == active_owner && tstate) {
			chosen_head = tstate->cframe;
			head_source = "active";
		} else if (iter->tstate.has_state) {
			chosen_head = iter->tstate.cframe;
			head_source = "saved";
		}
		if (chosen_head && iter->tstate.stack_near_saved)
			d_saved_near = tealet_stack_diff(iter->tstate.stack_near_saved, (void *)chosen_head);
		if (chosen_head && iter->tstate.stack_far_saved)
			d_saved_far = tealet_stack_diff(iter->tstate.stack_far_saved, (void *)chosen_head);
		if (chosen_head) {
			void *hash_near = (iter == active_owner) ? near_sp : iter->tstate.stack_near_saved;
			void *hash_far = (iter == active_owner) ? live_far : iter->tstate.stack_far_saved;
			in_live = stack_contains_ptr(near_sp, live_far, (void *)chosen_head);
			in_live_obj = stack_contains_obj(near_sp, live_far, (void *)chosen_head, sizeof(*chosen_head));
			in_saved = stack_contains_ptr(iter->tstate.stack_near_saved,
						 iter->tstate.stack_far_saved,
						 (void *)chosen_head);
			in_saved_obj = stack_contains_obj(iter->tstate.stack_near_saved,
						     iter->tstate.stack_far_saved,
						     (void *)chosen_head,
						     sizeof(*chosen_head));
			if (phase && strstr(phase, "after-restore") != NULL &&
			    iter == active_owner && in_live_obj == 1 && iter->tstate.has_state == 0) {
				chain_hash = cframe_chain_hash(chosen_head,
						      hash_near,
						      hash_far,
						      anchor,
						      tstate ? &tstate->root_cframe : NULL,
						      64,
						      &chain_depth,
						      &chain_valid,
						      &chain_reached_anchor,
						      &chain_first_bad_reason,
						      &chain_first_bad_frame,
						      &chain_stop);
				if (dbg_cframe_failfast_any_enabled < 0) {
					const char *env = getenv("PYTEALET_CFRAME_FAILFAST_ANY");
					dbg_cframe_failfast_any_enabled = (env && *env && *env != '0') ? 1 : 0;
				}
				if (dbg_cframe_failfast_any_enabled &&
				    chain_valid == 0 &&
				    chain_first_bad_reason != CFRAME_BAD_ANCHOR_NOT_REACHED &&
				    chain_first_bad_reason != CFRAME_BAD_MAX_DEPTH) {
					fprintf(stderr,
						"[CFRAME_FAILFAST_ANY] phase=%s id=%ld obj=%p tealet=%p state=%d has_state=%d own_refs=%d head=%p head_src=%s anchor=%p chain_hash=0x%016llx chain_depth=%d chain_valid=%d chain_reached_anchor=%d bad_reason=%s bad_frame=%p chain_stop=%p action=abort\n",
						phase,
						iter->dbg_id,
						(void *)iter,
						(void *)iter->tealet,
						iter->state,
						iter->tstate.has_state,
						iter->tstate.own_refs,
						(void *)chosen_head,
						head_source,
						(void *)anchor,
						(unsigned long long)chain_hash,
						chain_depth,
						chain_valid,
						chain_reached_anchor,
						cframe_bad_reason_name(chain_first_bad_reason),
						(void *)chain_first_bad_frame,
						(void *)chain_stop);
					abort();
				}
			}
		}
		if (chosen_head && (d_saved_near > 65536 || d_saved_near < -(8 * 1024 * 1024)))
			likely_bogus = 1;
		if (chosen_head && (chosen_head->use_tracing < 0 || chosen_head->use_tracing > 2))
			likely_bogus = 1;

		is_uninvolved_parked = (iter != prev_owner && iter != next_owner && iter->tstate.has_state);
		if (is_uninvolved_parked &&
		    iter->tstate.has_state &&
		    iter->tstate.stack_near_saved &&
		    iter->tstate.stack_far_saved) {
			stack_bounds(iter->tstate.stack_near_saved,
				    iter->tstate.stack_far_saved,
				    &saved_lo,
				    &saved_hi);
			if (saved_lo && saved_hi && (char *)saved_hi >= (char *)saved_lo) {
				saved_blob_is_heap = dbg_heap_range_in_single_block(saved_lo,
								       saved_hi,
								       &saved_payload_lo,
								       &saved_payload_size);
				if (saved_blob_is_heap) {
					saved_blob_addr = saved_lo;
					saved_blob_len = (size_t)((char *)saved_hi - (char *)saved_lo + 1);
					saved_blob_hash = fnv1a64_update(1469598103934665603ULL,
								   saved_blob_addr,
								   saved_blob_len);
					has_saved_blob = 1;
				}
			}
		}

		if (dbg_saved_heap_watch_enabled &&
		    is_uninvolved_parked &&
		    !has_saved_blob &&
		    iter->tstate.stack_near_saved &&
		    iter->tstate.stack_far_saved) {
			fprintf(stderr,
				"[HEAP_SAVED_WATCH] phase=%s id=%ld obj=%p tealet=%p state=%d has_state=%d own_refs=%d action=skip reason=range_not_heap saved_near=%p saved_far=%p\n",
				phase,
				iter->dbg_id,
				(void *)iter,
				(void *)iter->tealet,
				iter->state,
				iter->tstate.has_state,
				iter->tstate.own_refs,
				iter->tstate.stack_near_saved,
				iter->tstate.stack_far_saved);
		}

		if (dbg_saved_heap_watch_enabled &&
		    is_uninvolved_parked &&
		    has_saved_blob &&
		    iter->dbg_saved_heap_has_snapshot &&
		    iter->dbg_saved_heap_addr == saved_blob_addr &&
		    iter->dbg_saved_heap_len == saved_blob_len &&
		    iter->dbg_saved_heap_hash != saved_blob_hash) {
			saved_blob_mutated = 1;
			fprintf(stderr,
				"[HEAP_SAVED_WATCH] phase=%s id=%ld obj=%p tealet=%p state=%d has_state=%d own_refs=%d saved_blob=[%p,%p] len=%zu saved_payload=[%p,%p] payload_size=%zu old_hash=0x%016llx new_hash=0x%016llx head=%p head_src=%s chain_hash=0x%016llx chain_depth=%d chain_valid=%d bad_reason=%s action=%s\n",
				phase,
				iter->dbg_id,
				(void *)iter,
				(void *)iter->tealet,
				iter->state,
				iter->tstate.has_state,
				iter->tstate.own_refs,
				saved_blob_addr,
				(void *)((char *)saved_blob_addr + saved_blob_len - 1),
				saved_blob_len,
				saved_payload_lo,
				(void *)((char *)saved_payload_lo + saved_payload_size - 1),
				saved_payload_size,
				(unsigned long long)iter->dbg_saved_heap_hash,
				(unsigned long long)saved_blob_hash,
				(void *)chosen_head,
				head_source,
				(unsigned long long)chain_hash,
				chain_depth,
				chain_valid,
				cframe_bad_reason_name(chain_first_bad_reason),
				dbg_saved_heap_abort_enabled ? "abort" : "log");
			if (dbg_saved_heap_abort_enabled)
				abort();
		}

		if (!iter->dbg_has_last_snapshot ||
		    iter->dbg_last_head != (void *)chosen_head ||
		    iter->dbg_last_saved_cframe != (void *)iter->tstate.cframe ||
		    iter->dbg_last_saved_near != iter->tstate.stack_near_saved ||
		    iter->dbg_last_saved_far != iter->tstate.stack_far_saved ||
		    iter->dbg_last_live_far != live_far ||
		    iter->dbg_last_chain_hash != chain_hash ||
		    iter->dbg_last_chain_depth != chain_depth ||
		    iter->dbg_last_chain_valid != chain_valid ||
		    iter->dbg_last_state != iter->state ||
		    iter->dbg_last_has_state != iter->tstate.has_state ||
		    iter->dbg_last_own_refs != iter->tstate.own_refs) {
			fprintf(stderr,
				"[TEALET_DELTA] phase=%s id=%ld obj=%p state:%d->%d has_state:%d->%d own_refs:%d->%d head:%p->%p saved_cframe:%p->%p saved_near:%p->%p saved_far:%p->%p live_far:%p->%p chain_hash:0x%016llx->0x%016llx chain_depth:%d->%d chain_valid:%d->%d\n",
				phase,
				iter->dbg_id,
				(void *)iter,
				iter->dbg_last_state,
				iter->state,
				iter->dbg_last_has_state,
				iter->tstate.has_state,
				iter->dbg_last_own_refs,
				iter->tstate.own_refs,
				iter->dbg_last_head,
				(void *)chosen_head,
				iter->dbg_last_saved_cframe,
				(void *)iter->tstate.cframe,
				iter->dbg_last_saved_near,
				iter->tstate.stack_near_saved,
				iter->dbg_last_saved_far,
				iter->tstate.stack_far_saved,
				iter->dbg_last_live_far,
				live_far,
				(unsigned long long)iter->dbg_last_chain_hash,
				(unsigned long long)chain_hash,
				iter->dbg_last_chain_depth,
				chain_depth,
				iter->dbg_last_chain_valid,
				chain_valid);
		}

		fprintf(stderr,
			"[TEALET_DUMP]   id=%ld obj=%p tealet=%p state=%d flags=%s%s has_state=%d own_refs=%d live_far=%p saved_near=%p saved_far=%p saved_cframe=%p head=%p head_src=%s anchor=%p has_anchor=%d d_saved_near=%p d_saved_far=%p in_live=%d in_live_obj=%d in_saved=%d in_saved_obj=%d chain_hash=0x%016llx chain_depth=%d chain_valid=%d chain_reached_anchor=%d chain_first_bad_frame=%p chain_first_bad_reason=%s chain_stop=%p%s\n",
			iter->dbg_id,
			(void *)iter,
			(void *)iter->tealet,
			iter->state,
			(iter == prev_owner) ? "PREV" : "",
			(iter == next_owner) ? "NEXT" : "",
			iter->tstate.has_state,
			iter->tstate.own_refs,
			live_far,
			iter->tstate.stack_near_saved,
			iter->tstate.stack_far_saved,
			(void *)iter->tstate.cframe,
			(void *)chosen_head,
			head_source,
			iter->dbg_anchor_cframe,
			iter->dbg_has_anchor,
			diff_to_ptr(d_saved_near),
			diff_to_ptr(d_saved_far),
			in_live,
			in_live_obj,
			in_saved,
			in_saved_obj,
			(unsigned long long)chain_hash,
			chain_depth,
			chain_valid,
			chain_reached_anchor,
			(void *)chain_first_bad_frame,
			cframe_bad_reason_name(chain_first_bad_reason),
			(void *)chain_stop,
			likely_bogus ? " BOGUS" : "");

		iter->dbg_last_head = (void *)chosen_head;
		iter->dbg_last_saved_cframe = (void *)iter->tstate.cframe;
		iter->dbg_last_saved_near = iter->tstate.stack_near_saved;
		iter->dbg_last_saved_far = iter->tstate.stack_far_saved;
		iter->dbg_last_live_far = live_far;
		iter->dbg_last_chain_hash = chain_hash;
		iter->dbg_last_chain_depth = chain_depth;
		iter->dbg_last_chain_valid = chain_valid;
		iter->dbg_last_state = iter->state;
		iter->dbg_last_has_state = iter->tstate.has_state;
		iter->dbg_last_own_refs = iter->tstate.own_refs;
		iter->dbg_has_last_snapshot = 1;

		if (!has_saved_blob) {
			iter->dbg_saved_heap_has_snapshot = 0;
			iter->dbg_saved_heap_addr = NULL;
			iter->dbg_saved_heap_len = 0;
			iter->dbg_saved_heap_hash = 0;
		} else if (!saved_blob_mutated || !dbg_saved_heap_abort_enabled) {
			iter->dbg_saved_heap_has_snapshot = 1;
			iter->dbg_saved_heap_addr = saved_blob_addr;
			iter->dbg_saved_heap_len = saved_blob_len;
			iter->dbg_saved_heap_hash = saved_blob_hash;
		}
	}

	log_cframe_chain_short(phase, "prev-active", active_owner, active_head, near_sp);
	log_cframe_chain_short(phase, "next-saved", next_owner, next_saved_head, near_sp);
}
#endif
#endif

/* helpers for getting main and current and checking relationship */
static PyTealetObject *GetMain(int create);
static PyTealetObject *GetCurrent(PyTealetObject *main);
static int CheckTarget(PyTealetObject *target, PyTealetObject *main);

static tealet_t * pytealet_main(tealet_t *t_current, void *arg);

static PyObject *TealetError;
static PyObject *InvalidError;
static PyObject *StateError;
static PyObject *DefunctError;


static void PyTealetTstate_Init(PyTealetTstate *saved)
{
	saved->frame = NULL;
	saved->exc_type = NULL;
	saved->exc_val = NULL;
	saved->exc_tb = NULL;
	saved->exc_info = NULL;
	saved->exc_state.exc_value = NULL;
	saved->recursion_depth = 0;
	saved->trash_delete_nesting = 0;
	saved->context = NULL;
	saved->has_state = 0;
	saved->own_refs = 0;
	saved->stack_near_saved = NULL;
	saved->stack_far_saved = NULL;
#if defined(PY_HAS_CFRAME)
	saved->cframe = NULL;
	saved->cframe_owned = NULL;
#endif
}

/* Copy saved tealet state for object cloning (PyTealet(stub)).
 * This is distinct from switch-time capture/restore move semantics.
 * If source has no saved state, destination remains initialized/empty.
 */
static void PyTealetTstate_CopyForClone(PyTealetTstate *dst, const PyTealetTstate *src)
{
	PyTealetTstate_Init(dst);
	if (!src->has_state)
		return;

	dst->frame = src->frame;
	dst->exc_type = src->exc_type;
	dst->exc_val = src->exc_val;
	dst->exc_tb = src->exc_tb;
	dst->exc_state = src->exc_state;

	/* If source exc_info points to its inline exc_state, remap to destination. */
	if (src->exc_info == &src->exc_state)
		dst->exc_info = &dst->exc_state;
	else
		dst->exc_info = src->exc_info;

	dst->recursion_depth = src->recursion_depth;
	dst->trash_delete_nesting = src->trash_delete_nesting;
	dst->context = src->context;
	dst->stack_near_saved = src->stack_near_saved;
	dst->stack_far_saved = src->stack_far_saved;
#if defined(PY_HAS_CFRAME)
	dst->cframe = src->cframe;
	dst->cframe_owned = src->cframe;
#endif
	dst->has_state = 1;
	dst->own_refs = 0;
}

/* Capture (move) thread-state fields into a tealet tstate struct.
 * Ownership is transferred from PyThreadState to saved.
 */
static void PyTealetTstate_Capture(PyTealetTstate *saved, PyThreadState *py_tstate)
{
	char stack_marker;
	if (!py_tstate)
		py_tstate = PyThreadState_GET();

	assert(saved->has_state == 0);
	saved->frame = py_tstate->frame;
	saved->recursion_depth = py_tstate->recursion_depth;
	py_tstate->frame = NULL;
	py_tstate->recursion_depth = 0;

	saved->exc_type = py_tstate->curexc_type;
	saved->exc_val = py_tstate->curexc_value;
	saved->exc_tb = py_tstate->curexc_traceback;
	py_tstate->curexc_type = py_tstate->curexc_value = py_tstate->curexc_traceback = NULL;

	/* exc_info is borrowed: save/restore pointer value only. */
	saved->exc_info = py_tstate->exc_info;
	saved->exc_state = py_tstate->exc_state;
	py_tstate->exc_info = &py_tstate->exc_state;
	py_tstate->exc_state.exc_value = NULL;

	saved->context = py_tstate->context;
	py_tstate->context = NULL;
	saved->stack_near_saved = (void *)&stack_marker;

#if defined(PY_HAS_CFRAME)
	saved->cframe = py_tstate->cframe;
	saved->cframe_owned = saved->cframe;
#endif
	saved->trash_delete_nesting = py_tstate->trash_delete_nesting;
	py_tstate->trash_delete_nesting = 0;

	saved->has_state = 1;
}

/* Restore (move) previously saved tealet tstate into PyThreadState. */
static void PyTealetTstate_Restore(PyTealetTstate *saved, PyThreadState *py_tstate)
{
	if (!py_tstate)
		py_tstate = PyThreadState_GET();

	assert(saved->has_state == 1);

	assert(py_tstate->frame == NULL);
	assert(py_tstate->recursion_depth == 0);
	assert(py_tstate->context == NULL);

	py_tstate->frame = saved->frame;
	py_tstate->recursion_depth = saved->recursion_depth;

	Py_CLEAR(py_tstate->curexc_type);
	Py_CLEAR(py_tstate->curexc_value);
	Py_CLEAR(py_tstate->curexc_traceback);
	py_tstate->curexc_type = saved->exc_type;
	py_tstate->curexc_value = saved->exc_val;
	py_tstate->curexc_traceback = saved->exc_tb;

	py_tstate->exc_state = saved->exc_state;
	py_tstate->exc_info = saved->exc_info ? saved->exc_info : &py_tstate->exc_state;

	py_tstate->context = saved->context;
	py_tstate->context_ver++;  /* Invalidate contextvars cache */

#if defined(PY_HAS_CFRAME)
	py_tstate->cframe = saved->cframe;
	assert(py_tstate->cframe != NULL);
	saved->cframe_owned = saved->cframe;
#endif
	py_tstate->trash_delete_nesting = saved->trash_delete_nesting;

	saved->has_state = 0;
}

/* Increment and decrement the reference count of the tstate's references.
 * we need to Increment the references when we create new tealets from an existing
 * one (or main), and decrement when a tealet terminates.
 */
static void PyTealetTstate_IncRef(PyTealetTstate *saved)
{
	assert(saved->has_state == 1);
	assert(saved->own_refs == 0);
	Py_XINCREF(saved->frame);
	Py_XINCREF(saved->exc_type);
	Py_XINCREF(saved->exc_val);
	Py_XINCREF(saved->exc_tb);
	Py_XINCREF(saved->exc_state.exc_value);
	/* exc_info is a pointer to exc_state or a stack item, so we don't own a reference to it */
	Py_XINCREF(saved->context);
	saved->own_refs = 1;
}

static void PyTealetTstate_DecRef(PyTealetTstate *saved)
{
	assert(saved->has_state == 1);
	assert(saved->own_refs == 1);
	Py_XDECREF(saved->frame);
	Py_XDECREF(saved->exc_type);
	Py_XDECREF(saved->exc_val);
	Py_XDECREF(saved->exc_tb);
	Py_XDECREF(saved->exc_state.exc_value);
	/* exc_info is a pointer to exc_state or a stack item, so we don't own a reference to it */
	Py_XDECREF(saved->context);
	saved->own_refs = 0;
}

/* get the far pointer that we need at least ot store any stack based data currently
 * in the python tstate.  this varies by python version
 */

static void * PyTealet_GetStackFar(const PyThreadState *py_tstate)
{
#if defined(PY_HAS_CFRAME)
	/* python 3.10 has cframe on stack.  make sure we save our stacks to include
	 * this whole structure
	 */
	if (py_tstate->cframe)
		return tealet_stack_further(&py_tstate->cframe[0], &py_tstate->cframe[1]);
#else
	(void)py_tstate;
#endif
	return NULL;
}

/* helper functions to save and restore callstack related data from the python threadstate
 * into the tealet object
 */
static void
save_tstate(PyTealetObject *current, PyThreadState *tstate)
{
	LOG("save_tstate: current=%p, tstate=%p, frame=%p, depth=%d\n",
	    current, tstate, tstate->frame, tstate->recursion_depth);
	if (!tstate)
		tstate = PyThreadState_GET();
	if (current && current->tealet)
		current->tstate.stack_far_saved = tealet_get_far(current->tealet);

#if defined(PY_HAS_CFRAME)
#if TEALET_PYTEALET_ENABLE_STACK_DIAGNOSTICS
	log_cframe_chain("save_tstate BEFORE capture", (void *)current,
		current && current->tealet ? tealet_get_far(current->tealet) : NULL,
		tstate);
#endif
#endif

	assert(current->tstate.has_state == 0);
	LOG("save_tstate: tstate->context=%p before save\n", tstate->context);
	PyTealetTstate_Capture(&current->tstate, tstate);

#if defined(PY_HAS_CFRAME)
	dbg_capture_saved_main_window("save_tstate-after-capture", current);
#endif

#if defined(PY_HAS_CFRAME)
#if TEALET_PYTEALET_ENABLE_STACK_DIAGNOSTICS
	log_cframe_chain("save_tstate AFTER capture", (void *)current,
		current && current->tealet ? tealet_get_far(current->tealet) : NULL,
		tstate);
	fprintf(stderr,
		"[CFRAME] save_tstate SAVED owner=%p saved_cframe=%p\n",
		(void *)current,
		(void *)current->tstate.cframe);
#endif
#endif
}
/* helper functions to save and restore callstack related data from the python threadstate
 * into the tealet object
 */
static void
restore_tstate(PyTealetObject *current, PyThreadState *tstate)
{
	LOG("restore_tstate: current=%p, tstate=%p, restoring frame=%p, depth=%d\n",
	    current, tstate, current->tstate.frame, current->tstate.recursion_depth);
	LOG("restore_tstate: current->context=%p, current->exc_val=%p\n",
	    current->tstate.context, current->tstate.exc_val);

#if defined(PY_HAS_CFRAME)
#if TEALET_PYTEALET_ENABLE_STACK_DIAGNOSTICS
	fprintf(stderr,
		"[CFRAME] restore_tstate BEFORE restore chain_walk=skipped reason=transient_pre_restore_state owner=%p saved_cframe=%p\n",
		(void *)current,
		(void *)current->tstate.cframe);
#endif
#endif

	assert(!PyErr_Occurred());

	PyTealetTstate_Restore(&current->tstate, tstate);
	current->dbg_saved_window_has_snapshot = 0;

#if defined(PY_HAS_CFRAME)
#if TEALET_PYTEALET_ENABLE_STACK_DIAGNOSTICS
	log_cframe_chain("restore_tstate AFTER restore", (void *)current,
		current && current->tealet ? tealet_get_far(current->tealet) : NULL,
		tstate);
#endif
#endif
	
	/* Comprehensive verification of restored state */
	/* Frame can be NULL for a new tealet that hasn't executed yet */
	/* recursion_depth should match whether we have frames */
	
	/* Context can be NULL in some Python states */
	/* No assertion needed for context */
	
	/* cframe should always point somewhere valid */
#if defined(PY_HAS_CFRAME)
	assert(tstate->cframe != NULL);
#endif
	
	/* Current exceptions should be NULL or valid objects */
	assert(tstate->curexc_type == NULL || PyType_Check(tstate->curexc_type) || tstate->curexc_type == Py_None);
	assert(tstate->curexc_value == NULL || PyObject_CheckBuffer(tstate->curexc_value) || 1); /* Any object ok */
	assert(tstate->curexc_traceback == NULL || PyTraceBack_Check(tstate->curexc_traceback));
	
	/* Recursion depth should be non-negative */
	assert(tstate->recursion_depth >= 0);
	
	LOG("restore_tstate COMPLETE: tstate now has frame=%p, depth=%d\n",
	    tstate->frame, tstate->recursion_depth);
}

/* Helper functions to fill/empty the dustbin.  We must be careful not to
 * clear references at a delicate moment before switching, rather
 * references must be cleared after, so that any side-effects of
 * clearing references won't affect the state of the program.
 */
static void
dustbin_fill(tealet_t *tealet, PyObject *a, PyObject *b, PyObject *c)
{
	main_data *mdata = (main_data*)*tealet_main_userpointer(tealet);
	assert(!mdata->dustbin[0]);
	assert(!mdata->dustbin[1]);
	assert(!mdata->dustbin[2]);
	mdata->dustbin[0] = a;
	mdata->dustbin[1] = b;
	mdata->dustbin[2] = c;
}

static void
dustbin_clear(tealet_t *tealet)
{
	main_data *mdata = (main_data*)*tealet_main_userpointer(tealet);
	PyObject *a, *b, *c;
	a = mdata->dustbin[0];
	b = mdata->dustbin[1];
	c = mdata->dustbin[2];
	mdata->dustbin[0] = mdata->dustbin[1] = mdata->dustbin[2] = NULL;
	Py_XDECREF(a);
	Py_XDECREF(b);
	Py_XDECREF(c);
}

static PyObject *
pytealet_new(PyTypeObject *subtype, PyObject *args, PyObject *kwds)
{
	PyTealetObject *src = NULL;
	PyTealetObject *result;
	if (args && PyTuple_GET_SIZE(args)>0) {
		src = (PyTealetObject*)PyTuple_GET_ITEM(args, 0);
		if (!PyTealet_Check(src)) {
			PyErr_SetNone(PyExc_TypeError);
			return NULL;
		}
		if (src->state != STATE_NEW && src->state != STATE_STUB) {
			PyErr_SetString(StateError, "state must be new or stub");
			return NULL;
		}
	}
	result = (PyTealetObject*)subtype->tp_alloc(subtype, 0);
	if (!result)
		return NULL;
	result->state = STATE_NEW;
	result->tealet = NULL;
	PyTealetTstate_Init(&result->tstate);
	result->weakreflist = NULL;
	result->dbg_id = 0;
	result->dbg_prev = NULL;
	result->dbg_next = NULL;
	result->dbg_last_head = NULL;
	result->dbg_anchor_cframe = NULL;
	result->dbg_anchor_near = NULL;
	result->dbg_anchor_far = NULL;
	result->dbg_has_anchor = 0;
	result->dbg_last_saved_cframe = NULL;
	result->dbg_last_saved_near = NULL;
	result->dbg_last_saved_far = NULL;
	result->dbg_last_live_far = NULL;
	result->dbg_last_state = -1;
	result->dbg_last_has_state = -1;
	result->dbg_last_own_refs = -1;
	result->dbg_has_last_snapshot = 0;
	result->dbg_saved_window_has_snapshot = 0;
	result->dbg_saved_window_addr = NULL;
	result->dbg_saved_window_len = 0;
	result->dbg_saved_window_hash = 0;
	result->dbg_saved_heap_has_snapshot = 0;
	result->dbg_saved_heap_addr = NULL;
	result->dbg_saved_heap_len = 0;
	result->dbg_saved_heap_hash = 0;
	dbg_register_tealet(result);

	if (src) {
		assert(0 && "clone-from-source path executed unexpectedly; add/adjust tests before enabling this path");
		PyTealetTstate_CopyForClone(&result->tstate, &src->tstate);
		PyTealetTstate_IncRef(&result->tstate);
		if (src->state == STATE_STUB) {
			result->tealet = tealet_duplicate(src->tealet);
			if (!result->tealet) {
				PyTealetTstate_DecRef(&result->tstate);
				Py_DECREF(result);
				return PyErr_NoMemory();
			}
			TEALET_SET_PYOBJECT(result->tealet, result);
		}
		result->state = src->state;
	}
	return (PyObject*) result;
}

static void
pytealet_dealloc(PyObject *obj)
{
	PyTealetObject *tealet = (PyTealetObject *)obj;
	if (tealet->state == STATE_RUN) {
		int err = PyErr_WarnEx(PyExc_RuntimeWarning, "freeing an active tealet leaks memory", 1);
		if (err) {
			PyErr_WriteUnraisable(Py_None);
		}
	}
	dbg_unregister_tealet(tealet);
	/* Release any owned saved thread-state references */
	if (tealet->tstate.has_state) {
		if (tealet->tstate.own_refs)
			PyTealetTstate_DecRef(&tealet->tstate);
	}
	PyTealetTstate_Init(&tealet->tstate);
	if (tealet->weakreflist != NULL)
        PyObject_ClearWeakRefs(obj);
	if (tealet->tealet)
		tealet_delete(tealet->tealet);
	Py_TYPE(obj)->tp_free(obj);
}

/* make stub here */
static PyObject *
pytealet_stub(PyObject *self)
{
	PyTealetObject *main, *pytealet = (PyTealetObject*)self;
	tealet_t *tresult;
	if (pytealet->state != STATE_NEW) {
		PyErr_SetString(StateError, "must be new");
		return NULL;
	}
	assert(pytealet->tealet == NULL);
	main = GetMain(1);
	if (!main)
		return NULL;
	tresult = stub_new(main->tealet);
	if (!tresult)
		return PyErr_NoMemory();
	pytealet->tealet = tresult;
	pytealet->state = STATE_STUB;
	TEALET_SET_PYOBJECT(tresult, pytealet);
	Py_INCREF(self);
	return self;
}

/* run a tealet and optinonally run */
static PyObject *
pytealet_run(PyObject *self, PyObject *args, PyObject *kwds)
{
	PyTealetObject *target = (PyTealetObject *)self;
	PyTealetObject *current;
	PyObject *func; 
	PyObject *farg = Py_None;
	int fail;
	tealet_t *tealet;
	char *keywords[] = {"function", "arg", NULL};
	PyThreadState *tstate = PyThreadState_GET();
	PyObject *result = NULL;
	int created_from_new;
	main_data *mdata;
	tealet_new_arg *ptarg;
	void *switch_arg;

	/* target->tealet is null or a stub tealet.  GetCurrent works either way. */
	current = GetCurrent(target);
	if (!current)
		return NULL;
	if (CheckTarget(target, current))
		return NULL;

	if (target->state != STATE_NEW && target->state != STATE_STUB) {
		PyErr_SetString(StateError, "must be new or stub");
		return NULL;
	}
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|O:run", keywords,
		&func, &farg))
		return NULL;
	
	created_from_new = (target->state == STATE_NEW);
	mdata = (main_data*)*tealet_main_userpointer(current->tealet);
	ptarg = &mdata->new_arg;
	switch_arg = (void*)ptarg;
	
	if (!created_from_new) {
		assert(0 && "temporarily disabled during development");
	}
	ptarg->stub = !created_from_new;
	ptarg->dest = target;
	ptarg->func = func;
	ptarg->arg = farg;
	
	/* Save caller threadstate; on first run from STATE_NEW, keep an extra owning
	 * reference set for caller's parked state.
	 */
	dbg_failfast_validate_active_cframe("py-run-before-save", current, tstate);
#if defined(PY_HAS_CFRAME)
#if TEALET_PYTEALET_ENABLE_STACK_DIAGNOSTICS
	log_switch_cframes("run-before-save", current, target, tstate);
#endif
#endif
	save_tstate(current, tstate);
	dbg_failfast_validate_active_cframe("py-run-after-save", current, tstate);

#if defined(PY_HAS_CFRAME)
#if TEALET_PYTEALET_ENABLE_STACK_DIAGNOSTICS
	log_switch_cframes("run-after-save-before-transfer", current, target, tstate);
#endif
#endif
	if (created_from_new)
		PyTealetTstate_IncRef(&current->tstate);
	if (ptarg->stub) {
		fail = stub_run(target->tealet, pytealet_main, &switch_arg);
		if (fail) {
			PyErr_NoMemory();
			goto err;
		}
	} else {
		void *stack_limit = PyTealet_GetStackFar(tstate);
		tealet = tealet_new(current->tealet, pytealet_main, &switch_arg, stack_limit);
		if (!tealet) {
			PyErr_NoMemory();
			goto err;
		}
	}
	/* success */
	result = (PyObject *)switch_arg;
err:
	if (created_from_new)
		PyTealetTstate_DecRef(&current->tstate);
	/* restore frame */
	restore_tstate(current, tstate);
	dbg_failfast_validate_active_cframe("py-run-after-restore", current, tstate);

#if defined(PY_HAS_CFRAME)
#if TEALET_PYTEALET_ENABLE_STACK_DIAGNOSTICS
	log_switch_cframes("run-after-restore", current, target, tstate);
#endif
#endif
	/* clear garbage */
	dustbin_clear(current->tealet);
	return result;
}
/* switch to a different tealet */
static PyObject *
pytealet_switch(PyObject *_self, PyObject *args)
{
	PyTealetObject *self = (PyTealetObject *)_self;
	PyTealetObject *current;
	int fail;
	PyThreadState *tstate = PyThreadState_GET();
	PyObject *pyarg = Py_None;
	void *switch_arg;
	int stack_marker_before;
	int stack_marker_after;
	
	LOG("pytealet_switch: self=%p (state=%d), stack=%p\n", self, self->state, (void*)&stack_marker_before);
	
	if (!PyArg_ParseTuple(args, "|O:switch", &pyarg))
		return NULL;

	if (self->state != STATE_RUN) {
		LOG("pytealet_switch ERROR: self=%p is not STATE_RUN (state=%d)\n", self, self->state);
		PyErr_SetString(StateError, "must be active");
		return NULL;
	}
	assert(self->tealet);
	current = GetCurrent(NULL);
	if (!current)
		return NULL;
	LOG("pytealet_switch: switching from %p (state=%d) to %p (state=%d)\n",
	    current, current->state, self, self->state);
	if (CheckTarget(self, current))
		return NULL;
	dbg_failfast_validate_active_cframe("py-switch-before-save", current, tstate);
	
	Py_INCREF(pyarg);
	switch_arg = (void*)pyarg;
	/* switch */
#if defined(PY_HAS_CFRAME)
#if TEALET_PYTEALET_ENABLE_STACK_DIAGNOSTICS
	log_switch_cframes("before-save", current, self, tstate);
#endif
#endif
	save_tstate(current, tstate);
#if defined(PY_HAS_CFRAME)
#if TEALET_PYTEALET_ENABLE_STACK_DIAGNOSTICS
	log_switch_cframes("after-save-before-switch", current, self, tstate);
#endif
#endif
	LOG("pytealet_switch: about to call tealet_switch(tealet=%p)\n", self->tealet);
	fail = tealet_switch(self->tealet, &switch_arg);
	LOG("pytealet_switch: tealet_switch returned, tealet=%p\n", self->tealet);
	restore_tstate(current, tstate);
	dbg_failfast_validate_active_cframe("py-switch-after-restore", current, tstate);
#if defined(PY_HAS_CFRAME)
#if TEALET_PYTEALET_ENABLE_STACK_DIAGNOSTICS
	log_switch_cframes("after-restore", current, self, tstate);
#endif
#endif

	LOG("pytealet_switch: returned from switch, fail=%d, stack=%p\n", fail, (void*)&stack_marker_after);

	/* clear out garbage */
	dustbin_clear(current->tealet);
	
	if (fail == TEALET_ERR_DEFUNCT) {
		Py_DECREF(pyarg);
		PyErr_SetString(DefunctError, "target is defunct");
		return NULL;
	} else if (fail == TEALET_ERR_MEM) {
		Py_DECREF(pyarg);
		return PyErr_NoMemory();
	}
	/* return the arg passed to us */
	pyarg = (PyObject *)switch_arg;
	return pyarg;
}
	
static struct PyMethodDef pytealet_methods[] = {
	{"stub", (PyCFunction) pytealet_stub, METH_NOARGS, ""},
	{"run", (PyCFunction) pytealet_run, METH_VARARGS|METH_KEYWORDS, ""},
    {"switch", (PyCFunction) pytealet_switch, METH_VARARGS, ""},
	{NULL,       NULL}          /* sentinel */
};

/************
 * Properties
 */
static PyObject *
pytealet_get_main(PyObject *_self, void *_closure)
{
	PyTealetObject *self = (PyTealetObject *)_self;
	PyTealetObject *main;
	
	if (!self->tealet) {
		/* New tealet not yet initialized (STATE_NEW) or
		 * tealet has exited and been auto-deleted (STATE_EXIT).
		 * Return the thread's main tealet.
		 * TODO: Review if STATE_NEW should exist without tealet (lazy creation)
		 */
		main = GetMain(1);
		if (!main)
			return NULL;
	} else {
		main = TEALET_PYOBJECT(self->tealet->main);
	}
	Py_INCREF(main);
	return (PyObject*)main;
}

static PyObject *
pytealet_get_state(PyObject *_self, void *_closure)
{
	PyTealetObject *self = (PyTealetObject *)_self;
	return PyLong_FromLong(self->state);
}

static PyObject *
pytealet_get_frame(PyObject *_self, void *_closure)
{
	PyTealetObject *self = (PyTealetObject *)_self;
	PyObject *frame = (PyObject*)self->tstate.frame;
	if (!frame) {
		/* is it the current tealet of the current thread? */
		if (self == GetCurrent(NULL)) {
			PyThreadState *tstate = PyThreadState_GET();
			frame = (PyObject*)tstate->frame;
		}
	}
	if (!frame)
		frame = Py_None;
	Py_INCREF(frame);
	return frame;
}

static PyObject *
pytealet_get_tid(PyObject *_self, void *_closure)
{
	PyTealetObject *self = (PyTealetObject *)_self;
	long tid = 0;
	if (self->tealet) {
		main_data *mdata = (main_data*)*tealet_main_userpointer(self->tealet);
		tid = mdata->tid;
	}
	return PyLong_FromLong(tid);
}


static struct PyGetSetDef pytealet_getset[] = {
	{"main", pytealet_get_main, NULL, "", NULL},
	{"state", pytealet_get_state, NULL, "", NULL},
	{"frame", pytealet_get_frame, NULL, "", NULL},
	{"thread_id", pytealet_get_tid, NULL, "", NULL},
	{0}
};


static PyTypeObject PyTealetType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_tealet.tealet",                           /* tp_name */
    sizeof(PyTealetObject),                     /* tp_basicsize */
    0,                                          /* tp_itemsize */
    (destructor)pytealet_dealloc,                 /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_compare */
    0,                                          /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    0,                                          /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,   /* tp_flags */
    "",                                         /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    offsetof(PyTealetObject,weakreflist),       /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    pytealet_methods,                           /* tp_methods */
    0,                                          /* tp_members */
    pytealet_getset,                            /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    0,                                          /* tp_init */
    0,                                          /* tp_alloc */
    pytealet_new,                               /* tp_new */
    0,                                          /* tp_free */
};


/* The main function.  Invoked either from tealet.new or tealet.run */
static tealet_t *
pytealet_main(tealet_t *t_current, void *arg)
{
	tealet_new_arg *targ = (tealet_new_arg*)arg;
	PyTealetObject *tealet = targ->dest;
	PyObject *func = targ->func;
	PyObject *farg = targ->arg;
	PyObject *result, *return_arg;
	PyTealetObject *return_to;
	tealet_t *t_return;
	#if defined(PY_HAS_CFRAME)
		PyThreadState *entry_tstate;
		CFrame tealet_owned_cframe;
	#endif
	
	if (targ->stub) {
		assert(tealet->state == STATE_STUB);
		assert(t_current == tealet->tealet);
		assert(TEALET_PYOBJECT(t_current) == tealet);
	} else {
		/* set up the pointer in the tealet */
		tealet->tealet = t_current;
		TEALET_SET_PYOBJECT(t_current, tealet);
	}

#if defined(PY_HAS_CFRAME)
	entry_tstate = PyThreadState_GET();
	#if TEALET_PYTEALET_FIX_LOCAL_CFRAME_COPY
	if (entry_tstate && entry_tstate->cframe) {
		memcpy(&tealet_owned_cframe, entry_tstate->cframe, sizeof(tealet_owned_cframe));
		tealet_owned_cframe.previous = NULL;
		entry_tstate->cframe = &tealet_owned_cframe;
		tealet->tstate.cframe = &tealet_owned_cframe;
		tealet->tstate.cframe_owned = &tealet_owned_cframe;
	} else {
		tealet->tstate.cframe_owned = NULL;
	}
	#else
	if (entry_tstate) {
		tealet->tstate.cframe_owned = NULL;
	}
	#endif
#endif

#if defined(PY_HAS_CFRAME)
#if TEALET_PYTEALET_ENABLE_STACK_DIAGNOSTICS
	dbg_maybe_capture_anchor("run-entry", tealet, PyThreadState_GET());
#endif
#endif

	/* We only have borrowed references from the calling tealet.
	 * the argument to the function will get their own reference, but
	 * anything we need after the function we keep oru own references
	 * for, because when the function returns, the calling tealet
	 * may have exited and dropped the references we borrowed.
	 */
	Py_INCREF(func);
	Py_INCREF(tealet);
	
	/* clear frame and run the tealet function */
	tealet->state = STATE_RUN;
	result = PyObject_CallFunctionObjArgs(func, tealet, farg, NULL);
	
	/* return_to can be a tuple of tealet, arg */
	return_to = NULL;
	return_arg = NULL;
	if (result && PyTuple_Check(result)) {
		/* arg and return_to are borrowed refs */
		if (PyTuple_GET_SIZE(result)>0)
			return_to = (PyTealetObject*)PyTuple_GET_ITEM(result, 0);
		if (PyTuple_GET_SIZE(result)>1)
			return_arg = PyTuple_GET_ITEM(result, 1);
	} else
		return_to = (PyTealetObject*)result;
		
	/* perform sanity checks on the result */
	if (return_to) {
		/* it is ok to rock the GC boat here, because we will switch to
		 * main in case of error, and main is always around
		 */
	if (!PyTealet_Check(return_to)) {
		return_to = NULL;
		PyErr_SetString(PyExc_TypeError, "tealet object expected");
	} else if (return_to->state != STATE_RUN) {
		return_to = NULL;
		PyErr_SetString(StateError, "must be 'run'");
	} else if (CheckTarget(return_to, tealet))
		return_to = NULL;
	}
	if (!return_to) {
		Py_CLEAR(result);
		return_arg = NULL;
	}
	if (!return_arg)
		return_arg = Py_None;
	
	/* handle errors */
	if (!return_to) {
		PyErr_WriteUnraisable(func);
		/* must switch to main */
		return_to = GetMain(0);
		assert(return_to);
		result = (PyObject*)return_to;
		Py_INCREF(result);
	}
	/* now, the reference to return_to and return_arg are borrowed, kept alive
	 * by 'result', which may be the same as return_to.
	 */
	
	/* clear the old tealet */
	tealet->state = STATE_EXIT;
	tealet->tealet = NULL; /* will be auto-deleted on return */
	TEALET_SET_PYOBJECT(t_current, NULL);
	t_return = return_to->tealet;
	
	/* decref the objects after the switch */
	dustbin_fill(t_return, func, (PyObject*)tealet, result);
	
	Py_INCREF(return_arg);
	if (tealet_exit(t_return, (void*)return_arg, TEALET_EXIT_DELETE))
		tealet_exit(t_return->main, (void *)return_arg, TEALET_EXIT_DELETE);
	/* never reach here */
	return 0;
}

/* Wrapper functions for system malloc/free to match libtealet's allocator API
 * Use system malloc for valgrind heap corruption detection */
static void* tealet_malloc_wrapper(size_t size, void *context)
{
	dbg_heap_block *block;
	uint64_t *tail;
	(void)context;  /* unused */
	block = (dbg_heap_block *)malloc(sizeof(*block) + size + sizeof(uint64_t));
	if (!block)
		return NULL;
	block->magic_head = DBG_HEAP_MAGIC_HEAD;
	block->payload_size = size;
	block->prev = NULL;
	block->next = dbg_heap_blocks_head;
	if (dbg_heap_blocks_head)
		dbg_heap_blocks_head->prev = block;
	dbg_heap_blocks_head = block;
	tail = (uint64_t *)((char *)(block + 1) + size);
	*tail = DBG_HEAP_MAGIC_TAIL;
	return (void *)(block + 1);
}

static void tealet_free_wrapper(void *ptr, void *context)
{
	dbg_heap_block *block;
	uint64_t *tail;
	(void)context;  /* unused */
	if (!ptr)
		return;
	block = ((dbg_heap_block *)ptr) - 1;
	if (block->magic_head != DBG_HEAP_MAGIC_HEAD) {
		fprintf(stderr,
			"[HEAP_ALLOC_GUARD] action=abort reason=bad_head ptr=%p head=0x%016llx\n",
			ptr,
			(unsigned long long)block->magic_head);
		abort();
	}
	tail = (uint64_t *)((char *)ptr + block->payload_size);
	if (*tail != DBG_HEAP_MAGIC_TAIL) {
		fprintf(stderr,
			"[HEAP_ALLOC_GUARD] action=abort reason=bad_tail ptr=%p size=%zu tail=0x%016llx\n",
			ptr,
			block->payload_size,
			(unsigned long long)*tail);
		abort();
	}
	if (block->prev)
		block->prev->next = block->next;
	else if (dbg_heap_blocks_head == block)
		dbg_heap_blocks_head = block->next;
	if (block->next)
		block->next->prev = block->prev;
	block->magic_head = 0;
	free(block);
}

/* return a borrowed reference to this thread's main tealet */
static PyTealetObject *GetMain(int create)
{
	/* Get the thread's main tealet */
	PyTealetObject *t_main = (PyTealetObject*)PyThread_get_key_value(tls_key);
	if (!t_main && !create) {
		return NULL;
	}

	/* main tealet doesn't exist yet.  create it. */
	if (!t_main) {
		tealet_alloc_t talloc;
		tealet_t *tmain;
		main_data *mdata;
		/* Use system malloc/free so valgrind can detect heap corruption */
		talloc.malloc_p = tealet_malloc_wrapper;
		talloc.free_p = tealet_free_wrapper;
		talloc.context = NULL;
		tmain = tealet_initialize(&talloc, sizeof(tealet_extra_t));
		if (!tmain) {
			PyErr_NoMemory();
			return NULL;
		}
		{
			const char *check_stack_env = getenv("PYTEALET_CHECK_STACK");
			if (check_stack_env && *check_stack_env && *check_stack_env != '0') {
				if (tealet_configure_check_stack(tmain, 0) < 0) {
					tealet_finalize(tmain);
					PyErr_SetString(PyExc_RuntimeError, "tealet_configure_check_stack failed");
					return NULL;
				}
			}
		}
		mdata = (main_data*)PyMem_Malloc(sizeof(*mdata));
		if (!mdata) {
			tealet_finalize(tmain);
			PyErr_NoMemory();
			return NULL;
		}
		memset(mdata, 0, sizeof(*mdata));
		mdata->tid = PyThread_get_thread_ident();
		*tealet_main_userpointer(tmain) = (void*)mdata;

		/* create the main tealet */
		t_main = (PyTealetObject*)pytealet_new(&PyTealetType, NULL, NULL);
		if (!t_main) {
			tealet_finalize(tmain);
			PyMem_Free(mdata);
			return NULL;
		}
		t_main->tealet = tmain;
		t_main->state = STATE_RUN;
		TEALET_SET_PYOBJECT(tmain, t_main); /* back link */
		PyThread_set_key_value(tls_key, (void*)t_main);
#if defined(PY_HAS_CFRAME)
#if TEALET_PYTEALET_ENABLE_STACK_DIAGNOSTICS
		dbg_maybe_capture_anchor("main-created", t_main, PyThreadState_GET());
#endif
#endif
	}
	assert(t_main->tealet);
	assert(TEALET_IS_MAIN(t_main->tealet));
	assert(t_main->state == STATE_RUN);		
	return t_main;
}

/* return a borrowed ref to this threads current tealet */
static PyTealetObject *
GetCurrent(PyTealetObject *pytealet)
{
	/* if we are being passed no tealet, or it is a new tealet, 
	 * we must get the current main from the thread-local storage */
	if (!pytealet || !pytealet->tealet)
		pytealet = GetMain(1);
	if (!pytealet)
		return NULL;
	return TEALET_PYOBJECT(tealet_current(pytealet->tealet));
}

/* check if a target tealet is valid */
static int
CheckTarget(PyTealetObject *target, PyTealetObject *ref)
{
	if (!ref)
		ref = GetMain(1);
	if (!ref)
		return -1;
	if (!target->tealet)
		return 0; /* no tealet yet */
	if (ref->tealet->main != target->tealet->main) {
		PyErr_SetString(InvalidError, "foreign tealet");
		return -1;
	}
	return 0;
}

/******************************************
 * Module methods
 */


static PyObject *
module_current()
{
	PyTealetObject* current = GetCurrent(NULL);
	Py_XINCREF(current);
	return (PyObject*)current;
}

static PyObject *
module_main()
{
	PyTealetObject* main = GetMain(1);
	Py_XINCREF(main);
	return (PyObject*)main;
}

static PyObject *
hide_frame(PyObject *self, PyObject *_args)
{
	/* this function calls a method, clearing the frame.  This hides
	 * higher frames in the callstack
	 */
	PyObject *func, *args=NULL, *kwds=NULL;
	PyThreadState *tstate = PyThreadState_GET();
	PyFrameObject *f = tstate->frame;
	PyObject *result;
	if (!PyArg_ParseTuple(_args, "O|OO:hide_frame", &func, &args, &kwds))
		return NULL;
	if (!args) {
		PyObject *empty = PyTuple_New(0);
		if (!empty)
			return NULL;
		tstate->frame = NULL;
		result = PyObject_Call(func, empty, kwds);
		Py_DECREF(empty);
	} else {
		tstate->frame = NULL;
		result = PyObject_Call(func, args, kwds);
	}
	tstate->frame = f;
	return result;
}

static PyMethodDef module_methods[] = {
	{"current", (PyCFunction)module_current, METH_NOARGS, ""},
	{"main", (PyCFunction)module_main, METH_NOARGS, ""},
	{"hide_frame", (PyCFunction)hide_frame, METH_VARARGS, ""},
	{NULL, NULL, 0, NULL}  /* Sentinel */
};

static struct PyModuleDef _tealet_module = {
	PyModuleDef_HEAD_INIT,
	"_tealet",   /* name of module */
	NULL,        /* module documentation, may be NULL */
	-1,          /* size of per-interpreter state of the module,
	                or -1 if the module keeps state in global variables. */
	module_methods
};

PyMODINIT_FUNC
PyInit__tealet(void)
{
	PyObject *m;
	PyTealetObject *main;

	tls_key = PyThread_create_key();

	/* init the type */
	if (PyType_Ready(&PyTealetType) < 0)
		return NULL;

	main = GetMain(1);
	if (!main)
		return NULL;
	
	m = PyModule_Create(&_tealet_module);
	if (m == NULL)
		return NULL;

	/* Todo: Improve error handling */
	Py_INCREF(&PyTealetType);
	if (PyModule_AddObject(m, "tealet", (PyObject*)&PyTealetType) < 0) {
		Py_DECREF(&PyTealetType);
		Py_DECREF(m);
		return NULL;
	}
	
	TealetError = PyErr_NewException("_tealet.TealetError", NULL, NULL);
	Py_INCREF(TealetError);
	PyModule_AddObject(m, "TealetError", TealetError);
	
	DefunctError = PyErr_NewException("_tealet.DefunctError", TealetError, NULL);
	Py_INCREF(DefunctError);
	PyModule_AddObject(m, "DefunctError", DefunctError);
	
	InvalidError = PyErr_NewException("_tealet.InvalidError", TealetError, NULL);
	Py_INCREF(InvalidError);
	PyModule_AddObject(m, "InvalidError", InvalidError);
	
	StateError = PyErr_NewException("_tealet.StateError", TealetError, NULL);
	Py_INCREF(StateError);
	PyModule_AddObject(m, "StateError", StateError);

	PyModule_AddIntMacro(m, STATE_NEW);
	PyModule_AddIntMacro(m, STATE_STUB);
	PyModule_AddIntMacro(m, STATE_RUN);
	PyModule_AddIntMacro(m, STATE_EXIT);
	
	return m;
}

#if defined(PY_HAS_CFRAME)
static void
dbg_capture_saved_main_window(const char *phase, PyTealetObject *owner)
{
	void *saved_lo;
	void *saved_hi;
	char *frame_start;
	char *window_lo;
	char *window_hi;
	size_t window_len;
	uint64_t window_hash;

	dbg_init_main_window_watch_config();
	if (!owner || !owner->tealet || !TEALET_IS_MAIN(owner->tealet))
		return;
	if (!owner->tstate.has_state || !owner->tstate.cframe || !owner->tstate.stack_near_saved || !owner->tstate.stack_far_saved)
		return;
	if (!dbg_main_window_watch_enabled)
		return;

	stack_bounds(owner->tstate.stack_near_saved,
		    owner->tstate.stack_far_saved,
		    &saved_lo,
		    &saved_hi);
	frame_start = (char *)owner->tstate.cframe;
	window_lo = frame_start - dbg_main_window_radius;
	window_hi = frame_start + (ptrdiff_t)sizeof(CFrame) + dbg_main_window_radius;
	if (window_lo < (char *)saved_lo)
		window_lo = (char *)saved_lo;
	if (window_hi > (char *)saved_hi + 1)
		window_hi = (char *)saved_hi + 1;
	if (window_hi <= window_lo) {
		owner->dbg_saved_window_has_snapshot = 0;
		return;
	}
	window_len = (size_t)(window_hi - window_lo);
	window_hash = fnv1a64_update(1469598103934665603ULL, window_lo, window_len);

	owner->dbg_saved_window_has_snapshot = 1;
	owner->dbg_saved_window_addr = (void *)window_lo;
	owner->dbg_saved_window_len = window_len;
	owner->dbg_saved_window_hash = window_hash;

	fprintf(stderr,
		"[MAIN_WINDOW_CAPTURE] phase=%s main_id=%ld main_obj=%p main_tealet=%p cframe=%p window=[%p,%p) len=%zu hash=0x%016llx\n",
		phase ? phase : "unknown",
		owner->dbg_id,
		(void *)owner,
		(void *)owner->tealet,
		(void *)owner->tstate.cframe,
		(void *)window_lo,
		(void *)window_hi,
		window_len,
		(unsigned long long)window_hash);
}

static void
dbg_compare_saved_main_window(const char *phase, PyTealetObject *owner)
{
	void *saved_lo;
	void *saved_hi;
	char *frame_start;
	char *window_lo;
	char *window_hi;
	size_t window_len;
	uint64_t window_hash;

	dbg_init_main_window_watch_config();
	if (!owner || !owner->tealet || !TEALET_IS_MAIN(owner->tealet))
		return;
	if (!dbg_main_window_watch_enabled)
		return;
	if (!owner->dbg_saved_window_has_snapshot)
		return;
	if (!owner->tstate.has_state || !owner->tstate.cframe || !owner->tstate.stack_near_saved || !owner->tstate.stack_far_saved)
		return;

	stack_bounds(owner->tstate.stack_near_saved,
		    owner->tstate.stack_far_saved,
		    &saved_lo,
		    &saved_hi);
	frame_start = (char *)owner->tstate.cframe;
	window_lo = frame_start - dbg_main_window_radius;
	window_hi = frame_start + (ptrdiff_t)sizeof(CFrame) + dbg_main_window_radius;
	if (window_lo < (char *)saved_lo)
		window_lo = (char *)saved_lo;
	if (window_hi > (char *)saved_hi + 1)
		window_hi = (char *)saved_hi + 1;
	if (window_hi <= window_lo)
		return;

	window_len = (size_t)(window_hi - window_lo);
	window_hash = fnv1a64_update(1469598103934665603ULL, window_lo, window_len);

	if (owner->dbg_saved_window_addr == (void *)window_lo &&
	    owner->dbg_saved_window_len == window_len &&
	    owner->dbg_saved_window_hash != window_hash) {
		fprintf(stderr,
			"[MAIN_WINDOW_WATCH] phase=%s main_id=%ld main_obj=%p main_tealet=%p cframe=%p window=[%p,%p) len=%zu old_hash=0x%016llx new_hash=0x%016llx action=%s\n",
			phase ? phase : "unknown",
			owner->dbg_id,
			(void *)owner,
			(void *)owner->tealet,
			(void *)owner->tstate.cframe,
			(void *)window_lo,
			(void *)window_hi,
			window_len,
			(unsigned long long)owner->dbg_saved_window_hash,
			(unsigned long long)window_hash,
			dbg_main_window_abort_enabled ? "abort" : "log");
		if (dbg_main_window_abort_enabled)
			abort();
	}
}
#endif