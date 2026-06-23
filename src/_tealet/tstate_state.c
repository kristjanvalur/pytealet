/* tstate_state.c - manage the thread state fields that we need to save/restore
 * across tealet switches.
 *
 * The functions in this module contain most of the version specific code related
 * to saving and restoring the Python thread state when switching between tealets.
 *
 * Fields are copied to and from the native threadstate object.  There are also
 * functions to increment the reference counts of the Python objects in
 * the saved state, and to help release them when we drop the state.
 */

#include "tstate_state.h"
#include "pytealet.h"

#include <assert.h>
#include <string.h>

/* we need to treat the "frame" part of the tstate specially, since we
 * can't share it among tealets.  We must be careful when we branch off
 * to a new tealet that we clear either the source or destination frame data */
static void PyTealetTstate_GetFrame(PyTealetTstateFrame *dst, const PyThreadState *src);
static void PyTealetTstate_PutFrame(const PyTealetTstateFrame *src, PyThreadState *dst);
/* clean the python frame structures in either python or our threadstate */
static void PyTealetTstate_ClearFrame(PyTealetTstateFrame *ttstate, PyThreadState *tstate);
/* directly clean up datastack pointer members and clear them afterwards. */
#if defined(PY_HAS_TSTATE_DATASTACK)
static void PyTealetTstate_CleanupDatastack(_PyStackChunk **datastack_chunk, PyObject ***datastack_top,
                                            PyObject ***datastack_limit);
#endif


/* Raw copy the tstate fields from PyThreadState to our local structure. */
static void PyTealetTstate_Get(PyTealetTstate *dst, const PyThreadState *src, int with_context) {
#if defined(PY_HAS_TSTATE_RECURSION_DEPTH)
    dst->recursion_depth = src->recursion_depth;
#elif defined(PY_HAS_TSTATE_RECURSION_REMAINING)
    dst->recursion_remaining = src->recursion_remaining;
    dst->recursion_limit = src->recursion_limit;
#else /* 3.12+ */
    dst->py_recursion_remaining = src->py_recursion_remaining;
    dst->py_recursion_limit = src->py_recursion_limit;
#if defined(PY_HAS_TSTATE_C_RECURSION_REMAINING)
    dst->c_recursion_remaining = src->c_recursion_remaining;
#endif
#endif

    /* context follows stricter transfer rules than other members*/
    if (with_context) {
        assert(dst->context == NULL); /* it should have been cleared on last PUT */
        dst->context = src->context;
    }
#if defined(PY_HAS_TSTATE_DELETE_LATER)
    dst->delete_later = src->delete_later;
#elif defined(PY312)
    dst->trash_delete_nesting = src->trash.delete_nesting;
#else /* 3.10-3.11 */
    dst->trash_delete_nesting = src->trash_delete_nesting;
#endif
    PyTealetTstate_GetFrame(&dst->frame_data, src);
}

/* Raw copy previously saved tealet tstate into PyThreadState. */
static void PyTealetTstate_Put(PyTealetTstate *src, PyThreadState *dst) {
#if defined(PY_HAS_TSTATE_RECURSION_DEPTH)
    dst->recursion_depth = src->recursion_depth;
#elif defined(PY_HAS_TSTATE_RECURSION_REMAINING)
    dst->recursion_remaining = src->recursion_remaining;
    dst->recursion_limit = src->recursion_limit;
#else /* 3.12+ */
    dst->py_recursion_remaining = src->py_recursion_remaining;
    dst->py_recursion_limit = src->py_recursion_limit;
#if defined(PY_HAS_TSTATE_C_RECURSION_REMAINING)
    dst->c_recursion_remaining = src->c_recursion_remaining;
#endif
#endif

    dst->context = src->context;
    dst->context_ver++;  /* Invalidate contextvars cache */
    src->context = NULL; /* ownership transferred to live tstate */
#if defined(PY_HAS_TSTATE_DELETE_LATER)
    dst->delete_later = src->delete_later;
#elif defined(PY312)
    dst->trash.delete_nesting = src->trash_delete_nesting;
#else /* 3.10-3.11 */
    dst->trash_delete_nesting = src->trash_delete_nesting;
#endif
    PyTealetTstate_PutFrame(&src->frame_data, dst);
}

/* Increment and decrement the reference count of the tstate's references.
 * we need to Increment the references when we create new tealets from an
 * existing one (or main), and decrement when a tealet terminates.
 */
static void PyTealetTstate_IncRef(PyTealetTstate *saved, int with_context) {
    assert(saved->has_state == 1);
#if defined(PY_HAS_TSTATE_DELETE_LATER)
    Py_XINCREF(saved->delete_later);
#endif
    /* exc_info is a pointer to exc_state or a stack item, so we don't own a
     * reference to it */
    if (with_context)
        Py_XINCREF(saved->context);
}

static void PyTealetTstate_DecRef(PyTealetTstate *saved, tealet_t *dustbin_tealet, int with_context) {
    assert(saved->has_state == 1);

#if defined(PY_HAS_TSTATE_DELETE_LATER)
    PyTealet_XDECREF(dustbin_tealet, saved->delete_later);
#endif
    if (with_context)
        PyTealet_CLEAR(dustbin_tealet, saved->context);
}

/* Hygiene helper: clear active Python thread state slots.
 * Enabled only when PYTEALET_ENABLE_TSTATE_HYGIENE is defined.
 */
static void PyTealetTstate_ClearPy(PyThreadState *py_tstate) {
#if defined(PYTEALET_ENABLE_TSTATE_HYGIENE)
#if defined(PY_HAS_TSTATE_RECURSION_DEPTH)
    py_tstate->recursion_depth = 0;
#elif defined(PY_HAS_TSTATE_RECURSION_REMAINING)
    py_tstate->recursion_remaining = 0;
    py_tstate->recursion_limit = 0;
#else /* 3.12+ */
    py_tstate->py_recursion_remaining = 0;
    py_tstate->py_recursion_limit = 0;
#if defined(PY_HAS_TSTATE_C_RECURSION_REMAINING)
    py_tstate->c_recursion_remaining = 0;
#endif
#endif
#if defined(PY_HAS_TSTATE_DELETE_LATER)
    py_tstate->delete_later = NULL;
#elif defined(PY312)
    py_tstate->trash.delete_nesting = 0;
#else /* 3.10-3.11 */
    py_tstate->trash_delete_nesting = 0;
#endif
    py_tstate->context = NULL;

#endif

    /* Delegate frame-related slots to the shared frame clear helper.
     * In !NDEBUG builds this also applies the exc_info sentinel used by Restore asserts.
     */
    PyTealetTstate_ClearFrame(NULL, py_tstate);
}

/* Hygiene helper: clear frame-related state in either python's or our saved
 * tstate struct. Enabled only when PYTEALET_ENABLE_TSTATE_HYGIENE is defined.
 */
static void PyTealetTstate_ClearFrame(PyTealetTstateFrame *ttstate, PyThreadState *tstate) {
    /* assert exactly one of tstate and ttstate is non-null */
    assert((tstate == NULL) != (ttstate == NULL));

#if !defined(NDEBUG)
    /* Save/restore invariant: NULL marks the in-between (cleared) live tstate. */
    if (ttstate) {
        ttstate->exc_info = NULL;
    } else {
        tstate->exc_info = NULL;
    }
#endif

#if defined(PYTEALET_ENABLE_TSTATE_HYGIENE)
    if (ttstate) {
#if defined(PY_HAS_TSTATE_CUREXC_FIELDS)
        ttstate->curexc_type = NULL;
        ttstate->curexc_value = NULL;
        ttstate->curexc_traceback = NULL;
#endif
        memset(&ttstate->exc_state, 0, sizeof(ttstate->exc_state));
        ttstate->exc_info = &ttstate->exc_state;
#if defined(PY_HAS_TSTATE_FRAME)
        ttstate->frame = NULL;
#endif
#if defined(PY_HAS_TSTATE_CURRENT_EXECUTOR)
        ttstate->current_executor = NULL;
#endif
#if defined(PY_HAS_TSTATE_CRITICAL_SECTION)
        ttstate->critical_section = 0;
#endif
#if defined(PY_HAS_TSTATE_DATASTACK)
#if defined(PY_HAS_TSTATE_CFRAME)
        ttstate->cframe = NULL;
#else
        ttstate->current_frame = NULL;
#endif
        ttstate->datastack_chunk = NULL;
        ttstate->datastack_top = NULL;
        ttstate->datastack_limit = NULL;
#endif
    } else {
#if defined(PY_HAS_TSTATE_CUREXC_FIELDS)
        tstate->curexc_type = NULL;
        tstate->curexc_value = NULL;
        tstate->curexc_traceback = NULL;
#endif
        memset(&tstate->exc_state, 0, sizeof(tstate->exc_state));
        tstate->exc_info = &tstate->exc_state;
#if defined(PY_HAS_TSTATE_FRAME)
        tstate->frame = NULL;
#endif
#if defined(PY_HAS_TSTATE_CURRENT_EXECUTOR)
        tstate->current_executor = NULL;
#endif
#if defined(PY_HAS_TSTATE_CRITICAL_SECTION)
        tstate->critical_section = 0;
#endif
#if defined(PY_HAS_TSTATE_DATASTACK)
#if defined(PY_HAS_TSTATE_CFRAME)
        tstate->cframe = NULL;
#else
        tstate->current_frame = NULL;
#endif
        tstate->datastack_chunk = NULL;
        tstate->datastack_top = NULL;
        tstate->datastack_limit = NULL;
#endif
    }
#else
    (void)ttstate;
    (void)tstate;
#endif
}

/* Debug-only hygiene helper: verify sentinel clear state. */
static void PyTealetTstate_AssertClearPy(PyThreadState *py_tstate) {
#if defined(Py_DEBUG)
    /* We use exc_info == NULL as a sentinel for the save/restore in-between state. */
    assert(py_tstate->exc_info == NULL);
#else
    (void)py_tstate;
#endif
}

void PyTealetTstate_Init(PyTealetTstate *saved) {
    saved->has_state = 0;
    saved->context = NULL;
}

/* copy the thread state, e.g. when we create a stub, or when we save current and
 * continue in a new tealet */
void PyTealetTstate_Copy(PyTealetTstate *dst, PyThreadState *src, int dst_is_new, int with_context) {
    assert(dst->has_state == 0);
    PyTealetTstate_Get(dst, src, with_context);
    dst->has_state = 1;
    /* the new tealet must have a fresh frame stack, they can't be shared */
    if (dst_is_new) {
        PyTealetTstate_ClearFrame(&dst->frame_data, NULL);
    } else {
        PyTealetTstate_ClearFrame(NULL, src);
    }
    PyTealetTstate_IncRef(dst, with_context);
}

/* undo the copy operation in case of error. In particular, we must restore
 * any cleared frame data in the python tstate if we cleared it previously
 */
void PyTealetTstate_UndoCopy(PyTealetTstate *dst, PyThreadState *src, int dst_is_new) {
    assert(dst->has_state == 1);
    if (!dst_is_new) {
        PyTealetTstate_PutFrame(&dst->frame_data, src);
    }
    PyTealetTstate_DecRef(dst, NULL, 1);
    dst->has_state = 0;
}

/* duplicate a thread state, e.g. when duplicating a tealet */
void PyTealetTstate_Duplicate(PyTealetTstate *dst, const PyTealetTstate *src) {
    assert(dst->has_state == 0);
    assert(src->has_state == 1);
    *dst = *src;
    dst->has_state = 1;
    PyTealetTstate_IncRef(dst, 1);
}

/* drop our own threadstate refs, e.g. after failure, or at tealet end */
void PyTealetTstate_Drop(PyTealetTstate *dst, tealet_t *dustbin_tealet, int with_context) {
    if (with_context) {
        /* context can live outside the has_state flag*/
        PyTealet_CLEAR(dustbin_tealet, dst->context);
    }
    if (!dst->has_state)
        return;

    PyTealetTstate_DecRef(dst, dustbin_tealet, with_context);
    dst->has_state = 0;
}

int PyTealetTstate_Visit(const PyTealetTstate *saved, visitproc visit, void *arg) {
    Py_VISIT(saved->context);
#if defined(PY_HAS_TSTATE_DELETE_LATER)
    if (saved->has_state)
        Py_VISIT(saved->delete_later);
#endif
    return 0;
}

/* Move out the thread state to a saved struct before switch.
 * The caller restores it afterwards. */
void PyTealetTstate_Save(PyTealetTstate *dst, PyThreadState *src) {
    assert(dst->has_state == 0);
    PyTealetTstate_Get(dst, src, 1);
    PyTealetTstate_ClearPy(src);
    dst->has_state = 1;
}

/* Restore the thread state after someone has saved it. */
void PyTealetTstate_Restore(PyTealetTstate *src, PyThreadState *dst) {
    assert(src->has_state == 1);
    PyTealetTstate_AssertClearPy(dst);
    PyTealetTstate_Put(src, dst);
    src->has_state = 0;
}

/* Frame state functions. We treat the frame part of the thread state with specific semantics
 * to ensure proper isolation and management of frame-related resources.
 */

/* Get frame state info from PyThreadState into private tstate. */
static void PyTealetTstate_GetFrame(PyTealetTstateFrame *dst, const PyThreadState *src) {
#if defined(PY_HAS_TSTATE_CUREXC_FIELDS)
    dst->curexc_type = src->curexc_type;
    dst->curexc_value = src->curexc_value;
    dst->curexc_traceback = src->curexc_traceback;
#endif

    dst->exc_state = src->exc_state;
    /* Keep saved exc_info self-contained when it points at exc_state. */
    if (src->exc_info == &src->exc_state)
        dst->exc_info = &dst->exc_state;
    else
        dst->exc_info = src->exc_info;

#if defined(PY_HAS_TSTATE_FRAME)
    dst->frame = src->frame;
#endif
#if defined(PY_HAS_TSTATE_CURRENT_EXECUTOR)
    dst->current_executor = src->current_executor;
#endif
#if defined(PY_HAS_TSTATE_CRITICAL_SECTION)
    dst->critical_section = src->critical_section;
#endif
#if defined(PY_HAS_TSTATE_CFRAME)
    dst->cframe = src->cframe;
#endif
#if defined(PY_HAS_TSTATE_DATASTACK)
#if defined(PY_HAS_TSTATE_CFRAME)
    dst->current_frame = src->cframe ? (void *)src->cframe->current_frame : NULL;
#else
    dst->current_frame = (void *)src->current_frame;
#endif
#if defined(PY_HAS_TSTATE_CFRAME_USE_TRACING)
    dst->cframe_use_tracing = src->cframe ? src->cframe->use_tracing : 0;
#endif
    dst->datastack_chunk = src->datastack_chunk;
    dst->datastack_top = src->datastack_top;
    dst->datastack_limit = src->datastack_limit;
#endif
}

/* Write tealet tstate frame info into PyThreadState. */
static void PyTealetTstate_PutFrame(const PyTealetTstateFrame *src, PyThreadState *dst) {
#if defined(PY_HAS_TSTATE_CUREXC_FIELDS)
    dst->curexc_type = src->curexc_type;
    dst->curexc_value = src->curexc_value;
    dst->curexc_traceback = src->curexc_traceback;
#endif

    dst->exc_state = src->exc_state;
    if (src->exc_info == &src->exc_state)
        dst->exc_info = &dst->exc_state;
    else
        dst->exc_info = src->exc_info;

#if defined(PY_HAS_TSTATE_FRAME)
    dst->frame = src->frame;
#endif
#if defined(PY_HAS_TSTATE_CURRENT_EXECUTOR)
    dst->current_executor = src->current_executor;
#endif
#if defined(PY_HAS_TSTATE_CRITICAL_SECTION)
    dst->critical_section = src->critical_section;
#endif
#if defined(PY_HAS_TSTATE_CFRAME)
    dst->cframe = src->cframe;
#endif
#if defined(PY_HAS_TSTATE_DATASTACK)
#if defined(PY_HAS_TSTATE_CFRAME)
    if (dst->cframe) {
#if defined(PY_HAS_TSTATE_CFRAME_USE_TRACING)
        dst->cframe->use_tracing = src->cframe_use_tracing;
#endif
        dst->cframe->current_frame = src->current_frame;
    }
#else
    dst->current_frame = src->current_frame;
#endif
    dst->datastack_chunk = src->datastack_chunk;
    dst->datastack_top = src->datastack_top;
    dst->datastack_limit = src->datastack_limit;
#endif
}

/* Setup/clear frame-like state.
 * - ttstate must be non-NULL.
 * - tstate must be non-NULL.
 * - target_is_tstate=0 clears saved ttstate frame slots.
 * - target_is_tstate=1 clears live tstate frame slots.
 * - For cframe builds, frame_data->top_cframe is initialized from
 *   tstate->root_cframe, then normalized into a standalone root frame
 *   (previous points to live root_cframe, current_frame is NULL).
 *   frame_data->cframe and live tstate->cframe are then wired to top_cframe.
 */
void PyTealetTstate_Frame_Setup(PyTealetTstate *ttstate, PyThreadState *tstate, int target_is_tstate) {
    PyTealetTstateFrame *frame_data;

    assert(tstate && ttstate);
    frame_data = &ttstate->frame_data;

#if defined(PY_HAS_TSTATE_CFRAME)
    /* A cleared cframe can't be NULL; anchor it in branch-local storage. */
    frame_data->top_cframe = tstate->root_cframe;
    frame_data->top_cframe.previous = &tstate->root_cframe;
#if PY311P
    frame_data->top_cframe.current_frame = NULL;
#endif
    frame_data->cframe = &frame_data->top_cframe;
#endif

    if (!target_is_tstate) {
#if defined(PY_HAS_TSTATE_CUREXC_FIELDS)
        frame_data->curexc_type = NULL;
        frame_data->curexc_value = NULL;
        frame_data->curexc_traceback = NULL;
#endif
        memset(&frame_data->exc_state, 0, sizeof(frame_data->exc_state));
        frame_data->exc_info = &frame_data->exc_state;
#if defined(PY_HAS_TSTATE_FRAME)
        frame_data->frame = NULL;
#endif
#if defined(PY_HAS_TSTATE_CURRENT_EXECUTOR)
        frame_data->current_executor = NULL;
#endif
#if defined(PY_HAS_TSTATE_CRITICAL_SECTION)
        frame_data->critical_section = 0;
#endif
#if defined(PY_HAS_TSTATE_DATASTACK)
        frame_data->current_frame = NULL;
#if defined(PY_HAS_TSTATE_CFRAME_USE_TRACING)
        frame_data->cframe_use_tracing = 0;
#endif
        frame_data->datastack_chunk = NULL;
        frame_data->datastack_top = NULL;
        frame_data->datastack_limit = NULL;
#endif
        return;
    }

#if defined(PY_HAS_TSTATE_CUREXC_FIELDS)
    tstate->curexc_type = NULL;
    tstate->curexc_value = NULL;
    tstate->curexc_traceback = NULL;
#endif
    memset(&tstate->exc_state, 0, sizeof(tstate->exc_state));
    tstate->exc_info = &tstate->exc_state;
#if defined(PY_HAS_TSTATE_FRAME)
    /* CPython treats tstate->frame as a borrowed reference from the active stack frame. */
    tstate->frame = NULL;
#endif
#if defined(PY_HAS_TSTATE_CURRENT_EXECUTOR)
    /* Hygiene check: copy/clear path should have detached any active executor. */
#if defined(PYTEALET_ENABLE_TSTATE_HYGIENE)
    assert(tstate->current_executor == NULL);
#endif
    /* Runtime safety: do not inherit executor state across tealet branches. */
    tstate->current_executor = NULL;
#endif
#if defined(PY_HAS_TSTATE_CRITICAL_SECTION)
    /* Runtime safety: no-GIL critical-section chains point into C stack frames. */
    tstate->critical_section = 0;
#endif

#if defined(PY_HAS_TSTATE_CFRAME)
    tstate->cframe = frame_data->cframe;
#endif

#if defined(PY_HAS_TSTATE_DATASTACK)
#if !defined(PY_HAS_TSTATE_CFRAME)
    /* 3.13+: no cframe anchor exists; detach from any prior interpreter frame. */
    tstate->current_frame = NULL;
#endif
    /* Hygiene check: copy/clear path should already have detached datastack. */
#if defined(PYTEALET_ENABLE_TSTATE_HYGIENE)
    assert(tstate->datastack_chunk == NULL);
    assert(tstate->datastack_top == NULL);
    assert(tstate->datastack_limit == NULL);
#endif
    /* Runtime safety: always start this branch with a detached datastack. */
    tstate->datastack_chunk = NULL;
    tstate->datastack_top = NULL;
    tstate->datastack_limit = NULL;
#endif
}

/* clean up the frame state, including releasing the local frame stack */
void PyTealetTstate_Frame_Cleanup(PyThreadState *tstate, tealet_t *dustbin_tealet) {
#if defined(PY_HAS_TSTATE_CUREXC_FIELDS)
    PyTealet_CLEAR(dustbin_tealet, tstate->curexc_type);
    PyTealet_CLEAR(dustbin_tealet, tstate->curexc_value);
    PyTealet_CLEAR(dustbin_tealet, tstate->curexc_traceback);
#endif
    PyTealet_dustbin_push(dustbin_tealet, tstate->exc_state.exc_value);
    memset(&tstate->exc_state, 0, sizeof(tstate->exc_state));
    tstate->exc_info = &tstate->exc_state;

#if defined(PY_HAS_TSTATE_FRAME)
    /* CPython treats tstate->frame as a borrowed reference (3.10); do not DECREF it. */
    tstate->frame = NULL;
#endif
#if defined(PY_HAS_TSTATE_CURRENT_EXECUTOR)
    /* If a tealet exits while owning an active executor, release it. */
    PyTealet_CLEAR(dustbin_tealet, tstate->current_executor);
#endif
#if defined(PY_HAS_TSTATE_DATASTACK)
    /* if we have a datastack chunk, we need to release the frames in it before we can drop the tstate. */
    PyTealetTstate_CleanupDatastack(&tstate->datastack_chunk, &tstate->datastack_top, &tstate->datastack_limit);
    /* no need to clear the cframe pointer, since we're about to drop the tstate and the cframe is on the stack. */
#else
    (void)tstate;
#endif
    (void)dustbin_tealet;
}

/* Only available on Py311+ where datastack chunk types exist. */
#if defined(PY_HAS_TSTATE_DATASTACK)
static void PyTealetTstate_CleanupDatastack(_PyStackChunk **datastack_chunk, PyObject ***datastack_top,
                                            PyObject ***datastack_limit) {
#if defined(PY_HAS_TSTATE_DATASTACK)
    /* Free all chunks used to allocate stack frames from. */
    PyObjectArenaAllocator alloc = {0};
    _PyStackChunk *chunk = *datastack_chunk;

    if (chunk) {
        PyObject_GetArenaAllocator(&alloc);
    }
    if (alloc.free && chunk) {
        while (chunk) {
            _PyStackChunk *prev = chunk->previous;
            alloc.free(alloc.ctx, chunk, chunk->size);
            chunk = prev;
        }
    }
    *datastack_chunk = NULL;
    *datastack_top = NULL;
    *datastack_limit = NULL;
#else
    (void)datastack_chunk;
    (void)datastack_top;
    (void)datastack_limit;
#endif
}
#endif
