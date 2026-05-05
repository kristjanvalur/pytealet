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

/* we need to treat the "frame" part of the tstate specially, since we
 * can't share it among tealets.  We must be careful when we branch off
 * to a new tealet that we clear either the source or destination frame data */
static void PyTealetTstate_GetFrame(PyTealetTstate *dst, const PyThreadState *src);
static void PyTealetTstate_PutFrame(const PyTealetTstate *src, PyThreadState *dst);
/* clean the python frame structures in either python or our threadstate */
static void PyTealetTstate_ClearFrame(PyTealetTstate *ttstate, PyThreadState *tstate);
/* directly clean up datastack pointer members and clear them afterwards. */
#if defined(Py311P)
static void PyTealetTstate_CleanupDatastack(_PyStackChunk **datastack_chunk, PyObject ***datastack_top,
                                            PyObject ***datastack_limit);
#endif

/* Raw copy the tstate fields from PyThreadState to our local structure. */
static void PyTealetTstate_Get(PyTealetTstate *dst, const PyThreadState *src) {
#if defined(PY310)
    dst->recursion_depth = src->recursion_depth;
#elif defined(PY311)
    dst->recursion_remaining = src->recursion_remaining;
    dst->recursion_limit = src->recursion_limit;
#else /* 3.12+ */
    dst->py_recursion_remaining = src->py_recursion_remaining;
    dst->py_recursion_limit = src->py_recursion_limit;
    dst->c_recursion_remaining = src->c_recursion_remaining;
#endif

#if defined(PY310) || defined(PY311)
    dst->exc_type = src->curexc_type;
    dst->exc_val = src->curexc_value;
    dst->exc_tb = src->curexc_traceback;
#else
    dst->exc_type = NULL;
    dst->exc_val = NULL;
    dst->exc_tb = NULL;
#endif

    dst->exc_state = src->exc_state;
    /* Keep dst->exc_info self-contained when it points at exc_state. */
    if (src->exc_info == &src->exc_state)
        dst->exc_info = &dst->exc_state;
    else
        dst->exc_info = src->exc_info;

    dst->context = src->context;

    PyTealetTstate_GetFrame(dst, src);
}

/* Raw copy previously saved tealet tstate into PyThreadState. */
static void PyTealetTstate_Put(const PyTealetTstate *src, PyThreadState *dst) {
#if defined(PY310)
    dst->recursion_depth = src->recursion_depth;
#elif defined(PY311)
    dst->recursion_remaining = src->recursion_remaining;
    dst->recursion_limit = src->recursion_limit;
#else /* 3.12+ */
    dst->py_recursion_remaining = src->py_recursion_remaining;
    dst->py_recursion_limit = src->py_recursion_limit;
    dst->c_recursion_remaining = src->c_recursion_remaining;
#endif

#if defined(PY310) || defined(PY311)
    dst->curexc_type = src->exc_type;
    dst->curexc_value = src->exc_val;
    dst->curexc_traceback = src->exc_tb;
#endif

    dst->exc_state = src->exc_state;
    if (src->exc_info == &src->exc_state)
        dst->exc_info = &dst->exc_state;
    else
        dst->exc_info = src->exc_info;

    dst->context = src->context;
    dst->context_ver++; /* Invalidate contextvars cache */

    PyTealetTstate_PutFrame(src, dst);
}

/* Increment and decrement the reference count of the tstate's references.
 * we need to Increment the references when we create new tealets from an
 * existing one (or main), and decrement when a tealet terminates.
 */
static void PyTealetTstate_IncRef(PyTealetTstate *saved) {
    assert(saved->has_state == 1);
    Py_XINCREF(saved->exc_type);
    Py_XINCREF(saved->exc_val);
    Py_XINCREF(saved->exc_tb);
    Py_XINCREF(saved->exc_state.exc_value);
    /* exc_info is a pointer to exc_state or a stack item, so we don't own a
     * reference to it */
    Py_XINCREF(saved->context);
}

static void PyTealetTstate_DecRef(PyTealetTstate *saved, tealet_t *dustbin_tealet) {
    assert(saved->has_state == 1);
    if (dustbin_tealet) {
        PyTealet_dustbin_push(dustbin_tealet, saved->exc_type);
        PyTealet_dustbin_push(dustbin_tealet, saved->exc_val);
        PyTealet_dustbin_push(dustbin_tealet, saved->exc_tb);
        PyTealet_dustbin_push(dustbin_tealet, saved->exc_state.exc_value);
        PyTealet_dustbin_push(dustbin_tealet, saved->context);
    } else {
        Py_XDECREF(saved->exc_type);
        Py_XDECREF(saved->exc_val);
        Py_XDECREF(saved->exc_tb);
        Py_XDECREF(saved->exc_state.exc_value);
        Py_XDECREF(saved->context);
    }
}

/* Debug-only hygiene helper: clear active Python thread state slots. */
static void PyTealetTstate_ClearPy(PyThreadState *py_tstate) {
#if defined(Py_DEBUG)
#if defined(PY310) || defined(PY311)
    py_tstate->curexc_type = NULL;
    py_tstate->curexc_value = NULL;
    py_tstate->curexc_traceback = NULL;
#endif
    py_tstate->exc_info = NULL; /* use this as a sentinel, should never be null
                                   in a valid situation */
    py_tstate->exc_state.exc_value = NULL;
#if defined(PY310)
    py_tstate->recursion_depth = 0;
#elif defined(PY311)
    py_tstate->recursion_remaining = 0;
    py_tstate->recursion_limit = 0;
#else /* 3.12+ */
    py_tstate->py_recursion_remaining = 0;
    py_tstate->py_recursion_limit = 0;
    py_tstate->c_recursion_remaining = 0;
#endif
#if !defined(PY312P)
    py_tstate->trash_delete_nesting = 0;
#else /* 3.12+ */
    py_tstate->trash.delete_nesting = 0;
#endif
    py_tstate->context = NULL;
    PyTealetTstate_ClearFrame(NULL, py_tstate);
#else
    (void)py_tstate;
#endif
}

/* Debug-only hygiene helper: verify sentinel clear state. */
static void PyTealetTstate_AssertClearPy(PyThreadState *py_tstate) {
#if defined(Py_DEBUG)
    /* should never be null in a valid situation, null indicates that we
     * previously cleared it.*/
    assert(py_tstate->exc_info == NULL);
#else
    (void)py_tstate;
#endif
}

void PyTealetTstate_Init(PyTealetTstate *saved) { saved->has_state = 0; }

/* copy the thread state, e.g. when we create a stub, or when we save current and
 * continue in a new tealet */
void PyTealetTstate_Copy(PyTealetTstate *dst, PyThreadState *src, int dst_is_new) {
    assert(dst->has_state == 0);
    PyTealetTstate_Get(dst, src);
    dst->has_state = 1;
    /* the new tealet must have nulled python fields, they can't be shared */
    if (dst_is_new) {
        PyTealetTstate_ClearFrame(dst, NULL);
    } else {
        PyTealetTstate_ClearFrame(NULL, src);
    }
    PyTealetTstate_IncRef(dst);
}

/* undo the copy operation in case of error. In particular, we must restore
 * any cleared frame data in the python tstate if we cleared it previously
 */
void PyTealetTstate_UndoCopy(PyTealetTstate *dst, PyThreadState *src, int dst_is_new) {
    assert(dst->has_state == 1);
    if (!dst_is_new) {
        PyTealetTstate_PutFrame(dst, src);
    }
    PyTealetTstate_DecRef(dst, NULL);
    dst->has_state = 0;
}

/* duplicate a thread state, e.g. when duplicating a tealet */
void PyTealetTstate_Duplicate(PyTealetTstate *dst, const PyTealetTstate *src) {
    assert(dst->has_state == 0);
    assert(src->has_state == 1);
    *dst = *src;
    dst->has_state = 1;
    PyTealetTstate_IncRef(dst);
}

/* drop our own threadstate refs, e.g. after failure, or at tealet end */
void PyTealetTstate_Drop(PyTealetTstate *dst, tealet_t *dustbin_tealet) {
    if (!dst->has_state)
        return;
    PyTealetTstate_DecRef(dst, dustbin_tealet);
    dst->has_state = 0;
}

/* Move out the thread state to a saved struct before switch.
 * The caller restores it afterwards. */
void PyTealetTstate_Save(PyTealetTstate *dst, PyThreadState *src) {
    assert(dst->has_state == 0);
    PyTealetTstate_Get(dst, src);
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
static void PyTealetTstate_GetFrame(PyTealetTstate *dst, const PyThreadState *src) {
#if defined(PY_HAS_TSTATE_FRAME)
    dst->frame = src->frame;
#endif
#if defined(PY_HAS_CFRAME)
    dst->cframe = src->cframe;
#endif
#if defined(Py311P)
    dst->current_frame = src->cframe ? (void *)src->cframe->current_frame : NULL;
#if defined(PY311)
    dst->cframe_use_tracing = src->cframe ? src->cframe->use_tracing : 0;
#endif
    dst->datastack_chunk = src->datastack_chunk;
    dst->datastack_top = src->datastack_top;
    dst->datastack_limit = src->datastack_limit;
#endif
#if !defined(PY312P)
    dst->trash_delete_nesting = src->trash_delete_nesting;
#else /* 3.12+ */
    dst->trash_delete_nesting = src->trash.delete_nesting;
#endif
}

/* Write tealet tstate frame info into PyThreadState. */
static void PyTealetTstate_PutFrame(const PyTealetTstate *src, PyThreadState *dst) {
#if defined(PY_HAS_TSTATE_FRAME)
    dst->frame = src->frame;
#endif
#if defined(PY_HAS_CFRAME)
    dst->cframe = src->cframe;
#endif
#if defined(Py311P)
    if (dst->cframe) {
#if defined(PY311)
        dst->cframe->use_tracing = src->cframe_use_tracing;
#endif
        dst->cframe->current_frame = src->current_frame;
    }
    dst->datastack_chunk = src->datastack_chunk;
    dst->datastack_top = src->datastack_top;
    dst->datastack_limit = src->datastack_limit;
#endif
#if !defined(PY312P)
    dst->trash_delete_nesting = src->trash_delete_nesting;
#else /* 3.12+ */
    dst->trash.delete_nesting = src->trash_delete_nesting;
#endif
}

/* clear the python frame related structures in either python's or our tstate struct.
 * This needs to be done when we create a new tstate, when spawning a tealet, since
 * they cannot be shared between tealets.  One of the tstates keeps the existing
 * settings, and the other is cleared.
 */
static void PyTealetTstate_ClearFrame(PyTealetTstate *ttstate, PyThreadState *tstate) {
    /* assert exactly one of tstate and ttstate is non-null */
    assert((tstate == NULL) != (ttstate == NULL));
    if (ttstate) {
#if defined(PY_HAS_TSTATE_FRAME)
        ttstate->frame = NULL;
#endif
#if defined(Py311P)
#if defined(PY_HAS_CFRAME)
        ttstate->cframe = NULL;
#else
        ttstate->current_frame = NULL;
#endif
        ttstate->datastack_chunk = NULL;
        ttstate->datastack_top = NULL;
        ttstate->datastack_limit = NULL;
#endif

    } else {
#if defined(PY_HAS_TSTATE_FRAME)
        tstate->frame = NULL;
#endif
#if defined(Py311P)
#if defined(PY_HAS_CFRAME)
        tstate->cframe = NULL;
#else
        tstate->current_frame = NULL;
#endif
        tstate->datastack_chunk = NULL;
        tstate->datastack_top = NULL;
        tstate->datastack_limit = NULL;
#endif
    }
}

/* Set up the frame state object when a tealet starts running */
void PyTealetTstate_Frame_Setup(PyTealetTstate *ttstate, PyThreadState *tstate) {
#if defined(PY_HAS_TSTATE_FRAME)
    assert(ttstate->frame == NULL);
#endif
#if defined(Py311P)
    /* Entering tealet code must not inherit parent eval/datastack links from
     * another C stack.  We copy the cframe into a local variable and reset it so that
     * it has no parents.
     */
    assert(tstate->cframe == NULL);
    ttstate->top_cframe = tstate->root_cframe;
    ttstate->top_cframe.previous = &tstate->root_cframe;
    ttstate->top_cframe.current_frame = NULL;
    tstate->cframe = &ttstate->top_cframe;

    /* These should be NULL because we copied and then cleared frame state. */
    assert(tstate->datastack_chunk == NULL);
    assert(tstate->datastack_top == NULL);
    assert(tstate->datastack_limit == NULL);
#endif
}

/* clean up the frame state, including releasing the local frame stack */
void PyTealetTstate_Frame_Cleanup(PyTealetTstate *ttstate, PyThreadState *tstate, tealet_t *dustbin_tealet) {
#if defined(PY_HAS_TSTATE_FRAME)
    PyTealet_dustbin_push(dustbin_tealet, (PyObject *)tstate->frame);
    tstate->frame = NULL;
#endif
#if defined(Py311P)
    /* if we have a datastack chunk, we need to release the frames in it before we can drop the tstate. */
    PyTealetTstate_CleanupDatastack(&tstate->datastack_chunk, &tstate->datastack_top, &tstate->datastack_limit);
    /* no need to clear the cframe pointer, since we're about to drop the tstate and the cframe is on the stack. */
#else
    (void)tstate;
#endif
    (void)ttstate;
    (void)dustbin_tealet;
}

/* Only available on Py311+ where datastack chunk types exist. */
#if defined(Py311P)
static void PyTealetTstate_CleanupDatastack(_PyStackChunk **datastack_chunk, PyObject ***datastack_top,
                                            PyObject ***datastack_limit) {
#if defined(Py311P)
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