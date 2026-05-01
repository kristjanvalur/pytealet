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

/* Raw copy the tstate files from PyThreadState to our local structure */
static void PyTealetTstate_Get(PyTealetTstate *dst, const PyThreadState *src) {
#if defined(PY_HAS_TSTATE_FRAME)
    dst->frame = src->frame;
#endif
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

#if defined(PY_HAS_CFRAME)
    dst->cframe = src->cframe;
#endif
#if defined(Py311P)
#if defined(PY_HAS_CFRAME)
    dst->current_frame = src->cframe ? (void *)src->cframe->current_frame : NULL;
#else
    dst->current_frame = (void *)src->current_frame;
#endif
#if defined(PY311)
    dst->cframe_use_tracing = src->cframe ? src->cframe->use_tracing : 0;
#endif
    dst->datastack_chunk = src->datastack_chunk;
    dst->datastack_top = src->datastack_top;
    dst->datastack_limit = src->datastack_limit;
#endif
#if defined(PY312)
    dst->trash_delete_nesting = src->trash.delete_nesting;
#elif defined(PY313P)
    dst->delete_later = src->delete_later;
#else
    dst->trash_delete_nesting = src->trash_delete_nesting;
#endif
}

/* Raw copy previously saved tealet tstate into PyThreadState. */
static void PyTealetTstate_Put(const PyTealetTstate *src, PyThreadState *dst) {
#if defined(PY_HAS_TSTATE_FRAME)
    dst->frame = src->frame;
#endif
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

#if defined(PY_HAS_CFRAME)
    dst->cframe = src->cframe;
#endif
#if defined(Py311P)
#if defined(PY_HAS_CFRAME)
    if (dst->cframe) {
#if defined(PY311)
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
#if defined(PY312)
    dst->trash.delete_nesting = src->trash_delete_nesting;
#elif defined(PY313P)
    dst->delete_later = src->delete_later;
#else
    dst->trash_delete_nesting = src->trash_delete_nesting;
#endif
}

/* Increment and decrement the reference count of the tstate's references.
 * we need to Increment the references when we create new tealets from an
 * existing one (or main), and decrement when a tealet terminates.
 */
static void PyTealetTstate_IncRef(PyTealetTstate *saved) {
    assert(saved->has_state == 1);
#if defined(PY_HAS_TSTATE_FRAME)
    Py_XINCREF(saved->frame);
#endif
    Py_XINCREF(saved->exc_type);
    Py_XINCREF(saved->exc_val);
    Py_XINCREF(saved->exc_tb);
    Py_XINCREF(saved->exc_state.exc_value);
#if defined(PY313P)
    Py_XINCREF(saved->delete_later);
#endif
    /* exc_info is a pointer to exc_state or a stack item, so we don't own a
     * reference to it */
    Py_XINCREF(saved->context);
}

static void PyTealetTstate_DecRef(PyTealetTstate *saved, tealet_t *dustbin_tealet) {
    assert(saved->has_state == 1);
    if (dustbin_tealet) {
#if defined(PY_HAS_TSTATE_FRAME)
        PyTealet_dustbin_push(dustbin_tealet, (PyObject *)saved->frame);
#endif
        PyTealet_dustbin_push(dustbin_tealet, saved->exc_type);
        PyTealet_dustbin_push(dustbin_tealet, saved->exc_val);
        PyTealet_dustbin_push(dustbin_tealet, saved->exc_tb);
        PyTealet_dustbin_push(dustbin_tealet, saved->exc_state.exc_value);
    #if defined(PY313P)
        PyTealet_dustbin_push(dustbin_tealet, saved->delete_later);
    #endif
        PyTealet_dustbin_push(dustbin_tealet, saved->context);
    } else {
#if defined(PY_HAS_TSTATE_FRAME)
        Py_XDECREF(saved->frame);
#endif
        Py_XDECREF(saved->exc_type);
        Py_XDECREF(saved->exc_val);
        Py_XDECREF(saved->exc_tb);
        Py_XDECREF(saved->exc_state.exc_value);
    #if defined(PY313P)
        Py_XDECREF(saved->delete_later);
    #endif
        Py_XDECREF(saved->context);
    }
}

/* Debug-only hygiene helper: clear active Python thread state slots. */
static void PyTealetTstate_ClearPy(PyThreadState *py_tstate) {
#if defined(Py_DEBUG)
#if defined(PY_HAS_TSTATE_FRAME)
    py_tstate->frame = NULL;
#endif
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
#if defined(PY312)
    py_tstate->trash.delete_nesting = 0;
#elif defined(PY313P)
    py_tstate->delete_later = NULL;
#else
    py_tstate->trash_delete_nesting = 0;
#endif
    py_tstate->context = NULL;
#if defined(PY_HAS_CFRAME)
    py_tstate->cframe = NULL;
#endif
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

void PyTealetTstate_Init(PyTealetTstate *saved) {
    saved->has_state = 0;
#if defined(PY313P)
    saved->delete_later = NULL;
#endif
}

/* copy the threadstate, e.g. when we create a stub */
void PyTealetTstate_Copy(PyTealetTstate *dst, const PyThreadState *src) {
    assert(dst->has_state == 0);
    PyTealetTstate_Get(dst, src);
    dst->has_state = 1;
    PyTealetTstate_IncRef(dst);
}

/* duplicate a threadstate, e.g. when dupclicating a tealet */
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

/* Move out the threadstate to a saved struct before switch. someone will
 * restore after. */
void PyTealetTstate_Save(PyTealetTstate *dst, PyThreadState *src) {
    assert(dst->has_state == 0);
    PyTealetTstate_Get(dst, src);
    PyTealetTstate_ClearPy(src);
    dst->has_state = 1;
}

/* restore the threadstate, after someone has saved it.*/
void PyTealetTstate_Restore(PyTealetTstate *src, PyThreadState *dst) {
    assert(src->has_state == 1);
    PyTealetTstate_AssertClearPy(dst);
    PyTealetTstate_Put(src, dst);
    src->has_state = 0;
}
