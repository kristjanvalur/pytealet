/* tstate_state.h - functions for saving/restoring the PyThreadState state
 *
 * This module provides functions to save and restore the state of a PyThreadState
 * struct, which is necessary when switching between different "tealets" (lightweight
 * execution contexts) that may run on the same OS thread. The state includes the
 * current frame, exception state, recursion state, and other relevant fields.
 * The different implementations for different Python versions are handled with conditional
 * compilation in this file and the corresponding .c file
 *
 * The main functions are:
 * - PyTealetTstate_Init: Initializes a PyTealetTstate struct to an empty state.
 * - PyTealetTstate_Copy: Copies the state from a PyThreadState to a PyTealetTstate and increments references as needed.
 * - PyTealetTstate_Duplicate: Copies one saved PyTealetTstate into another and increments owned references.
 * - PyTealetTstate_Drop: Decrements reference counts for any Python objects in the saved state and clears it.
 * - PyTealetTstate_Save: Saves the current state from a PyThreadState into a PyTealetTstate, incrementing reference
 * counts as needed.
 * - PyTealetTstate_Restore: Restores the saved state from a PyTealetTstate into a PyThreadState, decrementing reference
 * counts as needed.
 *
 * These functions ensure that when we switch between tealets, we properly manage the Python-level state and avoid
 * memory leaks or crashes due to dangling references.
 */

#ifndef PYTEALET_TSTATE_STATE_H
#define PYTEALET_TSTATE_STATE_H

#include "Python.h"
#include "frameobject.h"

#include "pytealet_common.h"
#include "tealet.h"

    typedef struct PyTealetTstateFrame {
        /* current exception state */
    #if defined(PY_HAS_TSTATE_CUREXC_FIELDS)
        PyObject *curexc_type;
        PyObject *curexc_value;
        PyObject *curexc_traceback;
    #endif
        _PyErr_StackItem *exc_info;
        _PyErr_StackItem exc_state;
#if defined(PY_HAS_TSTATE_FRAME)
        PyFrameObject *frame;
#endif
#if defined(PY_HAS_TSTATE_CFRAME)
        /* Python 3.10-3.12: cframe tracks C-level call frames (removed in 3.13)
         * Stack-slicing preserves the CFrame struct itself; we just save the
         * pointer */
        PyTealetCFrame *cframe;
#endif
#if defined(PY_HAS_TSTATE_DATASTACK)
#if defined(PY_HAS_TSTATE_CFRAME)
        PyTealetCFrame top_cframe;
#endif
#if defined(PY_HAS_TSTATE_CFRAME_USE_TRACING)
        int cframe_use_tracing; /* tracing flag from cframe */
#endif
        /* new in 3.11, these four must be preserved together */
        void *current_frame; /* tstate->cstate->current_frame, or in 3.13plus, tstate->current_frame */
        _PyStackChunk *datastack_chunk;
        PyObject **datastack_top;
        PyObject **datastack_limit;
#endif
#if defined(PY_HAS_TSTATE_CURRENT_EXECUTOR)
        /* CPython tier2/JIT active executor. Treat as frame-like state: moved
         * with execution context and nulled for fresh branches. */
        PyObject *current_executor;
    #endif
    } PyTealetTstateFrame;

    typedef struct PyTealetTstate {
        int has_state; /* Debug helper: 1 when this struct currently stores a saved
                          tstate */

        /* current recursion state */
    #if defined(PY_HAS_TSTATE_RECURSION_DEPTH)
        int recursion_depth;
    #elif defined(PY_HAS_TSTATE_RECURSION_REMAINING)
        int recursion_remaining;
        int recursion_limit;
    #else /* 3.12+ */
        int py_recursion_remaining;
        int py_recursion_limit;
    #if defined(PY_HAS_TSTATE_C_RECURSION_REMAINING)
        int c_recursion_remaining;
    #endif
    #endif

    #if defined(PY_HAS_TSTATE_DELETE_LATER)
        PyObject *delete_later; /* Python 3.13+: trash queue head on tstate */
    #else
        int trash_delete_nesting; /* destructor nesting level, conserved. */
    #endif

        /* context pointer can be valid even if has_state is 0.  if non, null
         * in this struct, we own a reference to it.
         * this pointer can be accessed via api functions to set and get a the
         * tealet context both prior to and during execution.
         */
        PyObject *context; /* Python 3.7+ contextvars */

        /* frame-like execution state that cannot be shared between branches */
        PyTealetTstateFrame frame_data;
    } PyTealetTstate;

void PyTealetTstate_Init(PyTealetTstate *saved);

/* copy the Python thread state.
 * Frame-related state is isolated for new tealet creation:
 * - dst_is_new=1 clears frame fields in dst
 * - dst_is_new=0 clears frame fields in src
 */
void PyTealetTstate_Copy(PyTealetTstate *dst, PyThreadState *src, int dst_is_new);
/* undo the copy operation in case of error */
void PyTealetTstate_UndoCopy(PyTealetTstate *dst, PyThreadState *src, int dst_is_new);

/* duplicate a saved threadstate, e.g. when duplicating a tealet */
void PyTealetTstate_Duplicate(PyTealetTstate *dst, const PyTealetTstate *src);

/* drop the thread state, e.g. on error or when cleaning up */
void PyTealetTstate_Drop(PyTealetTstate *dst, tealet_t *dustbin_tealet);

/* save the current thread state into the tealet state */
void PyTealetTstate_Save(PyTealetTstate *dst, PyThreadState *src);

void PyTealetTstate_Restore(PyTealetTstate *src, PyThreadState *dst);

/* python frame state initialization and cleanup*/

/* Set up the frame state object when a tealet starts running */
void PyTealetTstate_Frame_Setup(PyTealetTstate *ttstate, PyThreadState *tstate);

/* clean up the frame state, including releasing the local frame stack */
void PyTealetTstate_Frame_Cleanup(PyThreadState *tstate, tealet_t *dustbin_tealet);

#endif
