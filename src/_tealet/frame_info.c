/* Implementation of frame info structures.
 * To keep frame information available for dormant tealets, we need to both keep
 * an active PyFrameObject alive, and also ensure that none of the frame links
 * point to incomplete frames, because the Python internal API will then try
 * to recreate them when accessed. This will fail for tealets that have
 * their stack swapped out.
 */

#include "frame_info.h"
#include "pytealet.h"

#include <string.h>

void PyTealetFrameInfo_Init(PyTealetFrameInfo *info) {
#if defined(PYTEALET_HAS_PENDING_FRAME_INTROSPECTION)
    info->frame = NULL;
#if defined(PY312P)
    info->items = info->fixed_items;
    info->size = 0;
    info->capacity = PYTEALET_FRAMEINFO_FIXED_ITEMS;
#endif
#else
    info->unused = 0;
#endif
}


void PyTealetFrameInfo_Fini(PyTealetFrameInfo *info) {
#if defined(PYTEALET_HAS_PENDING_FRAME_INTROSPECTION) && defined(PY312P)
    if (info->items != info->fixed_items) {
        PyMem_Free(info->items);
        info->items = info->fixed_items;
    }
#else
    (void)info;
#endif
}

#if defined(PYTEALET_HAS_PENDING_FRAME_INTROSPECTION) && defined(PY312P)
static void PyTealetFrameInfo_ClearRewrites(PyTealetFrameInfo *info) { info->size = 0; }

static int PyTealetFrameInfo_IsSafeForDormantChain(const _PyInterpreterFrame *iframe) {
    if (!iframe) {
        return 0;
    }
    return !_PyFrame_IsIncomplete((_PyInterpreterFrame *)iframe);
}

static int PyTealetFrameInfo_EnsureFrameObject(_PyInterpreterFrame *iframe) {
    PyFrameObject dummy_frame;
    _PyInterpreterFrame dummy_iframe;
    PyFrameObject *back;

    if (!iframe || _PyFrame_IsIncomplete(iframe) || iframe->frame_obj) {
        return 0;
    }

    /* Force frame object creation through public API machinery. */
    dummy_frame.f_back = NULL;
    dummy_frame.f_frame = &dummy_iframe;
#if defined(FRAME_OWNED_BY_GENERATOR)
    dummy_iframe.owner = FRAME_OWNED_BY_GENERATOR;
#elif defined(FRAME_OWNED_BY_THREAD)
    dummy_iframe.owner = FRAME_OWNED_BY_THREAD;
#else
    return 0;
#endif
    dummy_iframe.previous = iframe;
    back = PyFrame_GetBack(&dummy_frame);
    Py_XDECREF(back);

    if (!iframe->frame_obj) {
        return -1;
    }
    return 0;
}

/* Rewrite-buffer strategy:
 * 1) Start with the inline fixed buffer (info->fixed_items).
 * 2) On overflow, move to heap storage and copy existing entries.
 * 3) Once on heap, grow exponentially with PyMem_Realloc.
 * This keeps the common small case allocation-free while preserving amortized
 * linear behavior for larger chains.
 */
static int PyTealetFrameInfo_RecordRewrite(PyTealetFrameInfo *info, _PyInterpreterFrame **location) {
    PyTealetFrameInfoEntry *entry;
    Py_ssize_t next_capacity;
    PyTealetFrameInfoEntry *new_items;

    if (info->size == info->capacity) {
        next_capacity = info->capacity ? info->capacity * 2 : PYTEALET_FRAMEINFO_FIXED_ITEMS;
        if (next_capacity <= info->capacity)
            next_capacity = info->capacity + 1;
        if (info->items == info->fixed_items) {
            new_items = (PyTealetFrameInfoEntry *)PyMem_Malloc((size_t)next_capacity * sizeof(PyTealetFrameInfoEntry));
            if (new_items && info->size > 0)
                memcpy(new_items, info->items, (size_t)info->size * sizeof(PyTealetFrameInfoEntry));
        } else {
            new_items = (PyTealetFrameInfoEntry *)PyMem_Realloc(info->items,
                                                                (size_t)next_capacity * sizeof(PyTealetFrameInfoEntry));
        }
        if (!new_items) {
            PyErr_NoMemory();
            return -1;
        }
        info->items = new_items;
        info->capacity = next_capacity;
    }

    entry = &info->items[info->size++];
    entry->location = location;
    entry->old_value = *location;
    return 0;
}

/* 3.12+: expose original links by restoring rewritten frame pointers */
static void PyTealetFrameInfo_ExposeFrames(PyTealetFrameInfo *info) {
    while (info->size > 0) {
        PyTealetFrameInfoEntry *entry = &info->items[--info->size];
        *entry->location = entry->old_value;
    }
}
#endif

/* 3.12+: hide unsafe/incomplete frames by rewriting frame links.
 * We visit the frame chain and intentionally re-write previous frame links to skip over
 * incomplete frames and frames that are stored by the C stack, since these can not
 * be safely traversed when the stack that they belong to is saved into heap storage.
 * Rewrites are reversed on release.
 */
static int PyTealetFrameInfo_HideFrames(PyTealetFrameInfo *info) {
#if defined(PYTEALET_HAS_PENDING_FRAME_INTROSPECTION) && defined(PY312P)
    PyFrameObject *top_frame = info->frame;
    _PyInterpreterFrame **last_link;
    _PyInterpreterFrame *iframe;
    int linked_safe_frame = 0;

    if (!top_frame) {
        return 0;
    }

    PyTealetFrameInfo_ClearRewrites(info);
    last_link = &top_frame->f_frame;
    iframe = top_frame->f_frame;
    while (iframe) {
        iframe = _PyFrame_GetFirstComplete(iframe);
        if (!iframe) {
            break;
        }

        if (PyTealetFrameInfo_IsSafeForDormantChain(iframe)) {
            linked_safe_frame = 1;
            if (PyTealetFrameInfo_EnsureFrameObject(iframe) < 0) {
                /* Best-effort frame materialization for pending introspection. */
                PyErr_Clear();
            }
            /* A complete, non-C-stack frame. If last_link did not already point to it, rewrite. */
            if (*last_link != iframe) {
                if (PyTealetFrameInfo_RecordRewrite(info, last_link) < 0) {
                    PyTealetFrameInfo_ExposeFrames(info);
                    return -1;
                }
                *last_link = iframe;
            }
            last_link = &iframe->previous;
        }
        iframe = iframe->previous;
    }

    if (!linked_safe_frame) {
        /* No safe frame chain is available. Undo any transient rewrites
         * and drop captured introspection for this suspended stack.
         */
        PyTealetFrameInfo_ExposeFrames(info);
        Py_CLEAR(info->frame);
        return 0;
    }

    /* handle the last link */
    if (*last_link != NULL) {
        if (PyTealetFrameInfo_RecordRewrite(info, last_link) < 0) {
            PyTealetFrameInfo_ExposeFrames(info);
            return -1;
        }
        *last_link = NULL;
    }
    return 0;
#else
    (void)info;
    return 0;
#endif
}

int PyTealetFrameInfo_Capture(PyTealetFrameInfo *info, int rewrite_chain) {
#if !defined(PYTEALET_HAS_PENDING_FRAME_INTROSPECTION)
    (void)info;
    (void)rewrite_chain;
    return 0;
#else
    PyFrameObject *frame = (PyFrameObject *)PyEval_GetFrame();
    if (!frame) {
        info->frame = NULL;
        return 0;
    }

    Py_XSETREF(info->frame, (PyFrameObject *)Py_XNewRef((PyObject *)frame));
    if (rewrite_chain && PyTealetFrameInfo_HideFrames(info) < 0) {
        /* Best-effort rewrite only: keep captured frame and clear transient error. */
        PyErr_Clear();
    }
    return 0;
#endif
}

PyObject *PyTealetFrameInfo_GetFrame(const PyTealetFrameInfo *info) {
#if !defined(PYTEALET_HAS_PENDING_FRAME_INTROSPECTION)
    (void)info;
    return NULL;
#else
    return (PyObject *)info->frame;
#endif
}

void PyTealetFrameInfo_Release(PyTealetFrameInfo *info, tealet_t *dustbin_tealet) {
#if !defined(PYTEALET_HAS_PENDING_FRAME_INTROSPECTION)
    (void)info;
    (void)dustbin_tealet;
#else
#if defined(PY312P)
    PyTealetFrameInfo_ExposeFrames(info);
#endif
    PyTealet_CLEAR(dustbin_tealet, info->frame);
#endif
}
