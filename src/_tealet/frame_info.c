#include "frame_info.h"

#include <stdlib.h>

#if defined(PY312P)
#include "internal/pycore_frame.h"

typedef struct PyTealetFrameInfoEntry {
    _PyInterpreterFrame **location;
    _PyInterpreterFrame *old_value;
} PyTealetFrameInfoEntry;
#endif

void PyTealetFrameInfo_Init(PyTealetFrameInfo *info) {
#if !defined(PY_HAS_TSTATE_FRAME)
    info->frame = NULL;
#if defined(PY312P)
    info->items = NULL;
    info->size = 0;
    info->capacity = 0;
#endif
#else
    info->unused = 0;
#endif
}

void PyTealetFrameInfo_Fini(PyTealetFrameInfo *info) {
#if defined(PY312P)
    free(info->items);
    info->items = NULL;
    info->size = 0;
    info->capacity = 0;
#else
    (void)info;
#endif
}

#if defined(PY312P) && !defined(PY_HAS_TSTATE_FRAME)
static void PyTealetFrameInfo_ClearRewrites(PyTealetFrameInfo *info) { info->size = 0; }

static int PyTealetFrameInfo_RecordRewrite(PyTealetFrameInfo *info, _PyInterpreterFrame **location) {
    PyTealetFrameInfoEntry *entry;
    PyTealetFrameInfoEntry *items;
    Py_ssize_t next_capacity;
    void *new_items;

    if (info->size == info->capacity) {
        next_capacity = info->capacity ? info->capacity * 2 : 8;
        new_items = realloc(info->items, (size_t)next_capacity * sizeof(PyTealetFrameInfoEntry));
        if (!new_items) {
            PyErr_NoMemory();
            return -1;
        }
        info->items = new_items;
        info->capacity = next_capacity;
    }

    items = (PyTealetFrameInfoEntry *)info->items;
    entry = &items[info->size++];
    entry->location = location;
    entry->old_value = *location;
    return 0;
}

/* 3.12+: expose original links by restoring rewritten frame pointers */
static void PyTealetFrameInfo_ExposeFrames(PyTealetFrameInfo *info) {
    PyTealetFrameInfoEntry *items = (PyTealetFrameInfoEntry *)info->items;
    while (info->size > 0) {
        PyTealetFrameInfoEntry *entry = &items[--info->size];
        *entry->location = entry->old_value;
    }
}
#endif

/* 3.12+: hide unsafe/incomplete frames by rewriting frame links.
 * We visit the frame chain and intentionally re-write previous frame links to skip over
 * incomplete frames and frames that are stored by the C stack, since these can not
 * be safely traversed when the stack that they belong to is saved into heap storage.
 * TODO: We should also terminate the frame chain early if we encounter a frame that
 * is outside the current tealet.
 */
static int PyTealetFrameInfo_HideFrames(PyTealetFrameInfo *info) {
#if defined(PY312P)
    PyFrameObject *top_frame = info->frame;
    _PyInterpreterFrame **last_link;
    _PyInterpreterFrame *iframe;

    if (!top_frame) {
        return 0;
    }

    PyTealetFrameInfo_ClearRewrites(info);
    last_link = &top_frame->f_frame;
    iframe = top_frame->f_frame;
    while (iframe) {
        if (!_PyFrame_IsIncomplete(iframe) && iframe->owner != FRAME_OWNED_BY_CSTACK) {
            /* a complete frame. if the last link didn't point to it, rewrite. */
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
#if defined(PY_HAS_TSTATE_FRAME)
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
#if defined(PY_HAS_TSTATE_FRAME)
    (void)info;
    return NULL;
#else
    return (PyObject *)info->frame;
#endif
}

void PyTealetFrameInfo_Release(PyTealetFrameInfo *info, tealet_t *dustbin_tealet) {
#if defined(PY_HAS_TSTATE_FRAME)
    (void)info;
    (void)dustbin_tealet;
#else
#if defined(PY312P)
    PyTealetFrameInfo_ExposeFrames(info);
#endif
    if (dustbin_tealet) {
        dustbin_push(dustbin_tealet, (PyObject *)info->frame);
        info->frame = NULL;
    } else {
        Py_CLEAR(info->frame);
    }
#endif
}