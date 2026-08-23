/* tealet_profile.c - 3.12+ C profiler with per-stack timings and tealet switches.
 *
 * Uses sys.monitoring for call/return and the pytealet capsule set_trace hook
 * to swap stacks. timer "wall" (default) is monotonic/perf_counter; "thread"
 * is this thread's CPU. Wall + GIL: pause the previous thread's top frame.
 * Requires Python 3.12 or newer.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "pytealet_capi.h"

#include <stdatomic.h>
#include <stdint.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <time.h>
#endif

#if PY_VERSION_HEX < 0x030C00B0
#error "_tealet_profile requires Python 3.12 or newer"
#endif

#define ENTRY_CAPSULE "tealet._tealet_profile.entry"
#define FOLDED_CAPSULE "tealet._tealet_profile.folded"

#define TIMER_WALL 0
#define TIMER_THREAD 1

typedef struct Context {
    int64_t t0;
    int64_t spent;
    int64_t subt;
    PyObject *key;
    struct Context *previous;
} Context;

typedef struct Entry {
    PyObject *user;
    int64_t tt;
    int64_t it;
    long callcount;
    long recursivecallcount;
    long recursion_level;
    PyObject *calls;
} Entry;

typedef struct Stack {
    Context *ctx;
    PyObject *entries;
    PyObject *family;
    unsigned long thread_id;
    struct Stack *next;
} Stack;

typedef struct Folded {
    PyObject *family;
    PyObject *entries;
    PyObject *thread_ids;
    long nstacks;
} Folded;

typedef struct {
    PyObject_HEAD const PyTealet_CAPI *api;
    PyTealet_CAPI_Context *tealet_ctx;
    Py_tss_t tls_current;
    int tls_ready;
    Stack *stacks;
    PyObject *by_tealet;
    PyObject *folded;
    PyThread_type_lock lock;
    _Atomic unsigned long lock_owner;
    int lock_depth;
    int enabled;
    int builtins;
    int fold_on_exit;
    int timer_kind;
    int slice_gil;
    Stack *last_active;
    int tool_id;
    PyObject *monitoring;
    PyObject *missing;
    PyTealetApi_TraceFunc prev_trace;
    void *prev_trace_data;
    int prev_trace_data_owned;
} ProfilerObject;

static PyTypeObject ProfilerType;

static int64_t wall_ns(void) {
#ifdef _WIN32
    static LARGE_INTEGER freq;
    LARGE_INTEGER counter;

    if (freq.QuadPart == 0)
        QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&counter);
    return (int64_t)((double)counter.QuadPart * 1000000000.0 / (double)freq.QuadPart);
#else
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
#endif
}

static int64_t thread_ns(void) {
#ifdef _WIN32
    FILETIME create, exit_t, kernel, user;
    ULARGE_INTEGER krn, usr;

    if (!GetThreadTimes(GetCurrentThread(), &create, &exit_t, &kernel, &user))
        return 0;
    krn.LowPart = kernel.dwLowDateTime;
    krn.HighPart = kernel.dwHighDateTime;
    usr.LowPart = user.dwLowDateTime;
    usr.HighPart = user.dwHighDateTime;
    return (int64_t)((krn.QuadPart + usr.QuadPart) * 100);
#else
    struct timespec ts;

    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
#endif
}

static int64_t now_ns(ProfilerObject *p) {
    if (p->timer_kind == TIMER_THREAD)
        return thread_ns();
    return wall_ns();
}

static int gil_is_enabled(void) {
#ifdef Py_GIL_DISABLED
    PyObject *mod;
    PyObject *fn;
    PyObject *res;
    int on;

    mod = PyImport_ImportModule("sys");
    if (!mod) {
        PyErr_Clear();
        return 0;
    }
    fn = PyObject_GetAttrString(mod, "_is_gil_enabled");
    Py_DECREF(mod);
    if (!fn) {
        PyErr_Clear();
        return 0;
    }
    res = PyObject_CallNoArgs(fn);
    Py_DECREF(fn);
    if (!res) {
        PyErr_Clear();
        return 0;
    }
    on = PyObject_IsTrue(res);
    Py_DECREF(res);
    return on == 1;
#else
    return 1;
#endif
}

static void entry_capsule_destructor(PyObject *cap) {
    Entry *e = (Entry *)PyCapsule_GetPointer(cap, ENTRY_CAPSULE);

    if (!e)
        return;
    Py_XDECREF(e->user);
    Py_XDECREF(e->calls);
    PyMem_Free(e);
}

static Entry *entry_get_in(PyObject *entries, PyObject *key, PyObject *user, int create) {
    PyObject *cap;
    Entry *e;

    cap = PyDict_GetItemWithError(entries, key);
    if (cap)
        return (Entry *)PyCapsule_GetPointer(cap, ENTRY_CAPSULE);
    if (PyErr_Occurred() || !create)
        return NULL;
    e = (Entry *)PyMem_Calloc(1, sizeof(*e));
    if (!e)
        return (Entry *)PyErr_NoMemory();
    e->user = Py_NewRef(user);
    e->calls = PyDict_New();
    if (!e->calls) {
        Py_DECREF(e->user);
        PyMem_Free(e);
        return NULL;
    }
    cap = PyCapsule_New(e, ENTRY_CAPSULE, entry_capsule_destructor);
    if (!cap) {
        Py_DECREF(e->user);
        Py_DECREF(e->calls);
        PyMem_Free(e);
        return NULL;
    }
    if (PyDict_SetItem(entries, key, cap) < 0) {
        Py_DECREF(cap);
        return NULL;
    }
    Py_DECREF(cap);
    return e;
}

static Entry *entry_get(Stack *st, PyObject *key, PyObject *user, int create) {
    return entry_get_in(st->entries, key, user, create);
}

static int bump_callee(Entry *caller, PyObject *key) {
    PyObject *cur;
    long n = 0;
    PyObject *nxt;
    int rc;

    cur = PyDict_GetItemWithError(caller->calls, key);
    if (PyErr_Occurred())
        return -1;
    if (cur)
        n = PyLong_AsLong(cur);
    nxt = PyLong_FromLong(n + 1);
    if (!nxt)
        return -1;
    rc = PyDict_SetItem(caller->calls, key, nxt);
    Py_DECREF(nxt);
    return rc;
}

static void context_free_chain(Context *ctx) {
    while (ctx) {
        Context *prev = ctx->previous;
        Py_XDECREF(ctx->key);
        PyMem_Free(ctx);
        ctx = prev;
    }
}

static void stack_free(Stack *st) {
    context_free_chain(st->ctx);
    Py_XDECREF(st->entries);
    Py_XDECREF(st->family);
    PyMem_Free(st);
}

static void profiler_lock(ProfilerObject *p) {
    unsigned long me;

    me = (unsigned long)PyThread_get_thread_ident();
    if (atomic_load_explicit(&p->lock_owner, memory_order_acquire) == me) {
        p->lock_depth++;
        return;
    }
    PyThread_acquire_lock(p->lock, WAIT_LOCK);
    p->lock_depth = 1;
    atomic_store_explicit(&p->lock_owner, me, memory_order_release);
}

static void profiler_unlock(ProfilerObject *p) {
    if (--p->lock_depth > 0)
        return;
    atomic_store_explicit(&p->lock_owner, 0, memory_order_release);
    PyThread_release_lock(p->lock);
}

static Stack *stack_new(ProfilerObject *p) {
    Stack *st = (Stack *)PyMem_Calloc(1, sizeof(*st));

    if (!st) {
        PyErr_NoMemory();
        return NULL;
    }
    st->entries = PyDict_New();
    if (!st->entries) {
        PyMem_Free(st);
        return NULL;
    }
    st->thread_id = PyThread_get_thread_ident();
    st->family = NULL;
    st->ctx = NULL;
    profiler_lock(p);
    st->next = p->stacks;
    p->stacks = st;
    profiler_unlock(p);
    return st;
}

static Stack *profiler_current(ProfilerObject *p) {
    if (!p->tls_ready)
        return NULL;
    return (Stack *)PyThread_tss_get(&p->tls_current);
}

static int profiler_set_current(ProfilerObject *p, Stack *st) {
    if (!p->tls_ready) {
        PyErr_SetString(PyExc_RuntimeError, "profiler TLS is not ready");
        return -1;
    }
    if (PyThread_tss_set(&p->tls_current, st) != 0) {
        PyErr_SetString(PyExc_RuntimeError, "failed to set profiler TLS");
        return -1;
    }
    return 0;
}

static Stack *profiler_ensure_current(ProfilerObject *p) {
    Stack *st = profiler_current(p);

    if (st)
        return st;
    st = stack_new(p);
    if (!st)
        return NULL;
    if (profiler_set_current(p, st) < 0)
        return NULL;
    return st;
}

static void pause_top(ProfilerObject *p, Stack *st) {
    Context *ctx;
    int64_t now;

    if (!st || !st->ctx)
        return;
    now = now_ns(p);
    for (ctx = st->ctx; ctx; ctx = ctx->previous) {
        ctx->spent += now - ctx->t0;
        ctx->t0 = now;
    }
}

static void resume_top(ProfilerObject *p, Stack *st) {
    Context *ctx;
    int64_t now;

    if (!st || !st->ctx)
        return;
    now = now_ns(p);
    for (ctx = st->ctx; ctx; ctx = ctx->previous)
        ctx->t0 = now;
}

static void gil_handoff(ProfilerObject *p, Stack *st) {
    Stack *prev;

    if (!p->slice_gil || !st)
        return;
    prev = p->last_active;
    if (prev == st)
        return;
    if (prev)
        pause_top(p, prev);
    resume_top(p, st);
    p->last_active = st;
}

static int maybe_set_family(Stack *st, PyObject *code) {
    PyObject *filename = NULL;
    PyObject *name = NULL;
    PyObject *lineno = NULL;
    PyObject *family;
    const char *fn;
    Py_ssize_t fn_len;

    if (st->family || !PyCode_Check(code))
        return 0;
    filename = PyObject_GetAttrString(code, "co_filename");
    name = PyObject_GetAttrString(code, "co_name");
    lineno = PyObject_GetAttrString(code, "co_firstlineno");
    if (!filename || !name || !lineno)
        goto error;
    fn = PyUnicode_AsUTF8AndSize(filename, &fn_len);
    if (!fn)
        goto error;
    if (fn_len >= 12 && strcmp(fn + fn_len - 12, "threading.py") == 0)
        goto skip;
    if (fn_len >= 15 && strcmp(fn + fn_len - 15, "_weakrefset.py") == 0)
        goto skip;
    if (strstr(fn, "/threading/") || strstr(fn, "\\threading\\"))
        goto skip;
    family = PyTuple_Pack(3, filename, lineno, name);
    if (!family)
        goto error;
    st->family = family;
skip:
    Py_DECREF(filename);
    Py_DECREF(name);
    Py_DECREF(lineno);
    return 0;
error:
    Py_XDECREF(filename);
    Py_XDECREF(name);
    Py_XDECREF(lineno);
    PyErr_Clear();
    return 0;
}

static int enter_call(ProfilerObject *p, PyObject *key, PyObject *user) {
    Stack *st;
    Entry *e;
    Context *ctx;
    Context *prev;
    int rc = -1;

    profiler_lock(p);
    if (!p->enabled) {
        rc = 0;
        goto done;
    }
    st = profiler_ensure_current(p);
    if (!st)
        goto done;
    gil_handoff(p, st);
    maybe_set_family(st, user);
    e = entry_get(st, key, user, 1);
    if (!e)
        goto done;
    prev = st->ctx;
    if (prev) {
        Entry *caller = entry_get(st, prev->key, prev->key, 0);
        if (caller && bump_callee(caller, key) < 0)
            goto done;
        /* parent t0 stays put; child time is added to subt on leave (_lsprof). */
    }
    ctx = (Context *)PyMem_Malloc(sizeof(*ctx));
    if (!ctx) {
        PyErr_NoMemory();
        goto done;
    }
    ctx->t0 = now_ns(p);
    ctx->spent = 0;
    ctx->subt = 0;
    ctx->key = Py_NewRef(key);
    ctx->previous = prev;
    st->ctx = ctx;
    e->recursion_level++;
    rc = 0;
done:
    profiler_unlock(p);
    return rc;
}

static int leave_call(ProfilerObject *p, PyObject *key) {
    Stack *st;
    Entry *e;
    Context *ctx;
    int64_t now;
    int64_t tt;
    int64_t it;

    profiler_lock(p);
    if (!p->enabled) {
        profiler_unlock(p);
        return 0;
    }
    st = profiler_current(p);
    if (!st || !st->ctx) {
        profiler_unlock(p);
        return 0;
    }
    gil_handoff(p, st);
    ctx = st->ctx;
    now = now_ns(p);
    tt = ctx->spent + (now - ctx->t0);
    it = tt - ctx->subt;
    e = entry_get(st, key, key, 0);
    if (e) {
        if (--e->recursion_level == 0)
            e->tt += tt;
        else
            e->recursivecallcount++;
        e->it += it;
        e->callcount++;
    }
    if (ctx->previous)
        ctx->previous->subt += tt;
    st->ctx = ctx->previous;
    Py_DECREF(ctx->key);
    PyMem_Free(ctx);
    profiler_unlock(p);
    return 0;
}

static void stack_flush(ProfilerObject *p, Stack *st) {
    int64_t now = now_ns(p);

    while (st->ctx) {
        Context *ctx = st->ctx;
        Entry *e = entry_get(st, ctx->key, ctx->key, 0);
        int64_t tt = ctx->spent + (now - ctx->t0);
        int64_t it = tt - ctx->subt;

        if (e) {
            if (--e->recursion_level == 0)
                e->tt += tt;
            else
                e->recursivecallcount++;
            e->it += it;
            e->callcount++;
        }
        if (ctx->previous)
            ctx->previous->subt += tt;
        st->ctx = ctx->previous;
        Py_DECREF(ctx->key);
        PyMem_Free(ctx);
        now = now_ns(p);
    }
}

static void folded_capsule_destructor(PyObject *cap) {
    Folded *f = (Folded *)PyCapsule_GetPointer(cap, FOLDED_CAPSULE);

    if (!f)
        return;
    Py_XDECREF(f->family);
    Py_XDECREF(f->entries);
    Py_XDECREF(f->thread_ids);
    PyMem_Free(f);
}

static int entry_merge(Entry *dst, Entry *src) {
    PyObject *key;
    PyObject *val;
    Py_ssize_t pos = 0;

    dst->tt += src->tt;
    dst->it += src->it;
    dst->callcount += src->callcount;
    dst->recursivecallcount += src->recursivecallcount;
    while (PyDict_Next(src->calls, &pos, &key, &val)) {
        PyObject *cur;
        PyObject *nxt;
        long n = 0;

        cur = PyDict_GetItemWithError(dst->calls, key);
        if (PyErr_Occurred())
            return -1;
        if (cur)
            n = PyLong_AsLong(cur);
        n += PyLong_AsLong(val);
        if (PyErr_Occurred())
            return -1;
        nxt = PyLong_FromLong(n);
        if (!nxt)
            return -1;
        if (PyDict_SetItem(dst->calls, key, nxt) < 0) {
            Py_DECREF(nxt);
            return -1;
        }
        Py_DECREF(nxt);
    }
    return 0;
}

static Folded *folded_get(ProfilerObject *p, PyObject *family, int create) {
    PyObject *key = family ? family : Py_None;
    PyObject *cap;
    Folded *f;

    cap = PyDict_GetItemWithError(p->folded, key);
    if (cap)
        return (Folded *)PyCapsule_GetPointer(cap, FOLDED_CAPSULE);
    if (PyErr_Occurred() || !create)
        return NULL;
    f = (Folded *)PyMem_Calloc(1, sizeof(*f));
    if (!f)
        return (Folded *)PyErr_NoMemory();
    f->family = family ? Py_NewRef(family) : NULL;
    f->entries = PyDict_New();
    f->thread_ids = PySet_New(NULL);
    f->nstacks = 0;
    if (!f->entries || !f->thread_ids) {
        Py_XDECREF(f->family);
        Py_XDECREF(f->entries);
        Py_XDECREF(f->thread_ids);
        PyMem_Free(f);
        return NULL;
    }
    cap = PyCapsule_New(f, FOLDED_CAPSULE, folded_capsule_destructor);
    if (!cap) {
        Py_XDECREF(f->family);
        Py_DECREF(f->entries);
        Py_DECREF(f->thread_ids);
        PyMem_Free(f);
        return NULL;
    }
    if (PyDict_SetItem(p->folded, key, cap) < 0) {
        Py_DECREF(cap);
        return NULL;
    }
    Py_DECREF(cap);
    return f;
}

static int stack_merge_into_folded(Folded *f, Stack *st) {
    PyObject *key;
    PyObject *cap;
    PyObject *tid;
    Py_ssize_t pos = 0;

    while (PyDict_Next(st->entries, &pos, &key, &cap)) {
        Entry *src = (Entry *)PyCapsule_GetPointer(cap, ENTRY_CAPSULE);
        Entry *dst;

        if (!src)
            return -1;
        dst = entry_get_in(f->entries, key, src->user, 1);
        if (!dst)
            return -1;
        if (entry_merge(dst, src) < 0)
            return -1;
    }
    f->nstacks += 1;
    tid = PyLong_FromUnsignedLong(st->thread_id);
    if (!tid)
        return -1;
    if (PySet_Add(f->thread_ids, tid) < 0) {
        Py_DECREF(tid);
        return -1;
    }
    Py_DECREF(tid);
    return 0;
}

static void stack_unlink_and_free(ProfilerObject *p, Stack *st) {
    Stack **pp;

    if (p->last_active == st)
        p->last_active = NULL;
    for (pp = &p->stacks; *pp; pp = &(*pp)->next) {
        if (*pp == st) {
            *pp = st->next;
            stack_free(st);
            return;
        }
    }
}

static int profiler_fold_stack(ProfilerObject *p, Stack *st) {
    Folded *f;

    if (!p->fold_on_exit || !p->folded)
        return 0;
    f = folded_get(p, st->family, 1);
    if (!f)
        return -1;
    if (stack_merge_into_folded(f, st) < 0)
        return -1;
    if (profiler_current(p) == st)
        profiler_set_current(p, NULL);
    stack_unlink_and_free(p, st);
    return 0;
}

static int bind_tealet(ProfilerObject *p, PyObject *tealet, Stack *st) {
    PyObject *ptr;

    if (!p->by_tealet)
        return 0;
    ptr = PyLong_FromVoidPtr(st);
    if (!ptr)
        return -1;
    if (PyDict_SetItem(p->by_tealet, tealet, ptr) < 0) {
        Py_DECREF(ptr);
        return -1;
    }
    Py_DECREF(ptr);
    return 0;
}

static Stack *lookup_tealet(ProfilerObject *p, PyObject *tealet) {
    PyObject *ptr;

    if (!p->by_tealet)
        return NULL;
    ptr = PyDict_GetItemWithError(p->by_tealet, tealet);
    if (!ptr)
        return NULL;
    return (Stack *)PyLong_AsVoidPtr(ptr);
}

static int profiler_trace_cb(void *data, const char *event, PyObject *origin, PyObject *target) {
    ProfilerObject *p = (ProfilerObject *)data;
    Stack *current;
    Stack *next;
    PyTealet_State state = PYTEALET_STATE_RUN;

    (void)event;
    profiler_lock(p);
    if (!p->enabled) {
        profiler_unlock(p);
        return 0;
    }
    current = profiler_ensure_current(p);
    if (!current) {
        PyErr_Clear();
        profiler_unlock(p);
        return 0;
    }
    gil_handoff(p, current);
    pause_top(p, current);
    if (origin && bind_tealet(p, origin, current) < 0)
        PyErr_Clear();
    if (origin && p->api->state_get && p->tealet_ctx) {
        if (p->api->state_get(p->tealet_ctx, origin, &state) == 0 && state == PYTEALET_STATE_EXIT) {
            stack_flush(p, current);
            if (p->by_tealet)
                PyDict_DelItem(p->by_tealet, origin);
            PyErr_Clear();
            if (p->fold_on_exit) {
                if (profiler_fold_stack(p, current) < 0)
                    PyErr_Clear();
            }
        }
    }
    next = target ? lookup_tealet(p, target) : NULL;
    if (!next) {
        next = stack_new(p);
        if (!next) {
            PyErr_Clear();
            profiler_unlock(p);
            return 0;
        }
        if (target && bind_tealet(p, target, next) < 0)
            PyErr_Clear();
    }
    if (profiler_set_current(p, next) < 0)
        PyErr_Clear();
    resume_top(p, next);
    if (p->slice_gil)
        p->last_active = next;
    profiler_unlock(p);
    return 0;
}

static PyObject *cb_start(PyObject *self, PyObject *const *args, Py_ssize_t nargs) {
    if (nargs < 1) {
        PyErr_SetString(PyExc_TypeError, "callback expected a code object");
        return NULL;
    }
    if (enter_call((ProfilerObject *)self, args[0], args[0]) < 0 && PyErr_Occurred())
        PyErr_Clear();
    Py_RETURN_NONE;
}

static PyObject *cb_return(PyObject *self, PyObject *const *args, Py_ssize_t nargs) {
    if (nargs < 1) {
        PyErr_SetString(PyExc_TypeError, "callback expected a code object");
        return NULL;
    }
    if (leave_call((ProfilerObject *)self, args[0]) < 0 && PyErr_Occurred())
        PyErr_Clear();
    Py_RETURN_NONE;
}

static PyObject *cfunc_user(PyObject *fn) {
    PyObject *self_obj = PyCFunction_GET_SELF(fn);
    PyObject *name;
    PyObject *mod;
    PyObject *user;

    name = PyObject_GetAttrString(fn, "__name__");
    if (!name) {
        PyErr_Clear();
        name = PyUnicode_FromString("?");
        if (!name)
            return NULL;
    }
    mod = PyObject_GetAttrString(fn, "__module__");
    if (!mod)
        PyErr_Clear();
    if (self_obj == NULL) {
        if (mod && PyUnicode_Check(mod) && PyUnicode_CompareWithASCIIString(mod, "builtins") != 0)
            user = PyUnicode_FromFormat("<%U.%U>", mod, name);
        else
            user = PyUnicode_FromFormat("<%U>", name);
    } else if (mod && PyUnicode_Check(mod))
        user = PyUnicode_FromFormat("<built-in method %U.%U>", mod, name);
    else
        user = PyUnicode_FromFormat("<built-in method %U>", name);
    Py_DECREF(name);
    Py_XDECREF(mod);
    return user;
}

static PyObject *c_callable_user(PyObject *callable, PyObject *self_arg, PyObject *missing) {
    if (PyCFunction_Check(callable))
        return cfunc_user(callable);
    if (Py_TYPE(callable) == &PyMethodDescr_Type) {
        PyObject *meth;
        PyObject *user;

        if (self_arg == missing || missing == NULL)
            return NULL;
        meth = Py_TYPE(callable)->tp_descr_get(callable, self_arg, (PyObject *)Py_TYPE(self_arg));
        if (!meth) {
            PyErr_Clear();
            return NULL;
        }
        if (PyCFunction_Check(meth)) {
            user = cfunc_user(meth);
            Py_DECREF(meth);
            return user;
        }
        Py_DECREF(meth);
    }
    return NULL;
}

static PyObject *cb_ccall(PyObject *self, PyObject *const *args, Py_ssize_t nargs) {
    ProfilerObject *p = (ProfilerObject *)self;
    PyObject *user;

    if (!p->builtins || nargs < 4)
        Py_RETURN_NONE;
    user = c_callable_user(args[2], args[3], p->missing);
    if (!user)
        Py_RETURN_NONE;
    if (enter_call(p, user, user) < 0 && PyErr_Occurred())
        PyErr_Clear();
    Py_DECREF(user);
    Py_RETURN_NONE;
}

static PyObject *cb_creturn(PyObject *self, PyObject *const *args, Py_ssize_t nargs) {
    ProfilerObject *p = (ProfilerObject *)self;
    PyObject *user;

    if (!p->builtins || nargs < 4)
        Py_RETURN_NONE;
    user = c_callable_user(args[2], args[3], p->missing);
    if (!user)
        Py_RETURN_NONE;
    if (leave_call(p, user) < 0 && PyErr_Occurred())
        PyErr_Clear();
    Py_DECREF(user);
    Py_RETURN_NONE;
}

static int profiler_monitoring_stop(ProfilerObject *self);

static int monitoring_flag(PyObject *events, const char *name, int *out) {
    PyObject *v = PyObject_GetAttrString(events, name);
    long n;

    if (!v)
        return -1;
    n = PyLong_AsLong(v);
    Py_DECREF(v);
    if (n < 0 && PyErr_Occurred())
        return -1;
    *out = (int)n;
    return 0;
}

static int profiler_monitoring_start(ProfilerObject *self) {
    PyObject *events = NULL;
    PyObject *check = NULL;
    PyObject *cb = NULL;
    PyObject *res = NULL;
    int py_start, py_resume, py_throw, py_return, py_yield, py_unwind;
    int call_ev = 0, c_return = 0, c_raise = 0;
    int all = 0;
    int profiler_id;
    int i;

    {
        PyObject *sysmod = PyImport_ImportModule("sys");
        PyObject *mon;

        if (!sysmod)
            return -1;
        mon = PyObject_GetAttrString(sysmod, "monitoring");
        Py_DECREF(sysmod);
        if (!mon)
            return -1;
        Py_XSETREF(self->monitoring, mon);
    }
    events = PyObject_GetAttrString(self->monitoring, "events");
    if (!events)
        goto error;
    if (monitoring_flag(events, "PY_START", &py_start) < 0 || monitoring_flag(events, "PY_RESUME", &py_resume) < 0 ||
        monitoring_flag(events, "PY_THROW", &py_throw) < 0 || monitoring_flag(events, "PY_RETURN", &py_return) < 0 ||
        monitoring_flag(events, "PY_YIELD", &py_yield) < 0 || monitoring_flag(events, "PY_UNWIND", &py_unwind) < 0)
        goto error;
    if (self->builtins) {
        if (monitoring_flag(events, "CALL", &call_ev) < 0 || monitoring_flag(events, "C_RETURN", &c_return) < 0 ||
            monitoring_flag(events, "C_RAISE", &c_raise) < 0)
            goto error;
        if (!self->missing) {
            self->missing = PyObject_GetAttrString(self->monitoring, "MISSING");
            if (!self->missing)
                goto error;
        }
    }
    Py_CLEAR(events);

    if (monitoring_flag(self->monitoring, "PROFILER_ID", &profiler_id) < 0)
        goto error;
    check = PyObject_CallMethod(self->monitoring, "use_tool_id", "is", profiler_id, "tealet.cprofile");
    if (!check) {
        PyErr_Clear();
        PyErr_SetString(PyExc_ValueError, "another profiling tool is already active");
        goto error;
    }
    Py_DECREF(check);
    check = NULL;
    self->tool_id = profiler_id;

    struct {
        int event;
        const char *meth;
    } table[] = {
        {0, "_pystart_callback"},  {0, "_pystart_callback"},
        {0, "_pystart_callback"},  {0, "_pyreturn_callback"},
        {0, "_pyreturn_callback"}, {0, "_pyreturn_callback"},
        {0, "_ccall_callback"},    {0, "_creturn_callback"},
        {0, "_creturn_callback"},  {0, NULL},
    };
    table[0].event = py_start;
    table[1].event = py_resume;
    table[2].event = py_throw;
    table[3].event = py_return;
    table[4].event = py_yield;
    table[5].event = py_unwind;
    table[6].event = call_ev;
    table[7].event = c_return;
    table[8].event = c_raise;

    for (i = 0; table[i].meth; i++) {
        if (!table[i].event)
            continue;
        cb = PyObject_GetAttrString((PyObject *)self, table[i].meth);
        if (!cb)
            goto error;
        res = PyObject_CallMethod(self->monitoring, "register_callback", "iiO", self->tool_id, table[i].event, cb);
        Py_DECREF(cb);
        cb = NULL;
        if (!res)
            goto error;
        Py_DECREF(res);
        res = NULL;
        all |= table[i].event;
    }
    res = PyObject_CallMethod(self->monitoring, "set_events", "ii", self->tool_id, all);
    if (!res)
        goto error;
    Py_DECREF(res);
    return 0;

error:
    Py_XDECREF(events);
    Py_XDECREF(check);
    Py_XDECREF(cb);
    Py_XDECREF(res);
    if (self->tool_id >= 0)
        profiler_monitoring_stop(self);
    else
        Py_CLEAR(self->monitoring);
    return -1;
}

static int profiler_monitoring_stop(ProfilerObject *self) {
    PyObject *res;
    int i;
    int events[9] = {0};
    PyObject *ev;

    if (!self->monitoring || self->tool_id < 0)
        return 0;
    ev = PyObject_GetAttrString(self->monitoring, "events");
    if (ev) {
        monitoring_flag(ev, "PY_START", &events[0]);
        monitoring_flag(ev, "PY_RESUME", &events[1]);
        monitoring_flag(ev, "PY_THROW", &events[2]);
        monitoring_flag(ev, "PY_RETURN", &events[3]);
        monitoring_flag(ev, "PY_YIELD", &events[4]);
        monitoring_flag(ev, "PY_UNWIND", &events[5]);
        monitoring_flag(ev, "CALL", &events[6]);
        monitoring_flag(ev, "C_RETURN", &events[7]);
        monitoring_flag(ev, "C_RAISE", &events[8]);
        Py_DECREF(ev);
        PyErr_Clear();
    }
    for (i = 0; i < 9; i++) {
        if (!events[i])
            continue;
        res = PyObject_CallMethod(self->monitoring, "register_callback", "iiO", self->tool_id, events[i], Py_None);
        Py_XDECREF(res);
        PyErr_Clear();
    }
    res = PyObject_CallMethod(self->monitoring, "set_events", "ii", self->tool_id, 0);
    Py_XDECREF(res);
    PyErr_Clear();
    res = PyObject_CallMethod(self->monitoring, "free_tool_id", "i", self->tool_id);
    Py_XDECREF(res);
    PyErr_Clear();
    self->tool_id = -1;
    Py_CLEAR(self->monitoring);
    return 0;
}

static void profiler_drop_prev_trace_data(ProfilerObject *self) {
    if (self->prev_trace_data_owned && self->prev_trace_data) {
        Py_DECREF((PyObject *)self->prev_trace_data);
        self->prev_trace_data_owned = 0;
    }
    self->prev_trace = NULL;
    self->prev_trace_data = NULL;
}

static int profiler_tealet_start(ProfilerObject *self) {
    PyObject *mod;
    PyObject *py_cb;

    if (!self->api || !self->tealet_ctx || !self->api->set_trace)
        return 0;
    profiler_drop_prev_trace_data(self);
    self->api->get_trace(self->tealet_ctx, &self->prev_trace, &self->prev_trace_data);
    if (self->prev_trace == profiler_trace_cb) {
        self->prev_trace = NULL;
        self->prev_trace_data = NULL;
    }
    /* Python trampoline: data is the callback. keep it alive across set_trace. */
    if (self->prev_trace && self->prev_trace_data) {
        mod = PyImport_ImportModule("_tealet");
        if (mod) {
            py_cb = PyObject_CallMethod(mod, "gettrace", NULL);
            Py_DECREF(mod);
            if (py_cb && py_cb != Py_None) {
                Py_INCREF((PyObject *)self->prev_trace_data);
                self->prev_trace_data_owned = 1;
            }
            Py_XDECREF(py_cb);
            PyErr_Clear();
        }
    }
    return self->api->set_trace(self->tealet_ctx, profiler_trace_cb, self);
}

static int profiler_tealet_stop(ProfilerObject *self) {
    PyTealetApi_TraceFunc cur = NULL;
    void *cur_data = NULL;
    int rc = 0;

    if (!self->api || !self->tealet_ctx || !self->api->set_trace) {
        profiler_drop_prev_trace_data(self);
        return 0;
    }
    self->api->get_trace(self->tealet_ctx, &cur, &cur_data);
    if (cur == profiler_trace_cb)
        rc = self->api->set_trace(self->tealet_ctx, self->prev_trace, self->prev_trace_data);
    profiler_drop_prev_trace_data(self);
    return rc;
}

static PyObject *profiler_enable(ProfilerObject *self, PyObject *args, PyObject *kwds) {
    int subcalls = 1;
    int builtins = -1;
    static char *kwlist[] = {"subcalls", "builtins", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|pp:enable", kwlist, &subcalls, &builtins))
        return NULL;
    /* subcalls is accepted for cProfile compatibility; caller edges are always recorded. */
    (void)subcalls;
    if (self->enabled)
        Py_RETURN_NONE;
    if (builtins >= 0)
        self->builtins = builtins;
    self->slice_gil = (self->timer_kind == TIMER_WALL && gil_is_enabled());
    self->last_active = NULL;
    profiler_lock(self);
    if (!profiler_ensure_current(self)) {
        profiler_unlock(self);
        return NULL;
    }
    profiler_unlock(self);
    if (profiler_monitoring_start(self) < 0)
        return NULL;
    if (profiler_tealet_start(self) < 0) {
        profiler_monitoring_stop(self);
        return NULL;
    }
    self->enabled = 1;
    Py_RETURN_NONE;
}

static PyObject *profiler_disable(ProfilerObject *self, PyObject *Py_UNUSED(ignored)) {
    Stack *st;

    if (!self->enabled)
        Py_RETURN_NONE;
    profiler_lock(self);
    self->enabled = 0;
    for (st = self->stacks; st; st = st->next)
        stack_flush(self, st);
    self->last_active = NULL;
    profiler_unlock(self);
    profiler_tealet_stop(self);
    profiler_monitoring_stop(self);
    Py_RETURN_NONE;
}

static int dict_set_owned(PyObject *d, const char *key, PyObject *value) {
    int rc;

    if (!value)
        return -1;
    rc = PyDict_SetItemString(d, key, value);
    Py_DECREF(value);
    return rc;
}

static PyObject *entry_as_dict(Entry *e) {
    PyObject *d = PyDict_New();
    double factor = 1.0 / 1000000000.0;

    if (!d)
        return NULL;
    if (PyDict_SetItemString(d, "code", e->user) < 0)
        goto error;
    if (dict_set_owned(d, "callcount", PyLong_FromLong(e->callcount)) < 0)
        goto error;
    if (dict_set_owned(d, "reccallcount", PyLong_FromLong(e->recursivecallcount)) < 0)
        goto error;
    if (dict_set_owned(d, "totaltime", PyFloat_FromDouble(factor * (double)e->tt)) < 0)
        goto error;
    if (dict_set_owned(d, "inlinetime", PyFloat_FromDouble(factor * (double)e->it)) < 0)
        goto error;
    if (dict_set_owned(d, "calls", PyDict_Copy(e->calls)) < 0)
        goto error;
    return d;
error:
    Py_DECREF(d);
    return NULL;
}

static PyObject *stack_as_dict(Stack *st) {
    PyObject *d = PyDict_New();
    PyObject *entries = PyList_New(0);
    PyObject *key;
    PyObject *cap;
    Py_ssize_t pos = 0;

    if (!d || !entries)
        goto error;
    if (st->family) {
        if (PyDict_SetItemString(d, "family", st->family) < 0)
            goto error;
    } else {
        if (PyDict_SetItemString(d, "family", Py_None) < 0)
            goto error;
    }
    if (dict_set_owned(d, "thread_id", PyLong_FromUnsignedLong(st->thread_id)) < 0)
        goto error;
    while (PyDict_Next(st->entries, &pos, &key, &cap)) {
        Entry *e = (Entry *)PyCapsule_GetPointer(cap, ENTRY_CAPSULE);
        PyObject *item;

        if (!e)
            continue;
        item = entry_as_dict(e);
        if (!item)
            goto error;
        if (PyList_Append(entries, item) < 0) {
            Py_DECREF(item);
            goto error;
        }
        Py_DECREF(item);
    }
    if (PyDict_SetItemString(d, "entries", entries) < 0)
        goto error;
    Py_DECREF(entries);
    return d;
error:
    Py_XDECREF(d);
    Py_XDECREF(entries);
    return NULL;
}

static PyObject *profiler_dump_stacks(ProfilerObject *self, PyObject *Py_UNUSED(ignored)) {
    PyObject *list = PyList_New(0);
    Stack *st;

    if (!list)
        return NULL;
    profiler_lock(self);
    for (st = self->stacks; st; st = st->next) {
        PyObject *d = stack_as_dict(st);

        if (!d) {
            profiler_unlock(self);
            Py_DECREF(list);
            return NULL;
        }
        if (PyList_Append(list, d) < 0) {
            Py_DECREF(d);
            profiler_unlock(self);
            Py_DECREF(list);
            return NULL;
        }
        Py_DECREF(d);
    }
    profiler_unlock(self);
    return list;
}

static PyObject *folded_as_dict(Folded *f) {
    PyObject *d = PyDict_New();
    PyObject *entries = PyList_New(0);
    PyObject *key;
    PyObject *cap;
    Py_ssize_t pos = 0;

    if (!d || !entries)
        goto error;
    if (f->family) {
        if (PyDict_SetItemString(d, "family", f->family) < 0)
            goto error;
    } else {
        if (PyDict_SetItemString(d, "family", Py_None) < 0)
            goto error;
    }
    if (dict_set_owned(d, "nstacks", PyLong_FromLong(f->nstacks)) < 0)
        goto error;
    if (dict_set_owned(d, "thread_ids", PyObject_CallFunctionObjArgs((PyObject *)&PyList_Type, f->thread_ids, NULL)) <
        0)
        goto error;
    while (PyDict_Next(f->entries, &pos, &key, &cap)) {
        Entry *e = (Entry *)PyCapsule_GetPointer(cap, ENTRY_CAPSULE);
        PyObject *item;

        if (!e)
            continue;
        item = entry_as_dict(e);
        if (!item)
            goto error;
        if (PyList_Append(entries, item) < 0) {
            Py_DECREF(item);
            goto error;
        }
        Py_DECREF(item);
    }
    if (PyDict_SetItemString(d, "entries", entries) < 0)
        goto error;
    Py_DECREF(entries);
    return d;
error:
    Py_XDECREF(d);
    Py_XDECREF(entries);
    return NULL;
}

static PyObject *profiler_dump_folded(ProfilerObject *self, PyObject *Py_UNUSED(ignored)) {
    PyObject *list = PyList_New(0);
    PyObject *key;
    PyObject *cap;
    Py_ssize_t pos = 0;

    if (!list)
        return NULL;
    profiler_lock(self);
    if (self->folded) {
        while (PyDict_Next(self->folded, &pos, &key, &cap)) {
            Folded *f = (Folded *)PyCapsule_GetPointer(cap, FOLDED_CAPSULE);
            PyObject *d;

            if (!f)
                continue;
            d = folded_as_dict(f);
            if (!d) {
                profiler_unlock(self);
                Py_DECREF(list);
                return NULL;
            }
            if (PyList_Append(list, d) < 0) {
                Py_DECREF(d);
                profiler_unlock(self);
                Py_DECREF(list);
                return NULL;
            }
            Py_DECREF(d);
        }
    }
    profiler_unlock(self);
    return list;
}

static int profiler_init(ProfilerObject *self, PyObject *args, PyObject *kw) {
    int fold_on_exit = 1;
    const char *timer = "wall";
    static char *kwlist[] = {"fold_on_exit", "timer", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kw, "|ps:Profiler", kwlist, &fold_on_exit, &timer))
        return -1;
    if (strcmp(timer, "wall") == 0)
        self->timer_kind = TIMER_WALL;
    else if (strcmp(timer, "thread") == 0)
        self->timer_kind = TIMER_THREAD;
    else {
        PyErr_SetString(PyExc_ValueError, "timer must be 'wall' or 'thread'");
        return -1;
    }
    memset(&self->tls_current, 0, sizeof(self->tls_current));
    self->api = PyTealetApi_Import();
    if (!self->api)
        return -1;
    if (!self->api->set_trace || !self->api->get_trace || !self->api->ctx_new) {
        PyErr_SetString(PyExc_ImportError, "pytealet C API missing set_trace");
        return -1;
    }
    self->tealet_ctx = self->api->ctx_new();
    if (!self->tealet_ctx)
        return -1;
    if (PyThread_tss_create(&self->tls_current) != 0) {
        PyErr_SetString(PyExc_RuntimeError, "failed to create profiler TLS");
        return -1;
    }
    self->tls_ready = 1;
    self->lock = PyThread_allocate_lock();
    if (!self->lock) {
        PyErr_SetString(PyExc_RuntimeError, "failed to allocate profiler lock");
        return -1;
    }
    atomic_store(&self->lock_owner, 0);
    self->lock_depth = 0;
    self->by_tealet = PyDict_New();
    if (!self->by_tealet)
        return -1;
    self->folded = PyDict_New();
    if (!self->folded)
        return -1;
    self->stacks = NULL;
    self->enabled = 0;
    self->builtins = 1;
    self->fold_on_exit = fold_on_exit;
    self->slice_gil = 0;
    self->last_active = NULL;
    self->tool_id = -1;
    self->monitoring = NULL;
    self->missing = NULL;
    self->prev_trace = NULL;
    self->prev_trace_data = NULL;
    self->prev_trace_data_owned = 0;
    return 0;
}

static void profiler_dealloc(ProfilerObject *self) {
    Stack *st;

    if (self->enabled) {
        profiler_lock(self);
        self->enabled = 0;
        profiler_unlock(self);
        profiler_tealet_stop(self);
        profiler_monitoring_stop(self);
    } else {
        profiler_drop_prev_trace_data(self);
    }
    st = self->stacks;
    while (st) {
        Stack *next = st->next;
        stack_free(st);
        st = next;
    }
    self->stacks = NULL;
    Py_CLEAR(self->by_tealet);
    Py_CLEAR(self->folded);
    Py_CLEAR(self->monitoring);
    Py_CLEAR(self->missing);
    if (self->tealet_ctx && self->api && self->api->ctx_free) {
        self->api->ctx_free(self->tealet_ctx);
        self->tealet_ctx = NULL;
    }
    if (self->tls_ready) {
        PyThread_tss_delete(&self->tls_current);
        self->tls_ready = 0;
    }
    if (self->lock) {
        PyThread_free_lock(self->lock);
        self->lock = NULL;
    }
    Py_TYPE(self)->tp_free(self);
}

static PyObject *profiler_get_fold_on_exit(ProfilerObject *self, void *Py_UNUSED(closure)) {
    return PyBool_FromLong(self->fold_on_exit);
}

static int profiler_set_fold_on_exit(ProfilerObject *self, PyObject *value, void *Py_UNUSED(closure)) {
    int v;

    if (value == NULL) {
        PyErr_SetString(PyExc_AttributeError, "cannot delete fold_on_exit");
        return -1;
    }
    v = PyObject_IsTrue(value);
    if (v < 0)
        return -1;
    profiler_lock(self);
    self->fold_on_exit = v;
    profiler_unlock(self);
    return 0;
}

static PyObject *profiler_get_timer(ProfilerObject *self, void *Py_UNUSED(closure)) {
    return PyUnicode_FromString(self->timer_kind == TIMER_THREAD ? "thread" : "wall");
}

static int profiler_set_timer(ProfilerObject *self, PyObject *value, void *Py_UNUSED(closure)) {
    const char *timer;

    if (value == NULL) {
        PyErr_SetString(PyExc_AttributeError, "cannot delete timer");
        return -1;
    }
    timer = PyUnicode_AsUTF8(value);
    if (!timer)
        return -1;
    if (strcmp(timer, "wall") != 0 && strcmp(timer, "thread") != 0) {
        PyErr_SetString(PyExc_ValueError, "timer must be 'wall' or 'thread'");
        return -1;
    }
    profiler_lock(self);
    self->timer_kind = (strcmp(timer, "thread") == 0) ? TIMER_THREAD : TIMER_WALL;
    if (self->enabled)
        self->slice_gil = (self->timer_kind == TIMER_WALL && gil_is_enabled());
    profiler_unlock(self);
    return 0;
}

static PyGetSetDef profiler_getset[] = {
    {"fold_on_exit", (getter)profiler_get_fold_on_exit, (setter)profiler_set_fold_on_exit,
     "Merge a finished tealet into its family and drop the individual stack.", NULL},
    {"timer", (getter)profiler_get_timer, (setter)profiler_set_timer, "Clock: 'wall' or 'thread'.", NULL},
    {NULL, NULL, NULL, NULL, NULL},
};

static PyMethodDef profiler_methods[] = {
    {"enable", (PyCFunction)(void (*)(void))profiler_enable, METH_VARARGS | METH_KEYWORDS,
     "enable(subcalls=True, builtins=True)\nStart collecting profile data."},
    {"disable", (PyCFunction)profiler_disable, METH_NOARGS, "Stop collecting profile data."},
    {"dump_stacks", (PyCFunction)profiler_dump_stacks, METH_NOARGS, "Return raw per-stack timing dicts."},
    {"dump_folded", (PyCFunction)profiler_dump_folded, METH_NOARGS, "Return family accumulators for folded stacks."},
    {"_pystart_callback", (PyCFunction)(void (*)(void))cb_start, METH_FASTCALL, NULL},
    {"_pyreturn_callback", (PyCFunction)(void (*)(void))cb_return, METH_FASTCALL, NULL},
    {"_ccall_callback", (PyCFunction)(void (*)(void))cb_ccall, METH_FASTCALL, NULL},
    {"_creturn_callback", (PyCFunction)(void (*)(void))cb_creturn, METH_FASTCALL, NULL},
    {NULL, NULL, 0, NULL},
};

static PyTypeObject ProfilerType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_tealet_profile.Profiler",
    .tp_basicsize = sizeof(ProfilerObject),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
    .tp_init = (initproc)profiler_init,
    .tp_dealloc = (destructor)profiler_dealloc,
    .tp_methods = profiler_methods,
    .tp_getset = profiler_getset,
    .tp_doc = "C profiler with per-stack timings and tealet switch support (Python 3.12+).",
};

static int module_exec(PyObject *module) {
    if (PyType_Ready(&ProfilerType) < 0)
        return -1;
    if (PyModule_AddObjectRef(module, "Profiler", (PyObject *)&ProfilerType) < 0)
        return -1;
    return 0;
}

/* CPython API uses void* in module slots; this conversion is intentional. */
#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
static PyModuleDef_Slot module_slots[] = {
    {Py_mod_exec, module_exec},
#ifdef Py_mod_gil
    {Py_mod_gil, Py_MOD_GIL_NOT_USED},
#endif
    {0, NULL},
};
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

static struct PyModuleDef module_def = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_tealet_profile",
    .m_doc = "C profiler for tealet (sys.monitoring, Python 3.12+).",
    .m_size = 0,
    .m_slots = module_slots,
};

PyMODINIT_FUNC PyInit__tealet_profile(void) { return PyModuleDef_Init(&module_def); }
