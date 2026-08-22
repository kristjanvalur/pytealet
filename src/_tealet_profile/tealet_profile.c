/* tealet_profile.c - 3.12+ C profiler with per-stack timings and tealet switches.
 *
 * Uses sys.monitoring for call/return and the pytealet capsule set_trace hook
 * to swap stacks. Requires Python 3.12 or newer.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "pytealet_capi.h"

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

typedef struct {
    PyObject_HEAD const PyTealet_CAPI *api;
    PyTealet_CAPI_Context *tealet_ctx;
    Py_tss_t tls_current;
    int tls_ready;
    Stack *stacks;
    PyObject *by_tealet;
    PyThread_type_lock lock;
    int enabled;
    int tool_id;
    PyObject *monitoring;
    PyTealetApi_TraceFunc prev_trace;
    void *prev_trace_data;
} ProfilerObject;

static PyTypeObject ProfilerType;

static int64_t now_ns(void) {
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

static void entry_capsule_destructor(PyObject *cap) {
    Entry *e = (Entry *)PyCapsule_GetPointer(cap, ENTRY_CAPSULE);

    if (!e)
        return;
    Py_XDECREF(e->user);
    Py_XDECREF(e->calls);
    PyMem_Free(e);
}

static Entry *entry_get(Stack *st, PyObject *key, PyObject *user, int create) {
    PyObject *cap;
    Entry *e;

    cap = PyDict_GetItemWithError(st->entries, key);
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
    if (PyDict_SetItem(st->entries, key, cap) < 0) {
        Py_DECREF(cap);
        return NULL;
    }
    Py_DECREF(cap);
    return e;
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

static Stack *stack_new(ProfilerObject *p) {
    Stack *st = (Stack *)PyMem_Calloc(1, sizeof(*st));

    if (!st)
        return NULL;
    st->entries = PyDict_New();
    if (!st->entries) {
        PyMem_Free(st);
        return NULL;
    }
    st->thread_id = PyThread_get_thread_ident();
    st->family = NULL;
    st->ctx = NULL;
    PyThread_acquire_lock(p->lock, WAIT_LOCK);
    st->next = p->stacks;
    p->stacks = st;
    PyThread_release_lock(p->lock);
    return st;
}

static Stack *profiler_current(ProfilerObject *p) {
    if (!p->tls_ready)
        return NULL;
    return (Stack *)PyThread_tss_get(&p->tls_current);
}

static int profiler_set_current(ProfilerObject *p, Stack *st) {
    if (!p->tls_ready)
        return -1;
    return PyThread_tss_set(&p->tls_current, st);
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

static void pause_top(Stack *st) {
    int64_t now;

    if (!st || !st->ctx)
        return;
    now = now_ns();
    st->ctx->spent += now - st->ctx->t0;
    st->ctx->t0 = now;
}

static void resume_top(Stack *st) {
    if (!st || !st->ctx)
        return;
    st->ctx->t0 = now_ns();
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

    if (!p->enabled)
        return 0;
    st = profiler_ensure_current(p);
    if (!st)
        return -1;
    maybe_set_family(st, user);
    e = entry_get(st, key, user, 1);
    if (!e)
        return -1;
    prev = st->ctx;
    if (prev) {
        Entry *caller = entry_get(st, prev->key, prev->key, 0);
        if (caller && bump_callee(caller, key) < 0)
            return -1;
        prev->subt += now_ns() - prev->t0;
        prev->t0 = now_ns();
    }
    ctx = (Context *)PyMem_Malloc(sizeof(*ctx));
    if (!ctx)
        return -1;
    ctx->t0 = now_ns();
    ctx->spent = 0;
    ctx->subt = 0;
    ctx->key = Py_NewRef(key);
    ctx->previous = prev;
    st->ctx = ctx;
    e->recursion_level++;
    return 0;
}

static int leave_call(ProfilerObject *p, PyObject *key) {
    Stack *st;
    Entry *e;
    Context *ctx;
    int64_t now;
    int64_t tt;
    int64_t it;

    if (!p->enabled)
        return 0;
    st = profiler_current(p);
    if (!st || !st->ctx)
        return 0;
    ctx = st->ctx;
    now = now_ns();
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
    if (ctx->previous) {
        ctx->previous->subt += tt;
        ctx->previous->t0 = now;
    }
    st->ctx = ctx->previous;
    Py_DECREF(ctx->key);
    PyMem_Free(ctx);
    return 0;
}

static void stack_flush(Stack *st) {
    int64_t now = now_ns();

    while (st->ctx) {
        Context *ctx = st->ctx;
        Entry *e = entry_get(st, ctx->key, ctx->key, 0);
        int64_t tt = ctx->spent + (now - ctx->t0);
        int64_t it = tt - ctx->subt;

        if (e) {
            if (e->recursion_level > 0)
                e->recursion_level--;
            if (e->recursion_level == 0)
                e->tt += tt;
            e->it += it;
            e->callcount++;
        }
        st->ctx = ctx->previous;
        Py_DECREF(ctx->key);
        PyMem_Free(ctx);
        now = now_ns();
    }
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
    if (!p->enabled)
        return 0;
    current = profiler_ensure_current(p);
    if (!current)
        return -1;
    pause_top(current);
    if (origin && bind_tealet(p, origin, current) < 0)
        return -1;
    if (origin && p->api->state_get && p->tealet_ctx) {
        if (p->api->state_get(p->tealet_ctx, origin, &state) == 0 && state == PYTEALET_STATE_EXIT) {
            stack_flush(current);
            if (p->by_tealet)
                PyDict_DelItem(p->by_tealet, origin);
            PyErr_Clear();
        }
    }
    next = target ? lookup_tealet(p, target) : NULL;
    if (!next) {
        next = stack_new(p);
        if (!next)
            return -1;
        if (target && bind_tealet(p, target, next) < 0)
            return -1;
    }
    if (profiler_set_current(p, next) < 0)
        return -1;
    resume_top(next);
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
    int all = 0;
    int ids[] = {3, 4, 2, -1};
    int i;

    {
        PyObject *sysmod = PyImport_ImportModule("sys");

        if (!sysmod)
            return -1;
        self->monitoring = PyObject_GetAttrString(sysmod, "monitoring");
        Py_DECREF(sysmod);
        if (!self->monitoring)
            return -1;
    }
    events = PyObject_GetAttrString(self->monitoring, "events");
    if (!events)
        goto error;
    if (monitoring_flag(events, "PY_START", &py_start) < 0 || monitoring_flag(events, "PY_RESUME", &py_resume) < 0 ||
        monitoring_flag(events, "PY_THROW", &py_throw) < 0 || monitoring_flag(events, "PY_RETURN", &py_return) < 0 ||
        monitoring_flag(events, "PY_YIELD", &py_yield) < 0 || monitoring_flag(events, "PY_UNWIND", &py_unwind) < 0)
        goto error;
    Py_CLEAR(events);

    self->tool_id = -1;
    for (i = 0; ids[i] >= 0; i++) {
        PyErr_Clear();
        check = PyObject_CallMethod(self->monitoring, "use_tool_id", "is", ids[i], "tealet.cprofile");
        if (check) {
            self->tool_id = ids[i];
            Py_DECREF(check);
            check = NULL;
            break;
        }
    }
    if (self->tool_id < 0) {
        PyErr_SetString(PyExc_RuntimeError, "no free sys.monitoring tool id for tealet.cprofile");
        goto error;
    }

    struct {
        int event;
        const char *meth;
    } table[] = {
        {0, "_pystart_callback"},
        {0, "_pystart_callback"},
        {0, "_pystart_callback"},
        {0, "_pyreturn_callback"},
        {0, "_pyreturn_callback"},
        {0, "_pyreturn_callback"},
        {0, NULL},
    };
    table[0].event = py_start;
    table[1].event = py_resume;
    table[2].event = py_throw;
    table[3].event = py_return;
    table[4].event = py_yield;
    table[5].event = py_unwind;

    for (i = 0; table[i].meth; i++) {
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
    return -1;
}

static int profiler_monitoring_stop(ProfilerObject *self) {
    PyObject *res;
    int i;
    int events[6] = {0};
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
        Py_DECREF(ev);
        PyErr_Clear();
    }
    for (i = 0; i < 6; i++) {
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

static int profiler_tealet_start(ProfilerObject *self) {
    if (!self->api || !self->tealet_ctx || !self->api->set_trace)
        return 0;
    self->api->get_trace(self->tealet_ctx, &self->prev_trace, &self->prev_trace_data);
    if (self->prev_trace == profiler_trace_cb) {
        self->prev_trace = NULL;
        self->prev_trace_data = NULL;
    }
    return self->api->set_trace(self->tealet_ctx, profiler_trace_cb, self);
}

static int profiler_tealet_stop(ProfilerObject *self) {
    PyTealetApi_TraceFunc cur = NULL;
    void *cur_data = NULL;

    if (!self->api || !self->tealet_ctx || !self->api->set_trace)
        return 0;
    self->api->get_trace(self->tealet_ctx, &cur, &cur_data);
    if (cur == profiler_trace_cb)
        return self->api->set_trace(self->tealet_ctx, self->prev_trace, self->prev_trace_data);
    return 0;
}

static PyObject *profiler_enable(ProfilerObject *self, PyObject *Py_UNUSED(ignored)) {
    if (self->enabled)
        Py_RETURN_NONE;
    if (!profiler_ensure_current(self))
        return NULL;
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
    self->enabled = 0;
    profiler_tealet_stop(self);
    profiler_monitoring_stop(self);
    PyThread_acquire_lock(self->lock, WAIT_LOCK);
    for (st = self->stacks; st; st = st->next)
        stack_flush(st);
    PyThread_release_lock(self->lock);
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
    if (PyDict_SetItemString(d, "calls", e->calls) < 0)
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
    PyThread_acquire_lock(self->lock, WAIT_LOCK);
    for (st = self->stacks; st; st = st->next) {
        PyObject *d = stack_as_dict(st);

        if (!d) {
            PyThread_release_lock(self->lock);
            Py_DECREF(list);
            return NULL;
        }
        if (PyList_Append(list, d) < 0) {
            Py_DECREF(d);
            PyThread_release_lock(self->lock);
            Py_DECREF(list);
            return NULL;
        }
        Py_DECREF(d);
    }
    PyThread_release_lock(self->lock);
    return list;
}

static int profiler_init(ProfilerObject *self, PyObject *args, PyObject *kw) {
    static char *kwlist[] = {NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kw, ":Profiler", kwlist))
        return -1;
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
    self->by_tealet = PyDict_New();
    if (!self->by_tealet)
        return -1;
    self->stacks = NULL;
    self->enabled = 0;
    self->tool_id = -1;
    self->monitoring = NULL;
    self->prev_trace = NULL;
    self->prev_trace_data = NULL;
    return 0;
}

static void profiler_dealloc(ProfilerObject *self) {
    Stack *st;

    if (self->enabled) {
        self->enabled = 0;
        profiler_tealet_stop(self);
        profiler_monitoring_stop(self);
    }
    st = self->stacks;
    while (st) {
        Stack *next = st->next;
        stack_free(st);
        st = next;
    }
    self->stacks = NULL;
    Py_CLEAR(self->by_tealet);
    Py_CLEAR(self->monitoring);
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

static PyMethodDef profiler_methods[] = {
    {"enable", (PyCFunction)profiler_enable, METH_NOARGS, "Start collecting profile data."},
    {"disable", (PyCFunction)profiler_disable, METH_NOARGS, "Stop collecting profile data."},
    {"dump_stacks", (PyCFunction)profiler_dump_stacks, METH_NOARGS, "Return raw per-stack timing dicts."},
    {"_pystart_callback", (PyCFunction)(void (*)(void))cb_start, METH_FASTCALL, NULL},
    {"_pyreturn_callback", (PyCFunction)(void (*)(void))cb_return, METH_FASTCALL, NULL},
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
static PyModuleDef_Slot module_slots[] = {{Py_mod_exec, module_exec}, {0, NULL}};
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
