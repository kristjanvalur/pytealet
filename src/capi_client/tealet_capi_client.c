/* pytealet_capi_client.c - validation client for the _tealet capsule C API.
 *
 * This extension acts as a downstream consumer of _tealet._C_API and is used
 * by tests to validate import/version checks and basic call paths.
 */

#include <Python.h>

#include "pytealet_capi.h"

#include <string.h>

typedef struct {
    const PyTealet_CAPI *api;
    PyTealet_CAPI_Context *ctx;
} PyTealetCapiClientState;

static PyTealetCapiClientState *client_get_state(PyObject *module) {
    PyTealetCapiClientState *state;

    if (!module || !PyModule_Check(module)) {
        PyErr_SetString(PyExc_RuntimeError, "invalid module object");
        return NULL;
    }

    state = (PyTealetCapiClientState *)PyModule_GetState(module);
    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "client module state unavailable");
        return NULL;
    }
    if (!state->api) {
        PyErr_SetString(PyExc_RuntimeError, "pytealet C API unavailable");
        return NULL;
    }

    return state;
}

static int client_ensure_ctx(PyTealetCapiClientState *state) {
    if (state->ctx)
        return 0;

    if (!state->api->ctx_new) {
        PyErr_SetString(PyExc_RuntimeError, "ctx_new missing from pytealet C API");
        return -1;
    }

    state->ctx = state->api->ctx_new();
    if (!state->ctx)
        return -1;

    return 0;
}

static int client_dict_set_owned(PyObject *d, const char *key, PyObject *value) {
    int rc;

    if (!value)
        return -1;

    rc = PyDict_SetItemString(d, key, value);
    Py_DECREF(value);
    return rc;
}

static PyObject *client_api_info(PyObject *module, PyObject *Py_UNUSED(_ignored)) {
    PyTealetCapiClientState *state = client_get_state(module);
    PyObject *d;

    if (!state)
        return NULL;

    d = PyDict_New();
    if (!d)
        return NULL;

    if (client_dict_set_owned(d, "abi_version", PyLong_FromUnsignedLong(state->api->abi_version)) < 0)
        goto error;
    if (client_dict_set_owned(d, "struct_size", PyLong_FromUnsignedLong(state->api->struct_size)) < 0)
        goto error;
    if (client_dict_set_owned(d, "feature_flags", PyLong_FromUnsignedLongLong(state->api->feature_flags)) < 0)
        goto error;
    if (client_dict_set_owned(d, "has_base",
                              PyBool_FromLong((state->api->feature_flags & PYTEALET_CAPI_FEATURE_BASE) != 0)) < 0)
        goto error;
    if (client_dict_set_owned(d, "has_create",
                              PyBool_FromLong(state->api->create != NULL)) < 0)
        goto error;
    if (client_dict_set_owned(d, "has_stub",
                              PyBool_FromLong(state->api->stub != NULL)) < 0)
        goto error;
    if (client_dict_set_owned(d, "has_set_stub",
                              PyBool_FromLong(state->api->set_stub != NULL)) < 0)
        goto error;
    if (client_dict_set_owned(d, "has_prepare",
                              PyBool_FromLong(state->api->prepare != NULL)) < 0)
        goto error;
    if (client_dict_set_owned(d, "has_duplicate",
                              PyBool_FromLong(state->api->duplicate != NULL)) < 0)
        goto error;
    if (client_dict_set_owned(d, "has_run",
                              PyBool_FromLong(state->api->run != NULL)) < 0)
        goto error;
    if (client_dict_set_owned(d, "has_switch",
                              PyBool_FromLong(state->api->switch_ != NULL)) < 0)
        goto error;
    if (client_dict_set_owned(d, "has_throw",
                              PyBool_FromLong(state->api->throw_ != NULL)) < 0)
        goto error;
    if (client_dict_set_owned(d, "has_set_exception",
                              PyBool_FromLong(state->api->set_exception != NULL)) < 0)
        goto error;
    if (client_dict_set_owned(d, "has_thread_reap",
                              PyBool_FromLong(state->api->thread_reap != NULL)) < 0)
        goto error;
    if (client_dict_set_owned(d, "has_thread_sweep",
                              PyBool_FromLong(state->api->thread_sweep != NULL)) < 0)
        goto error;
    if (client_dict_set_owned(d, "has_thread_active",
                              PyBool_FromLong(state->api->thread_active != NULL)) < 0)
        goto error;
    if (client_dict_set_owned(d, "has_thread_kill",
                              PyBool_FromLong(state->api->thread_kill != NULL)) < 0)
        goto error;
    if (client_dict_set_owned(d, "has_error_was_remote",
                              PyBool_FromLong(state->api->error_was_remote != NULL)) < 0)
        goto error;
    if (client_dict_set_owned(d, "has_previous",
                              PyBool_FromLong(state->api->previous != NULL)) < 0)
        goto error;
    if (client_dict_set_owned(d, "has_frame_introspection_get",
                              PyBool_FromLong(state->api->frame_introspection_get != NULL)) < 0)
        goto error;
    if (client_dict_set_owned(d, "has_frame_introspection_set",
                              PyBool_FromLong(state->api->frame_introspection_set != NULL)) < 0)
        goto error;
    if (client_dict_set_owned(d, "has_is_foreign",
                              PyBool_FromLong(state->api->is_foreign != NULL)) < 0)
        goto error;
    if (client_dict_set_owned(d, "has_state_get",
                              PyBool_FromLong(state->api->state_get != NULL)) < 0)
        goto error;
    if (client_dict_set_owned(d, "has_thread_id_get",
                              PyBool_FromLong(state->api->thread_id_get != NULL)) < 0)
        goto error;

    return d;

error:
    Py_DECREF(d);
    return NULL;
}

static PyObject *client_current_is_main(PyObject *module, PyObject *Py_UNUSED(_ignored)) {
    PyTealetCapiClientState *state = client_get_state(module);
    PyObject *current;
    PyObject *main;
    PyObject *result;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    current = state->api->current(state->ctx);
    if (!current)
        return NULL;
    main = state->api->main(state->ctx);
    if (!main) {
        Py_DECREF(current);
        return NULL;
    }

    result = PyBool_FromLong(current == main);
    Py_DECREF(main);
    Py_DECREF(current);
    return result;
}

static PyObject *client_check_tealet(PyObject *module, PyObject *obj) {
    PyTealetCapiClientState *state = client_get_state(module);
    int rc;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    rc = state->api->check_tealet(state->ctx, obj);
    if (rc < 0)
        return NULL;

    return PyBool_FromLong(rc != 0);
}

static PyObject *client_capi_switch(PyObject *module, PyObject *args) {
    PyTealetCapiClientState *state = client_get_state(module);
    PyObject *target;
    PyObject *arg = NULL;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    if (!PyArg_ParseTuple(args, "O|O:capi_switch", &target, &arg))
        return NULL;

    return state->api->switch_(state->ctx, target, arg, PYTEALET_SWITCH_FLAGS_DEFAULT);
}

static PyObject *client_capi_run(PyObject *module, PyObject *args) {
    PyTealetCapiClientState *state = client_get_state(module);
    PyObject *target;
    PyObject *func;
    PyObject *arg = NULL;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    if (!PyArg_ParseTuple(args, "OO|O:capi_run", &target, &func, &arg))
        return NULL;

    return state->api->run(state->ctx, target, func, NULL, arg);
}

static int client_parse_u32(PyObject *obj, const char *what, uint32_t *out) {
    unsigned long v;

    v = PyLong_AsUnsignedLong(obj);
    if (v == (unsigned long)-1 && PyErr_Occurred())
        return -1;
    if (v > UINT32_MAX) {
        PyErr_Format(PyExc_OverflowError, "%s does not fit in uint32", what);
        return -1;
    }

    *out = (uint32_t)v;
    return 0;
}

static PyObject *client_capi_switch_flags(PyObject *module, PyObject *args) {
    PyTealetCapiClientState *state = client_get_state(module);
    PyObject *target;
    PyObject *flags_obj;
    PyObject *arg = NULL;
    uint32_t flags;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    if (!PyArg_ParseTuple(args, "OO|O:capi_switch_flags", &target, &flags_obj, &arg))
        return NULL;

    if (client_parse_u32(flags_obj, "flags", &flags) < 0)
        return NULL;

    return state->api->switch_(state->ctx, target, arg, flags);
}

static PyObject *client_capi_throw(PyObject *module, PyObject *args) {
    PyTealetCapiClientState *state = client_get_state(module);
    PyObject *target;
    PyObject *exc;
    PyObject *flags_obj = NULL;
    uint32_t flags = PYTEALET_THROW_FLAGS_DEFAULT;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    if (!PyArg_ParseTuple(args, "OO|O:capi_throw", &target, &exc, &flags_obj))
        return NULL;

    if (flags_obj != NULL) {
        if (client_parse_u32(flags_obj, "flags", &flags) < 0)
            return NULL;
    }

    return state->api->throw_(state->ctx, target, exc, flags);
}

static PyObject *client_capi_set_exception(PyObject *module, PyObject *args) {
    PyTealetCapiClientState *state = client_get_state(module);
    PyObject *target;
    PyObject *exc;
    PyObject *fallback = Py_None;
    int rc;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    if (!PyArg_ParseTuple(args, "OO|O:capi_set_exception", &target, &exc, &fallback))
        return NULL;

    rc = state->api->set_exception(state->ctx, target, exc, fallback);
    if (rc < 0)
        return NULL;
    Py_RETURN_NONE;
}

static PyObject *client_capi_thread_reap(PyObject *module, PyObject *args) {
    PyTealetCapiClientState *state = client_get_state(module);
    Py_ssize_t cleanup_passes = 3;
    PyObject *kill_exc = Py_None;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    if (!PyArg_ParseTuple(args, "|nO:capi_thread_reap", &cleanup_passes, &kill_exc))
        return NULL;

    return state->api->thread_reap(state->ctx, cleanup_passes, kill_exc);
}

static PyObject *client_capi_thread_sweep(PyObject *module, PyObject *Py_UNUSED(_ignored)) {
    PyTealetCapiClientState *state = client_get_state(module);

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    return state->api->thread_sweep(state->ctx);
}

static PyObject *client_capi_thread_active(PyObject *module, PyObject *Py_UNUSED(_ignored)) {
    PyTealetCapiClientState *state = client_get_state(module);

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    return state->api->thread_active(state->ctx);
}

static PyObject *client_capi_thread_kill(PyObject *module, PyObject *args) {
    PyTealetCapiClientState *state = client_get_state(module);
    Py_ssize_t cleanup_passes = 3;
    PyObject *kill_exc = Py_None;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    if (!PyArg_ParseTuple(args, "|nO:capi_thread_kill", &cleanup_passes, &kill_exc))
        return NULL;

    return state->api->thread_kill(state->ctx, cleanup_passes, kill_exc);
}

static PyObject *client_capi_error_was_remote(PyObject *module, PyObject *Py_UNUSED(_ignored)) {
    PyTealetCapiClientState *state = client_get_state(module);
    int rc;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    rc = state->api->error_was_remote(state->ctx);
    if (rc < 0)
        return NULL;

    return PyBool_FromLong(rc != 0);
}

static PyObject *client_capi_previous(PyObject *module, PyObject *Py_UNUSED(_ignored)) {
    PyTealetCapiClientState *state = client_get_state(module);

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    return state->api->previous(state->ctx);
}

static PyObject *client_capi_frame_introspection(PyObject *module, PyObject *Py_UNUSED(_ignored)) {
    PyTealetCapiClientState *state = client_get_state(module);
    int rc;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    rc = state->api->frame_introspection_get(state->ctx);
    if (rc < 0)
        return NULL;
    return PyBool_FromLong(rc != 0);
}

static PyObject *client_capi_frame_introspection_set(PyObject *module, PyObject *enabled_obj) {
    PyTealetCapiClientState *state = client_get_state(module);
    int enabled;
    int rc;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    enabled = PyObject_IsTrue(enabled_obj);
    if (enabled < 0)
        return NULL;

    rc = state->api->frame_introspection_set(state->ctx, enabled);
    if (rc < 0)
        return NULL;
    return PyBool_FromLong(rc != 0);
}

static PyObject *client_capi_is_foreign(PyObject *module, PyObject *target) {
    PyTealetCapiClientState *state = client_get_state(module);
    int rc;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    rc = state->api->is_foreign(state->ctx, target);
    if (rc < 0)
        return NULL;
    return PyBool_FromLong(rc != 0);
}

static PyObject *client_capi_state(PyObject *module, PyObject *target) {
    PyTealetCapiClientState *state = client_get_state(module);
    PyTealet_State value;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    if (state->api->state_get(state->ctx, target, &value) < 0)
        return NULL;
    return PyLong_FromLong((long)value);
}

static PyObject *client_capi_thread_id(PyObject *module, PyObject *target) {
    PyTealetCapiClientState *state = client_get_state(module);
    unsigned long value;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    if (state->api->thread_id_get(state->ctx, target, &value) < 0)
        return NULL;
    return PyLong_FromUnsignedLong(value);
}

static PyObject *client_capi_prepare(PyObject *module, PyObject *args) {
    PyTealetCapiClientState *state = client_get_state(module);
    PyObject *target;
    PyObject *func;
    int rc;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    if (!PyArg_ParseTuple(args, "OO:capi_prepare", &target, &func))
        return NULL;

    rc = state->api->prepare(state->ctx, target, func, NULL);
    if (rc < 0)
        return NULL;
    Py_RETURN_NONE;
}

static PyObject *client_capi_stub(PyObject *module, PyObject *target) {
    PyTealetCapiClientState *state = client_get_state(module);
    int rc;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    rc = state->api->stub(state->ctx, target);
    if (rc < 0)
        return NULL;
    Py_RETURN_NONE;
}

static PyObject *client_capi_set_stub(PyObject *module, PyObject *args) {
    PyTealetCapiClientState *state = client_get_state(module);
    PyObject *target;
    PyObject *source;
    int duplicate = 1;
    int rc;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    if (!PyArg_ParseTuple(args, "OO|p:capi_set_stub", &target, &source, &duplicate))
        return NULL;

    rc = state->api->set_stub(state->ctx, target, source, duplicate);
    if (rc < 0)
        return NULL;
    Py_RETURN_NONE;
}

static PyObject *client_capi_create(PyObject *module, PyObject *Py_UNUSED(_ignored)) {
    PyTealetCapiClientState *state = client_get_state(module);

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    return state->api->create(state->ctx);
}

static PyObject *client_capi_duplicate(PyObject *module, PyObject *source) {
    PyTealetCapiClientState *state = client_get_state(module);

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    return state->api->duplicate(state->ctx, source);
}

static PyObject *client_run_c_callback(PyObject *current, PyObject *arg) {
    PyObject *main_method;
    PyObject *main;
    PyObject *tag;
    PyObject *payload;
    PyObject *result;

    if (!current) {
        PyErr_SetString(PyExc_RuntimeError, "current tealet must not be NULL");
        return NULL;
    }
    if (!arg)
        arg = Py_None;

    main_method = PyObject_GetAttrString(current, "main");
    if (!main_method)
        return NULL;
    main = PyObject_CallNoArgs(main_method);
    Py_DECREF(main_method);
    if (!main)
        return NULL;

    tag = PyUnicode_FromString("via-capi-run-c");
    if (!tag) {
        Py_DECREF(main);
        return NULL;
    }

    payload = PyTuple_Pack(2, tag, arg);
    Py_DECREF(tag);
    if (!payload) {
        Py_DECREF(main);
        return NULL;
    }

    result = PyTuple_Pack(2, main, payload);
    Py_DECREF(payload);
    Py_DECREF(main);
    return result;
}

static PyObject *client_capi_run_c(PyObject *module, PyObject *args) {
    PyTealetCapiClientState *state = client_get_state(module);
    PyObject *target;
    PyObject *arg = NULL;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    if (!PyArg_ParseTuple(args, "O|O:capi_run_c", &target, &arg))
        return NULL;

    return state->api->run(state->ctx, target, NULL, client_run_c_callback, arg);
}

static PyObject *client_capi_prepare_c(PyObject *module, PyObject *target) {
    PyTealetCapiClientState *state = client_get_state(module);
    int rc;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    rc = state->api->prepare(state->ctx, target, NULL, client_run_c_callback);
    if (rc < 0)
        return NULL;
    Py_RETURN_NONE;
}

static PyMethodDef client_methods[] = {
    {"api_info", (PyCFunction)client_api_info, METH_NOARGS, "Return imported pytealet C API metadata."},
    {"current_is_main", (PyCFunction)client_current_is_main, METH_NOARGS,
     "Return whether C API current() and main() refer to the same object."},
    {"check_tealet", (PyCFunction)client_check_tealet, METH_O,
     "Return True if object is a _tealet.tealet instance according to C API."},
    {"capi_create", (PyCFunction)client_capi_create, METH_NOARGS,
     "Create a brand-new tealet using the imported C API."},
    {"capi_stub", (PyCFunction)client_capi_stub, METH_O,
     "Stub a tealet using the imported C API."},
    {"capi_set_stub", (PyCFunction)client_capi_set_stub, METH_VARARGS,
     "Attach a duplicated stub from source into target via imported C API."},
    {"capi_duplicate", (PyCFunction)client_capi_duplicate, METH_O,
     "Duplicate a tealet using the imported C API."},
    {"capi_prepare", (PyCFunction)client_capi_prepare, METH_VARARGS,
     "Prepare a tealet with a Python callable using the imported C API."},
    {"capi_prepare_c", (PyCFunction)client_capi_prepare_c, METH_O,
     "Prepare a tealet with a native C callback using the imported C API."},
    {"capi_run", (PyCFunction)client_capi_run, METH_VARARGS,
     "Run a tealet using the imported C API."},
    {"capi_run_c", (PyCFunction)client_capi_run_c, METH_VARARGS,
     "Run a tealet using a native C callback via the imported C API."},
    {"capi_switch", (PyCFunction)client_capi_switch, METH_VARARGS,
     "Switch to a tealet using the imported C API."},
    {"capi_switch_flags", (PyCFunction)client_capi_switch_flags, METH_VARARGS,
     "Switch to a tealet using the imported C API with explicit flags."},
    {"capi_throw", (PyCFunction)client_capi_throw, METH_VARARGS,
     "Throw into a tealet using the imported C API with optional flags."},
    {"capi_set_exception", (PyCFunction)client_capi_set_exception, METH_VARARGS,
     "Set pending exception on a target tealet via imported C API."},
    {"capi_thread_reap", (PyCFunction)client_capi_thread_reap, METH_VARARGS,
     "Run thread_reap via imported C API."},
    {"capi_thread_sweep", (PyCFunction)client_capi_thread_sweep, METH_NOARGS,
     "Run thread_sweep via imported C API."},
    {"capi_thread_active", (PyCFunction)client_capi_thread_active, METH_NOARGS,
     "List active wrappers via imported C API."},
    {"capi_thread_kill", (PyCFunction)client_capi_thread_kill, METH_VARARGS,
     "Run thread_kill via imported C API."},
    {"capi_error_was_remote", (PyCFunction)client_capi_error_was_remote, METH_NOARGS,
     "Return error_was_remote via imported C API."},
    {"capi_previous", (PyCFunction)client_capi_previous, METH_NOARGS,
     "Return module-level previous() via imported C API."},
    {"capi_frame_introspection", (PyCFunction)client_capi_frame_introspection, METH_NOARGS,
     "Return frame introspection setting via imported C API."},
    {"capi_frame_introspection_set", (PyCFunction)client_capi_frame_introspection_set, METH_O,
     "Set frame introspection setting via imported C API."},
    {"capi_is_foreign", (PyCFunction)client_capi_is_foreign, METH_O,
     "Return whether target is foreign to current thread via imported C API."},
    {"capi_state", (PyCFunction)client_capi_state, METH_O,
     "Return target state via imported C API."},
    {"capi_thread_id", (PyCFunction)client_capi_thread_id, METH_O,
     "Return target thread_id via imported C API."},
    {NULL, NULL, 0, NULL},
};

static int client_exec(PyObject *module) {
    PyTealetCapiClientState *state = (PyTealetCapiClientState *)PyModule_GetState(module);

    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "client module state unavailable");
        return -1;
    }

    memset(state, 0, sizeof(*state));

    state->api = PyTealetApi_Import();
    if (!state->api)
        return -1;

    if (state->api->abi_version != PYTEALET_CAPI_ABI_VERSION) {
        PyErr_Format(PyExc_ImportError,
                     "pytealet C API ABI mismatch: expected %u, got %u",
                     (unsigned int)PYTEALET_CAPI_ABI_VERSION,
                     (unsigned int)state->api->abi_version);
        return -1;
    }

    if (state->api->struct_size < sizeof(PyTealet_CAPI)) {
        PyErr_SetString(PyExc_ImportError, "pytealet C API table is too small");
        return -1;
    }

    if (!state->api->ctx_new || !state->api->ctx_free || !state->api->current || !state->api->main ||
        !state->api->thread_sweep || !state->api->check_tealet || !state->api->create || !state->api->duplicate ||
        !state->api->stub || !state->api->prepare || !state->api->run || !state->api->switch_ ||
        !state->api->throw_ || !state->api->set_exception || !state->api->thread_reap ||
        !state->api->thread_active || !state->api->thread_kill || !state->api->error_was_remote ||
        !state->api->previous || !state->api->frame_introspection_get || !state->api->frame_introspection_set ||
        !state->api->is_foreign || !state->api->state_get || !state->api->thread_id_get) {
        PyErr_SetString(PyExc_ImportError, "pytealet C API missing required functions");
        return -1;
    }

    if (PyModule_AddIntConstant(module, "SWITCH_PANIC", (long)PYTEALET_SWITCH_PANIC) < 0)
        return -1;
    if (PyModule_AddIntConstant(module, "THROW_PANIC", (long)PYTEALET_THROW_PANIC) < 0)
        return -1;

    state->ctx = state->api->ctx_new();
    if (!state->ctx)
        return -1;

    return 0;
}

static int client_clear(PyObject *module) {
    PyTealetCapiClientState *state = (PyTealetCapiClientState *)PyModule_GetState(module);

    if (!state)
        return 0;

    if (state->ctx && state->api && state->api->ctx_free) {
        state->api->ctx_free(state->ctx);
        state->ctx = NULL;
    }

    state->api = NULL;
    return 0;
}

static void client_free(void *module) {
    (void)client_clear((PyObject *)module);
}

/* CPython API uses void* in module slots; this conversion is intentional. */
#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
static PyModuleDef_Slot client_slots[] = {{Py_mod_exec, client_exec},
#if defined(Py_mod_gil)
                                          {Py_mod_gil, Py_MOD_GIL_NOT_USED},
#endif
                                          {0, NULL}};
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

static struct PyModuleDef client_module = {
    PyModuleDef_HEAD_INIT,
    "_tealet_capi_client",
    NULL,
    sizeof(PyTealetCapiClientState),
    client_methods,
    client_slots,
    NULL,
    client_clear,
    client_free,
};

PyMODINIT_FUNC PyInit__tealet_capi_client(void) {
    return PyModuleDef_Init(&client_module);
}
