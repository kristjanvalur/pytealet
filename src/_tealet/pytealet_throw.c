/* pytealet_throw.c - throw registry and deferred exception delivery helpers. */

#include "pytealet_throw.h"

#include <assert.h>

uint64_t PyTealetThrow_NextToken(PyTealetMainData *mdata) {
    uint64_t token;
    assert(mdata);
    token = ++mdata->throw_next_token;
    if (token == 0)
        token = ++mdata->throw_next_token;
    return token;
}

int PyTealetThrow_RegistrySet(PyTealetMainData *mdata, uint64_t token, PyObject *exc, PyObject *fallback) {
    PyObject *key;
    PyObject *record;
    int rc;

    assert(mdata);
    assert(mdata->throw_records && PyDict_Check(mdata->throw_records));
    assert(token != 0);
    assert(exc);

    if (!fallback)
        fallback = Py_None;

    key = PyLong_FromUnsignedLongLong(token);
    if (!key)
        return -1;

    record = PyTuple_New(2);
    if (!record) {
        Py_DECREF(key);
        return -1;
    }
    PyTuple_SET_ITEM(record, 0, Py_NewRef(exc));
    PyTuple_SET_ITEM(record, 1, Py_NewRef(fallback));

    rc = PyDict_SetItem(mdata->throw_records, key, record);
    Py_DECREF(record);
    Py_DECREF(key);
    return rc;
}

int PyTealetThrow_RegistryPop(PyTealetMainData *mdata, uint64_t token, PyObject **exc_out, PyObject **fallback_out) {
    PyObject *key;
    PyObject *record;
    PyObject *exc;
    PyObject *fallback;
    int found = 0;

    *exc_out = NULL;
    *fallback_out = NULL;

    assert(mdata);
    assert(mdata->throw_records && PyDict_Check(mdata->throw_records));
    assert(token != 0);

    key = PyLong_FromUnsignedLongLong(token);
    if (!key)
        return -1;

    record = PyDict_GetItemWithError(mdata->throw_records, key);
    if (!record) {
        Py_DECREF(key);
        if (PyErr_Occurred())
            return -1;
        return 0;
    }
    Py_INCREF(record);
    found = 1;

    if (PyDict_DelItem(mdata->throw_records, key) < 0) {
        Py_DECREF(record);
        Py_DECREF(key);
        return -1;
    }
    Py_DECREF(key);

    if (!PyTuple_Check(record) || PyTuple_GET_SIZE(record) != 2) {
        Py_DECREF(record);
        PyErr_SetString(PyExc_RuntimeError, "corrupt throw record");
        return -1;
    }

    exc = PyTuple_GET_ITEM(record, 0);
    fallback = PyTuple_GET_ITEM(record, 1);
    *exc_out = Py_NewRef(exc);
    if (fallback != Py_None)
        *fallback_out = Py_NewRef(fallback);
    Py_DECREF(record);
    return found;
}

void PyTealetThrow_ClearPendingException(PyTealetMainData *mdata) {
    uint64_t token;
    int pop_rc;
    PyObject *old_exc = NULL;
    PyObject *old_fallback = NULL;

    assert(mdata);

    token = mdata->pending_throw_token;
    if (token == 0)
        return;

    mdata->pending_throw_token = 0;
    pop_rc = PyTealetThrow_RegistryPop(mdata, token, &old_exc, &old_fallback);
    if (pop_rc < 0) {
        PyErr_WriteUnraisable(NULL);
        PyErr_Clear();
        return;
    }
    Py_XDECREF(old_exc);
    Py_XDECREF(old_fallback);
}

PyObject *PyTealetThrow_TakePendingException(PyTealetMainData *mdata) {
    uint64_t token;
    int pop_rc;
    PyObject *exc = NULL;
    PyObject *fallback = NULL;

    assert(mdata);

    token = mdata->pending_throw_token;
    if (token == 0)
        return NULL;

    mdata->pending_throw_token = 0;
    pop_rc = PyTealetThrow_RegistryPop(mdata, token, &exc, &fallback);
    if (pop_rc < 0) {
        PyErr_WriteUnraisable(NULL);
        PyErr_Clear();
        return NULL;
    }
    if (pop_rc == 0)
        return NULL;

    Py_XDECREF(fallback);
    return exc;
}

int PyTealetThrow_ExceptionChainContains(PyObject *raised, PyObject *needle) {
    PyObject *stack[256];
    int top = 0;
    int steps = 0;

    if (!raised || !needle)
        return 0;
    if (!PyExceptionInstance_Check(raised) || !PyExceptionInstance_Check(needle))
        return 0;

    stack[top++] = Py_NewRef(raised);

    while (top > 0 && steps < 256) {
        PyObject *cursor = stack[--top];
        PyObject *cause = NULL;
        PyObject *context = NULL;

        if (cursor == needle) {
            Py_DECREF(cursor);
            while (top > 0)
                Py_DECREF(stack[--top]);
            return 1;
        }

        cause = PyException_GetCause(cursor);
        context = PyException_GetContext(cursor);
        Py_DECREF(cursor);

        if (cause) {
            if (top < (int)(sizeof(stack) / sizeof(stack[0]))) {
                stack[top++] = cause;
            } else {
                Py_DECREF(cause);
            }
        }
        if (context) {
            if (top < (int)(sizeof(stack) / sizeof(stack[0]))) {
                stack[top++] = context;
            } else {
                Py_DECREF(context);
            }
        }
        steps++;
    }

    while (top > 0)
        Py_DECREF(stack[--top]);
    return 0;
}

PyObject *PyTealetThrow_GetRaisedException(void) {
#ifdef Py312P
    return PyErr_GetRaisedException();
#else
    PyObject *exc;
    PyObject *val;
    PyObject *tb;

    PyErr_Fetch(&exc, &val, &tb);
    PyErr_NormalizeException(&exc, &val, &tb);
    /* Decrefing these is safe; val keeps the instance alive. */
    Py_XDECREF(exc);
    Py_XDECREF(tb);
    return val;
#endif
}

void PyTealetThrow_SetRaisedException(PyObject *exc) {
#ifdef Py312P
    PyErr_SetRaisedException(exc);
#else
    if (!exc) {
        PyErr_Clear();
    } else {
        PyErr_SetObject((PyObject *)Py_TYPE(exc), exc);
        Py_DECREF(exc);
    }
#endif
}

int PyTealetThrow_RedirectUncaught(PyTealetModuleState *mstate, PyTealetMainData *mdata, PyTealetObject *tealet,
                                   PyObject *exception, PyTealetObject **return_to_io) {
    uint64_t token;
    PyObject *throw_exc = NULL;
    PyObject *throw_fallback = NULL;
    int pop_rc;

    assert(mstate);
    assert(mdata);
    assert(tealet);
    assert(return_to_io && *return_to_io != NULL);

    if (tealet->inflight_throw_token == 0)
        return 0;

    token = tealet->inflight_throw_token;
    tealet->inflight_throw_token = 0;

    pop_rc = PyTealetThrow_RegistryPop(mdata, token, &throw_exc, &throw_fallback);
    if (pop_rc < 0) {
        PyErr_WriteUnraisable(NULL);
        PyErr_Clear();
    } else if (pop_rc > 0 && exception && throw_fallback &&
               PyTealetThrow_ExceptionChainContains(exception, throw_exc)) {
        PyTealetObject *fallback_t = (PyTealetObject *)throw_fallback;
        assert(mstate->tealet_type);
        assert(PyObject_TypeCheck((PyObject *)fallback_t, mstate->tealet_type));
        assert(fallback_t->owner_tid == tealet->owner_tid);
        assert(fallback_t->tealet->main == tealet->tealet->main);
        if (fallback_t->state == STATE_RUN && fallback_t->tealet) {
            /* transfer ownership of throw_fallback to *return_to_io */
            Py_DECREF(*return_to_io);
            *return_to_io = fallback_t;
            Py_DECREF(throw_exc);
            return 1;
        }
    }
    Py_XDECREF(throw_exc);
    Py_XDECREF(throw_fallback);
    return 0;
}

PyObject *PyTealetThrow_MaybeRaisePending(PyTealetMainData *mdata, PyTealetObject *current, PyObject *result) {
    uint64_t token;
    int pop_rc;
    PyObject *exc = NULL;
    PyObject *fallback = NULL;

    assert(mdata);
    assert(current);

    token = mdata->pending_throw_token;
    if (token == 0)
        return result;

    mdata->pending_throw_token = 0;
    pop_rc = PyTealetThrow_RegistryPop(mdata, token, &exc, &fallback);
    if (pop_rc < 0) {
        Py_DECREF(result);
        return NULL;
    }
    if (pop_rc == 0)
        return result;

    if (fallback) {
        if (current->inflight_throw_token != 0) {
            PyObject *old_exc = NULL;
            PyObject *old_fallback = NULL;
            int old_pop_rc =
                PyTealetThrow_RegistryPop(mdata, current->inflight_throw_token, &old_exc, &old_fallback);
            if (old_pop_rc < 0) {
                Py_DECREF(result);
                Py_DECREF(exc);
                Py_DECREF(fallback);
                return NULL;
            }
            Py_XDECREF(old_exc);
            Py_XDECREF(old_fallback);
            current->inflight_throw_token = 0;
        }
        if (PyTealetThrow_RegistrySet(mdata, token, exc, fallback) < 0) {
            Py_DECREF(result);
            Py_DECREF(exc);
            Py_DECREF(fallback);
            return NULL;
        }
        current->inflight_throw_token = token;
    }

    Py_DECREF(result);
    assert(PyExceptionInstance_Check(exc));

    mdata->last_error_remote = 1;
    PyTealetThrow_SetRaisedException(exc);
    Py_XDECREF(fallback);
    return NULL;
}
