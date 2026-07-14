/* pytealet_lineage.c - thread-lineage ring and sweep/reap lifecycle helpers. */

#include "pytealet_lineage.h"

#include "pytealet_throw.h"

#include <assert.h>

/* Generic circular-ring helpers for PyTealetMainData nodes.
 * The ring anchor points at an arbitrary node in the ring, or NULL for empty.
 */
static void pytealet_ring_append(PyTealetMainData **ring_anchor_io, PyTealetMainData *mdata) {
    PyTealetMainData *head;
    PyTealetMainData *tail;

    assert(ring_anchor_io);
    assert(mdata);
    assert(!mdata->ring_prev && !mdata->ring_next);

    head = *ring_anchor_io;
    if (!head) {
        mdata->ring_prev = mdata;
        mdata->ring_next = mdata;
        *ring_anchor_io = mdata;
        return;
    }

    tail = head->ring_prev;
    assert(tail);
    mdata->ring_prev = tail;
    mdata->ring_next = head;
    tail->ring_next = mdata;
    head->ring_prev = mdata;
}

static void pytealet_ring_remove(PyTealetMainData **ring_anchor_io, PyTealetMainData *mdata) {
    assert(ring_anchor_io);
    assert(mdata);

    if (!mdata->ring_next || !mdata->ring_prev) {
        assert(mdata->ring_next == NULL && mdata->ring_prev == NULL);
        return;
    }

    if (mdata->ring_next == mdata) {
        assert(mdata->ring_prev == mdata);
        if (*ring_anchor_io == mdata)
            *ring_anchor_io = NULL;
    } else {
        if (*ring_anchor_io == mdata)
            *ring_anchor_io = mdata->ring_next;
        mdata->ring_prev->ring_next = mdata->ring_next;
        mdata->ring_next->ring_prev = mdata->ring_prev;
    }

    mdata->ring_prev = NULL;
    mdata->ring_next = NULL;
}

static PyTealetMainData *pytealet_ring_pop(PyTealetMainData **ring_anchor_io) {
    PyTealetMainData *head;

    assert(ring_anchor_io);
    head = *ring_anchor_io;
    if (!head)
        return NULL;
    pytealet_ring_remove(ring_anchor_io, head);
    return head;
}

int PyTealet_LineageLinkThreadData(PyTealetMainData *mdata) {
    PyTealetModuleState *mstate;

    assert(mdata);
    mstate = mdata->mstate;
    assert(mstate);
    assert(mstate->thread_data_lock);

    PyThread_acquire_lock(mstate->thread_data_lock, WAIT_LOCK);
    pytealet_ring_append(&mstate->thread_data_ring, mdata);
    PyThread_release_lock(mstate->thread_data_lock);
    return 0;
}

static void pytealet_unlink_thread_data_locked(PyTealetMainData *mdata) {
    PyTealetModuleState *mstate;

    assert(mdata);
    mstate = mdata->mstate;
    assert(mstate);
    assert(mstate->thread_data_lock);

    pytealet_ring_remove(&mstate->thread_data_ring, mdata);
}

static void pytealet_unlink_thread_data(PyTealetMainData *mdata) {
    PyTealetModuleState *mstate;

    assert(mdata);
    mstate = mdata->mstate;
    assert(mstate);
    assert(mstate->thread_data_lock);

    PyThread_acquire_lock(mstate->thread_data_lock, WAIT_LOCK);
    pytealet_unlink_thread_data_locked(mdata);
    PyThread_release_lock(mstate->thread_data_lock);
}

static void pytealet_lineage_lock(PyTealetMainData *mdata) {
    assert(mdata);
    assert(mdata->domain_lock_obj);
    pytealet_domain_lock_obj_lock(mdata->domain_lock_obj);
}

static void pytealet_lineage_unlock(PyTealetMainData *mdata) {
    assert(mdata);
    assert(mdata->domain_lock_obj);
    pytealet_domain_lock_obj_unlock(mdata->domain_lock_obj);
}

static int pytealet_weakref_get_live(PyObject *wref, PyObject **obj_out) {
    *obj_out = NULL;
#if defined(PY313P)
    return PyWeakref_GetRef(wref, obj_out);
#else
    {
        PyObject *obj = PyWeakref_GetObject(wref);
        if (!obj || obj == Py_None)
            return 0;
        *obj_out = Py_NewRef(obj);
        return 1;
    }
#endif
}

int PyTealet_LineageReapInner(PyTealetMainData *mdata, PyObject *nerfed, int clear_current_tss, int best_effort) {
    PyTealetModuleState *mstate;
    PyObject *wrappers = NULL;
    PyObject *wref;
    tealet_t *main_tealet = NULL;

    if (!mdata)
        return 0;
    mstate = mdata->mstate;
    assert(mstate);

    if (mdata->main_wrapper) {
        PyTealetObject *main_wrapper = (PyTealetObject *)mdata->main_wrapper;
        main_tealet = main_wrapper->tealet;
    }

    wrappers = mdata->wrappers;
    assert(wrappers && PySet_Check(wrappers));

    while (1) {
        PyObject *obj = NULL;
        PyTealetObject *wrapper;
        int weak_status;

        pytealet_lineage_lock(mdata);
        wref = PySet_Pop(wrappers);
        pytealet_lineage_unlock(mdata);
        if (!wref) {
            if (PyErr_ExceptionMatches(PyExc_KeyError)) {
                PyErr_Clear();
                break;
            }
            if (!best_effort)
                return -1;
            goto best_effort_fail;
        }

        weak_status = pytealet_weakref_get_live(wref, &obj);
        Py_DECREF(wref);
        if (weak_status < 0) {
            if (!best_effort)
                return -1;
            goto best_effort_fail;
        }
        if (weak_status == 0)
            continue;

        assert(mstate->tealet_type && PyObject_TypeCheck(obj, mstate->tealet_type));
        wrapper = (PyTealetObject *)obj;
        if (!wrapper->tealet) {
            wrapper->inflight_throw_token = 0;
            Py_DECREF(obj);
            continue;
        }
        /* Ignore main tealet, handled separately below. */
        if (TEALET_IS_MAIN(wrapper->tealet)) {
            Py_DECREF(obj);
            continue;
        }

        if (wrapper->inflight_throw_token != 0) {
            PyObject *old_exc = NULL;
            PyObject *old_fallback = NULL;
            int pop_rc =
                PyTealetThrow_RegistryPop(mdata, wrapper->inflight_throw_token, &old_exc, &old_fallback);
            if (pop_rc < 0) {
                if (!best_effort) {
                    Py_DECREF(obj);
                    return -1;
                }
                PyErr_WriteUnraisable(Py_None);
                PyErr_Clear();
            }
            Py_XDECREF(old_exc);
            Py_XDECREF(old_fallback);
            wrapper->inflight_throw_token = 0;
        }

        if (nerfed) {
            int add_to_list = (tealet_status(wrapper->tealet) == TEALET_STATUS_ACTIVE);
            /* Stubs are safe to delete and do not leak memory. */
            if (wrapper->state == STATE_STUB || wrapper->state == STATE_PREPARED)
                add_to_list = 0;
            if (add_to_list) {
                if (PyList_Append(nerfed, obj) < 0) {
                    Py_DECREF(obj);
                    if (!best_effort)
                        return -1;
                    goto best_effort_fail;
                }
            }
        }

        {
            tealet_t *tealet_to_delete = wrapper->tealet;
            pytealet_domain_lock_obj_lock(wrapper->domain_lock_obj);
            if (tealet_to_delete)
                TEALET_SET_PYOBJECT(tealet_to_delete, NULL);
            wrapper->tealet = NULL;
            wrapper->state = STATE_EXIT;
            pytealet_domain_lock_obj_unlock(wrapper->domain_lock_obj);
            if (tealet_to_delete)
                tealet_delete(tealet_to_delete);
        }
        Py_CLEAR(wrapper->tracking_ref);
        Py_DECREF(obj);
    }

    /* Clear main tealet and destroy the lineage. */
    if (mdata->main_wrapper) {
        PyTealetObject *main_wrapper = (PyTealetObject *)mdata->main_wrapper;
        pytealet_domain_lock_obj_lock(main_wrapper->domain_lock_obj);
        if (main_wrapper->tealet)
            TEALET_SET_PYOBJECT(main_wrapper->tealet, NULL);
        main_wrapper->tealet = NULL;
        main_wrapper->state = STATE_EXIT;
        pytealet_domain_lock_obj_unlock(main_wrapper->domain_lock_obj);
    }
    Py_CLEAR(mdata->main_wrapper);
    if (main_tealet)
        tealet_finalize(main_tealet);

    if (mdata->pending_throw_token != 0) {
        PyObject *old_exc = NULL;
        PyObject *old_fallback = NULL;
        int pop_rc = PyTealetThrow_RegistryPop(mdata, mdata->pending_throw_token, &old_exc, &old_fallback);
        if (pop_rc < 0) {
            if (!best_effort)
                return -1;
            PyErr_WriteUnraisable(Py_None);
            PyErr_Clear();
        }
        Py_XDECREF(old_exc);
        Py_XDECREF(old_fallback);
        mdata->pending_throw_token = 0;
    }

    Py_CLEAR(mdata->dustbin);
    Py_CLEAR(mdata->wrappers);
    Py_CLEAR(mdata->throw_records);
    pytealet_unlink_thread_data(mdata);
    Py_CLEAR(mdata->domain_lock_obj);
    PyMem_Free(mdata);

    if (clear_current_tss)
        (void)PyThread_tss_set(&mstate->tls_key, NULL);

    return 0;

best_effort_fail:
    /* Best-effort cleanup for teardown paths. */
    PyErr_WriteUnraisable(Py_None);
    PyErr_Clear();
    pytealet_unlink_thread_data(mdata);
    return -1;
}

int PyTealet_ThreadReapMdataForTeardown(PyTealetMainData *mdata) {
    return PyTealet_LineageReapInner(mdata, NULL, 0, 1);
}

int PyTealet_LineageThreadIdentIsAlive(unsigned long thread_id, int *alive_out) {
    PyObject *threading_mod = NULL;
    PyObject *active = NULL;
    PyObject *tid_obj = NULL;
    int contains = 0;

    assert(alive_out);
    *alive_out = 0;

    if (thread_id == (unsigned long)PyThread_get_thread_ident()) {
        *alive_out = 1;
        return 1;
    }

    threading_mod = PyImport_ImportModule("threading");
    if (!threading_mod) {
        PyErr_Clear();
        return 0;
    }

    active = PyObject_GetAttrString(threading_mod, "_active");
    Py_DECREF(threading_mod);
    if (!active) {
        PyErr_Clear();
        return 0;
    }

    tid_obj = PyLong_FromUnsignedLong(thread_id);
    if (!tid_obj) {
        Py_DECREF(active);
        PyErr_Clear();
        return 0;
    }

    if (PyDict_Check(active))
        contains = PyDict_Contains(active, tid_obj);
    else
        contains = PyMapping_HasKey(active, tid_obj);

    Py_DECREF(tid_obj);
    Py_DECREF(active);

    if (contains < 0) {
        PyErr_Clear();
        return 0;
    }

    *alive_out = contains ? 1 : 0;
    return 1;
}

static int pytealet_tid_set_contains(PyObject *known_thread_ids, long tid, int *contains_out) {
    PyObject *tid_obj;
    int contains;

    assert(known_thread_ids && PySet_Check(known_thread_ids));
    assert(contains_out);

    tid_obj = PyLong_FromLong(tid);
    if (!tid_obj)
        return -1;
    contains = PySet_Contains(known_thread_ids, tid_obj);
    Py_DECREF(tid_obj);
    if (contains < 0)
        return -1;
    *contains_out = contains ? 1 : 0;
    return 0;
}

static int pytealet_tid_set_add(PyObject *known_thread_ids, long tid) {
    PyObject *tid_obj;
    int rc;

    assert(known_thread_ids && PySet_Check(known_thread_ids));

    tid_obj = PyLong_FromLong(tid);
    if (!tid_obj)
        return -1;
    rc = PySet_Add(known_thread_ids, tid_obj);
    Py_DECREF(tid_obj);
    return rc;
}

static PyObject *pytealet_collect_known_thread_ids(unsigned long current_tid) {
    PyObject *known_thread_ids;
    PyObject *threading_mod = NULL;
    PyObject *active = NULL;
    PyObject *keys = NULL;
    PyObject *iter = NULL;
    PyObject *key;

    known_thread_ids = PySet_New(NULL);
    if (!known_thread_ids)
        return NULL;

    if (pytealet_tid_set_add(known_thread_ids, (long)current_tid) < 0) {
        Py_DECREF(known_thread_ids);
        return NULL;
    }

    threading_mod = PyImport_ImportModule("threading");
    if (!threading_mod) {
        PyErr_Clear();
        return known_thread_ids;
    }

    active = PyObject_GetAttrString(threading_mod, "_active");
    Py_DECREF(threading_mod);
    if (!active) {
        PyErr_Clear();
        return known_thread_ids;
    }

    keys = PyMapping_Keys(active);
    Py_DECREF(active);
    if (!keys) {
        PyErr_Clear();
        return known_thread_ids;
    }

    iter = PyObject_GetIter(keys);
    Py_DECREF(keys);
    if (!iter) {
        PyErr_Clear();
        return known_thread_ids;
    }

    while ((key = PyIter_Next(iter)) != NULL) {
        if (PySet_Add(known_thread_ids, key) < 0) {
            Py_DECREF(key);
            Py_DECREF(iter);
            Py_DECREF(known_thread_ids);
            return NULL;
        }
        Py_DECREF(key);
    }
    if (PyErr_Occurred())
        PyErr_Clear();

    Py_DECREF(iter);
    return known_thread_ids;
}

static void pytealet_detached_ring_drain(PyTealetMainData **ring_io) {
    PyTealetMainData *mdata;

    assert(ring_io);

    while ((mdata = pytealet_ring_pop(ring_io)) != NULL) {
        if (PyTealet_LineageReapInner(mdata, NULL, 0, 1) < 0) {
            if (PyErr_Occurred()) {
                PyErr_WriteUnraisable(Py_None);
                PyErr_Clear();
            }
        }
    }
}

PyObject *PyTealet_ThreadSweep(PyTealetModuleState *mstate) {
    PyObject *known_thread_ids = NULL;
    PyObject *nerfed = NULL;
    PyTealetMainData *detached_ring = NULL;
    Py_ssize_t ring_count = 0;
    Py_ssize_t idx;
    int lock_held = 0;

    assert(mstate);

    nerfed = PyList_New(0);
    if (!nerfed)
        goto error;

    known_thread_ids = pytealet_collect_known_thread_ids((unsigned long)PyThread_get_thread_ident());
    if (!known_thread_ids)
        goto error;

    assert(mstate->thread_data_lock);
    PyThread_acquire_lock(mstate->thread_data_lock, WAIT_LOCK);
    lock_held = 1;

    if (mstate->thread_data_ring) {
        PyTealetMainData *cursor = mstate->thread_data_ring;
        do {
            assert(cursor);
            assert(cursor->ring_next);
            ring_count++;
            cursor = cursor->ring_next;
        } while (cursor != mstate->thread_data_ring);
    }

    if (ring_count > 0) {
        PyTealetMainData *cursor = mstate->thread_data_ring;
        for (idx = 0; idx < ring_count; idx++) {
            PyTealetMainData *next = cursor->ring_next;
            int in_known = 0;

            if (pytealet_tid_set_contains(known_thread_ids, cursor->tid, &in_known) < 0)
                goto error;

            if (!in_known) {
                int alive = 0;
                int probed = PyTealet_LineageThreadIdentIsAlive((unsigned long)cursor->tid, &alive);

                if (probed && alive) {
                    if (pytealet_tid_set_add(known_thread_ids, cursor->tid) < 0)
                        goto error;
                } else {
                    pytealet_unlink_thread_data_locked(cursor);
                    pytealet_ring_append(&detached_ring, cursor);
                }
            }

            cursor = next;
        }
    }

    PyThread_release_lock(mstate->thread_data_lock);
    lock_held = 0;

    while (detached_ring) {
        PyTealetMainData *detached = pytealet_ring_pop(&detached_ring);

        assert(detached);

        if (PyTealet_LineageReapInner(detached, nerfed, 0, 1) < 0) {
            if (PyErr_Occurred()) {
                PyErr_WriteUnraisable(Py_None);
                PyErr_Clear();
            }
        }
    }

    Py_DECREF(known_thread_ids);
    return nerfed;

error:
    if (lock_held)
        PyThread_release_lock(mstate->thread_data_lock);

    if (detached_ring) {
        PyObject *etype = NULL;
        PyObject *evalue = NULL;
        PyObject *etb = NULL;

        PyErr_Fetch(&etype, &evalue, &etb);
        pytealet_detached_ring_drain(&detached_ring);
        PyErr_Restore(etype, evalue, etb);
    }

    Py_XDECREF(known_thread_ids);
    Py_XDECREF(nerfed);
    return NULL;
}
