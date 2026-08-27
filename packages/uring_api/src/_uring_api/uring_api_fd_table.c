/*
 * Per-fd send-all busy table and conflict FIFO.
 */

#include "uring_api_fd_table.h"
#include "uring_api_completion.h"

#define FD_TABLE_INITIAL_CAP 8

static unsigned int fd_hash(int fd, size_t cap) { return ((unsigned int)fd * 2654435761u) & (unsigned int)(cap - 1); }

static int fifo_grow(UringApiCompletionFifo *fifo) {
    UringApiCompletion **grown;
    size_t cap = fifo->cap == 0 ? 4 : fifo->cap * 2;
    size_t i;

    grown = (UringApiCompletion **)PyMem_Malloc(cap * sizeof(*grown));
    if (!grown) {
        PyErr_NoMemory();
        return -1;
    }
    for (i = 0; i < fifo->count; i++) {
        grown[i] = fifo->items[(fifo->head + i) % fifo->cap];
    }
    PyMem_Free(fifo->items);
    fifo->items = grown;
    fifo->head = 0;
    fifo->cap = cap;
    return 0;
}

void completion_fifo_clear(UringApiCompletionFifo *fifo) {
    size_t i;

    for (i = 0; i < fifo->count; i++) {
        Py_DECREF(fifo->items[(fifo->head + i) % fifo->cap]);
    }
    PyMem_Free(fifo->items);
    fifo->items = NULL;
    fifo->head = 0;
    fifo->count = 0;
    fifo->cap = 0;
}

int completion_fifo_push(UringApiCompletionFifo *fifo, UringApiCompletion *completion) {
    if (fifo->count == fifo->cap && fifo_grow(fifo) < 0) {
        return -1;
    }
    fifo->items[(fifo->head + fifo->count) % fifo->cap] = completion;
    fifo->count++;
    Py_INCREF(completion);
    return 0;
}

UringApiCompletion *completion_fifo_peek(UringApiCompletionFifo *fifo) {
    if (fifo->count == 0) {
        return NULL;
    }
    return fifo->items[fifo->head];
}

UringApiCompletion *completion_fifo_pop(UringApiCompletionFifo *fifo) {
    UringApiCompletion *completion;

    if (fifo->count == 0) {
        return NULL;
    }
    completion = fifo->items[fifo->head];
    fifo->head = (fifo->head + 1) % fifo->cap;
    fifo->count--;
    return completion;
}

int fd_table_fifo_push(UringApiFdSlot *slot, UringApiCompletion *completion) {
    return completion_fifo_push(&slot->fifo, completion);
}

UringApiCompletion *fd_table_fifo_peek(UringApiFdSlot *slot) { return completion_fifo_peek(&slot->fifo); }

UringApiCompletion *fd_table_fifo_pop(UringApiFdSlot *slot) { return completion_fifo_pop(&slot->fifo); }

static int fd_table_rehash(UringApiRing *self, size_t new_cap) {
    UringApiFdSlot **grown;
    size_t i;

    grown = (UringApiFdSlot **)PyMem_Calloc(new_cap, sizeof(*grown));
    if (!grown) {
        PyErr_NoMemory();
        return -1;
    }
    for (i = 0; i < self->fd_slot_cap; i++) {
        UringApiFdSlot *slot = self->fd_slots[i];

        while (slot) {
            UringApiFdSlot *next = slot->hash_next;
            unsigned int bucket = fd_hash(slot->fd, new_cap);

            slot->hash_next = grown[bucket];
            grown[bucket] = slot;
            slot = next;
        }
    }
    PyMem_Free(self->fd_slots);
    self->fd_slots = grown;
    self->fd_slot_cap = new_cap;
    return 0;
}

UringApiFdSlot *fd_table_lookup(UringApiRing *self, int fd) {
    UringApiFdSlot *slot;

    if (self->fd_slot_cap == 0) {
        return NULL;
    }
    slot = self->fd_slots[fd_hash(fd, self->fd_slot_cap)];
    while (slot) {
        if (slot->fd == fd) {
            return slot;
        }
        slot = slot->hash_next;
    }
    return NULL;
}

UringApiFdSlot *fd_table_get(UringApiRing *self, int fd) {
    UringApiFdSlot *slot;
    unsigned int bucket;

    slot = fd_table_lookup(self, fd);
    if (slot) {
        return slot;
    }
    if (self->fd_slot_cap == 0) {
        if (fd_table_rehash(self, FD_TABLE_INITIAL_CAP) < 0) {
            return NULL;
        }
    } else if (self->fd_slot_count * 4 >= self->fd_slot_cap * 3) {
        if (fd_table_rehash(self, self->fd_slot_cap * 2) < 0) {
            return NULL;
        }
    }
    slot = (UringApiFdSlot *)PyMem_Calloc(1, sizeof(*slot));
    if (!slot) {
        PyErr_NoMemory();
        return NULL;
    }
    slot->fd = fd;
    bucket = fd_hash(fd, self->fd_slot_cap);
    slot->hash_next = self->fd_slots[bucket];
    self->fd_slots[bucket] = slot;
    self->fd_slot_count++;
    return slot;
}

void fd_table_mark_drain(UringApiRing *self, UringApiFdSlot *slot) {
    if (slot->on_drain_list) {
        return;
    }
    slot->drain_next = self->fd_drain_head;
    self->fd_drain_head = slot;
    slot->on_drain_list = 1;
}

void fd_table_unlink_drain(UringApiRing *self, UringApiFdSlot *slot) {
    UringApiFdSlot **walk;

    if (!slot->on_drain_list) {
        return;
    }
    walk = &self->fd_drain_head;
    while (*walk) {
        if (*walk == slot) {
            *walk = slot->drain_next;
            break;
        }
        walk = &(*walk)->drain_next;
    }
    slot->drain_next = NULL;
    slot->on_drain_list = 0;
}

void fd_table_try_free(UringApiRing *self, UringApiFdSlot *slot) {
    UringApiFdSlot **walk;
    unsigned int bucket;

    if (slot->active != NULL || slot->fifo.count > 0) {
        return;
    }
    fd_table_unlink_drain(self, slot);
    bucket = fd_hash(slot->fd, self->fd_slot_cap);
    walk = &self->fd_slots[bucket];
    while (*walk) {
        if (*walk == slot) {
            *walk = slot->hash_next;
            break;
        }
        walk = &(*walk)->hash_next;
    }
    completion_fifo_clear(&slot->fifo);
    PyMem_Free(slot);
    self->fd_slot_count--;
}

int completion_fifo_traverse(UringApiCompletionFifo *fifo, visitproc visit, void *arg) {
    size_t j;

    for (j = 0; j < fifo->count; j++) {
        Py_VISIT(fifo->items[(fifo->head + j) % fifo->cap]);
    }
    return 0;
}

int fd_table_traverse(UringApiRing *self, visitproc visit, void *arg) {
    size_t i;

    for (i = 0; i < self->fd_slot_cap; i++) {
        UringApiFdSlot *slot = self->fd_slots ? self->fd_slots[i] : NULL;

        while (slot) {
            if (completion_fifo_traverse(&slot->fifo, visit, arg) < 0) {
                return -1;
            }
            slot = slot->hash_next;
        }
    }
    return 0;
}

void fd_table_clear(UringApiRing *self) {
    size_t i;

    for (i = 0; i < self->fd_slot_cap; i++) {
        UringApiFdSlot *slot = self->fd_slots ? self->fd_slots[i] : NULL;

        while (slot) {
            UringApiFdSlot *next = slot->hash_next;

            completion_fifo_clear(&slot->fifo);
            PyMem_Free(slot);
            slot = next;
        }
    }
    PyMem_Free(self->fd_slots);
    self->fd_slots = NULL;
    self->fd_slot_cap = 0;
    self->fd_slot_count = 0;
    self->fd_drain_head = NULL;
}
