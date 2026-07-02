#ifndef URING_API_STATX_LAYOUT_H
#define URING_API_STATX_LAYOUT_H

#include <stddef.h>

/*
 * Prefix of Linux struct statx through stx_size. Used for compile-time layout
 * checks without pulling linux/stat.h into translation units that already
 * include libc fcntl definitions via liburing.
 */
struct uring_api_statx_stx_size_prefix {
    unsigned int stx_mask;
    unsigned int stx_blksize;
    unsigned long long stx_attributes;
    unsigned int stx_nlink;
    unsigned int stx_uid;
    unsigned int stx_gid;
    unsigned short stx_mode;
    unsigned short stx__spare0[1];
    unsigned long long stx_ino;
    unsigned long long stx_size;
};

#define URING_API_STATX_STX_SIZE_OFFSET ((int)offsetof(struct uring_api_statx_stx_size_prefix, stx_size))

_Static_assert(URING_API_STATX_STX_SIZE_OFFSET == 40, "uring-api statx layout drift");

#endif