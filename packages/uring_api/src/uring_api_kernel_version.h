#ifndef URING_API_KERNEL_VERSION_H
#define URING_API_KERNEL_VERSION_H

/* private implementation header; not part of the public C API. */

/*
 * Parse the running kernel release via uname(2) once per process and compare
 * against documented minimum versions from io_uring_enter(2) and liburing prep
 * helpers. See uring_api_kernel_versions.h for per-capability floors.
 */
int uring_api_kernel_version_at_least(int major, int minor, int patch);

#endif