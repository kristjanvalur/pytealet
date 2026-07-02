/*
 * Running-kernel version helpers for version-gated capability reporting.
 */

#include "uring_api_kernel_version.h"

#include <stdio.h>
#include <sys/utsname.h>

static int kernel_version_cache_ready = 0;
static int kernel_version_major = 0;
static int kernel_version_minor = 0;
static int kernel_version_patch = 0;

static int parse_kernel_version(void) {
    struct utsname uts;
    int matched;

    if (kernel_version_cache_ready) {
        return 0;
    }

    kernel_version_major = 0;
    kernel_version_minor = 0;
    kernel_version_patch = 0;

    if (uname(&uts) != 0) {
        kernel_version_cache_ready = 1;
        return 0;
    }

    /* sscanf stops at the first non-digit in each field, so 5.6.0-rc1 parses as 5.6.0. */
    matched = sscanf(uts.release, "%d.%d.%d", &kernel_version_major, &kernel_version_minor, &kernel_version_patch);
    if (matched < 2) {
        kernel_version_major = 0;
        kernel_version_minor = 0;
        kernel_version_patch = 0;
    } else if (matched == 2) {
        kernel_version_patch = 0;
    }

    kernel_version_cache_ready = 1;
    return 0;
}

int uring_api_kernel_version_at_least(int major, int minor, int patch) {
    if (parse_kernel_version() < 0) {
        return 0;
    }

    if (kernel_version_major != major) {
        return kernel_version_major > major;
    }
    if (kernel_version_minor != minor) {
        return kernel_version_minor > minor;
    }
    return kernel_version_patch >= patch;
}