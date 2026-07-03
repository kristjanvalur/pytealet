#ifndef URING_API_KERNEL_VERSIONS_H
#define URING_API_KERNEL_VERSIONS_H

/* private implementation header; not part of the public C API. */

/*
 * Documented kernel floors for io_uring submit opcodes and multishot modes.
 * Sources: io_uring_enter(2) and the liburing prep helpers in section 3.
 */

/* IORING_OP_STATX — io_uring_enter(2) */
#define URING_API_KERNEL_VERSION_STATX_MAJOR 5
#define URING_API_KERNEL_VERSION_STATX_MINOR 6
#define URING_API_KERNEL_VERSION_STATX_PATCH 0

/* IORING_POLL_ADD_MULTI multishot poll — io_uring_enter(2) */
#define URING_API_KERNEL_VERSION_POLL_MULTISHOT_MAJOR 5
#define URING_API_KERNEL_VERSION_POLL_MULTISHOT_MINOR 13
#define URING_API_KERNEL_VERSION_POLL_MULTISHOT_PATCH 0

/* multishot accept variants — io_uring_prep_accept(3) */
#define URING_API_KERNEL_VERSION_ACCEPT_MULTISHOT_MAJOR 5
#define URING_API_KERNEL_VERSION_ACCEPT_MULTISHOT_MINOR 19
#define URING_API_KERNEL_VERSION_ACCEPT_MULTISHOT_PATCH 0

/* IORING_OP_SOCKET — io_uring_enter(2) */
#define URING_API_KERNEL_VERSION_SOCKET_MAJOR 5
#define URING_API_KERNEL_VERSION_SOCKET_MINOR 19
#define URING_API_KERNEL_VERSION_SOCKET_PATCH 0

/* recv multishot — io_uring_prep_recv(3) */
#define URING_API_KERNEL_VERSION_RECV_MULTISHOT_MAJOR 6
#define URING_API_KERNEL_VERSION_RECV_MULTISHOT_MINOR 0
#define URING_API_KERNEL_VERSION_RECV_MULTISHOT_PATCH 0

/* IORING_OP_SEND_ZC — io_uring_enter(2) */
#define URING_API_KERNEL_VERSION_SEND_ZC_MAJOR 6
#define URING_API_KERNEL_VERSION_SEND_ZC_MINOR 0
#define URING_API_KERNEL_VERSION_SEND_ZC_PATCH 0

/*
 * IORING_OP_SENDMSG_ZC — same zerocopy sendmsg family as prep_sendmsg_zc(3);
 * io_uring_enter(2) documents IORING_OP_SEND_ZC at 6.0 and does not list a
 * separate floor for SENDMSG_ZC.
 */
#define URING_API_KERNEL_VERSION_SENDMSG_ZC_MAJOR 6
#define URING_API_KERNEL_VERSION_SENDMSG_ZC_MINOR 0
#define URING_API_KERNEL_VERSION_SENDMSG_ZC_PATCH 0

#endif