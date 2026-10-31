#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <errno.h> /* IWYU pragma: keep */
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/uio.h>
#include <unistd.h>
#include <fuse_lowlevel.h>

#define STUDIOFS_LINUX_FUSE_MAX_READ_SIZE (4U * 1024U * 1024U)

/*
 * FUSE_DIRECT_IO_ALLOW_MMAP (kernel UAPI bit 36, flags2 bit 4) lets a
 * FOPEN_DIRECT_IO file take MAP_SHARED mappings. libfuse exposes it as
 * FUSE_CAP_DIRECT_IO_ALLOW_MMAP only since 3.17; older libfuse drops unknown
 * capability bits when it builds the INIT reply.
 */
#if defined(FUSE_CAP_DIRECT_IO_ALLOW_MMAP)
#define STUDIOFS_DIRECT_IO_MMAP_NATIVE 1
#elif FUSE_VERSION >= FUSE_MAKE_VERSION(3, 13)
#define STUDIOFS_DIRECT_IO_MMAP_INIT_PATCH 1
#endif

static const struct fuse_lowlevel_ops STUDIOFS_EMPTY_LOW_LEVEL_OPS = {0};
static void studiofs_linux_fuse_init(void *userdata, struct fuse_conn_info *conn);
static int studiofs_linux_fuse_set_feature_flag_compat(struct fuse_conn_info *conn,
                                                       uint64_t flag);
static int studiofs_linux_fuse_get_feature_flag_compat(struct fuse_conn_info *conn,
                                                       uint64_t flag);
static uint64_t studiofs_linux_fuse_capable_features(const struct fuse_conn_info *conn);
static uint64_t studiofs_linux_fuse_enabled_features(const struct fuse_conn_info *conn);
static void studiofs_linux_fuse_set_capable_features(struct fuse_conn_info *conn,
                                                     uint64_t features);
extern uint32_t studiofs_linux_fuse_connection_max_background(void *userdata);
extern uint32_t studiofs_linux_fuse_connection_congestion_threshold(void *userdata);
extern int studiofs_linux_fuse_connection_wants_async_dio(void *userdata);
extern int studiofs_linux_fuse_connection_wants_writeback_cache(void *userdata);
extern int studiofs_linux_fuse_connection_wants_direct_io_mmap(void *userdata);
extern void studiofs_linux_fuse_record_connection(void *userdata,
                                                  uint32_t requested_max_read,
                                                  uint32_t requested_max_readahead,
                                                  uint32_t effective_max_read,
                                                  uint32_t effective_max_readahead,
                                                  uint32_t effective_max_write,
                                                  uint32_t effective_max_background,
                                                  uint32_t effective_congestion_threshold,
                                                  uint64_t capable_features,
                                                  uint64_t enabled_features);
extern void studiofs_linux_fuse_destroy(void *userdata);
extern void studiofs_linux_fuse_forget(fuse_req_t req, fuse_ino_t ino, uint64_t nlookup);
extern void studiofs_linux_fuse_lookup(fuse_req_t req, fuse_ino_t parent, const char *name);
extern void studiofs_linux_fuse_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_create(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode);
extern void studiofs_linux_fuse_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev);
extern void studiofs_linux_fuse_symlink(fuse_req_t req, const char *link, fuse_ino_t parent, const char *name);
extern void studiofs_linux_fuse_unlink(fuse_req_t req, fuse_ino_t parent, const char *name);
extern void studiofs_linux_fuse_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name);
extern void studiofs_linux_fuse_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname, unsigned int flags);
extern void studiofs_linux_fuse_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char *newname);
extern void studiofs_linux_fuse_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_readlink(fuse_req_t req, fuse_ino_t ino);
extern void studiofs_linux_fuse_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_readdirplus(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_statfs(fuse_req_t req, fuse_ino_t ino);
extern void studiofs_linux_fuse_setxattr(fuse_req_t req, fuse_ino_t ino, const char *name, const char *value, size_t size, int flags);
extern void studiofs_linux_fuse_getxattr(fuse_req_t req, fuse_ino_t ino, const char *name, size_t size);
extern void studiofs_linux_fuse_listxattr(fuse_req_t req, fuse_ino_t ino, size_t size);
extern void studiofs_linux_fuse_removexattr(fuse_req_t req, fuse_ino_t ino, const char *name);

static const struct fuse_lowlevel_ops STUDIOFS_LOW_LEVEL_OPS = {
    .init = studiofs_linux_fuse_init,
    .destroy = studiofs_linux_fuse_destroy,
    .forget = studiofs_linux_fuse_forget,
    .lookup = studiofs_linux_fuse_lookup,
    .getattr = studiofs_linux_fuse_getattr,
    .setattr = studiofs_linux_fuse_setattr,
    .create = studiofs_linux_fuse_create,
    .mkdir = studiofs_linux_fuse_mkdir,
    .mknod = studiofs_linux_fuse_mknod,
    .symlink = studiofs_linux_fuse_symlink,
    .unlink = studiofs_linux_fuse_unlink,
    .rmdir = studiofs_linux_fuse_rmdir,
    .rename = studiofs_linux_fuse_rename,
    .link = studiofs_linux_fuse_link,
    .open = studiofs_linux_fuse_open,
    .read = studiofs_linux_fuse_read,
    .readlink = studiofs_linux_fuse_readlink,
    .write = studiofs_linux_fuse_write,
    .flush = studiofs_linux_fuse_flush,
    .fsync = studiofs_linux_fuse_fsync,
    .release = studiofs_linux_fuse_release,
    .opendir = studiofs_linux_fuse_opendir,
    .readdir = studiofs_linux_fuse_readdir,
    .readdirplus = studiofs_linux_fuse_readdirplus,
    .releasedir = studiofs_linux_fuse_releasedir,
    .statfs = studiofs_linux_fuse_statfs,
    .setxattr = studiofs_linux_fuse_setxattr,
    .getxattr = studiofs_linux_fuse_getxattr,
    .listxattr = studiofs_linux_fuse_listxattr,
    .removexattr = studiofs_linux_fuse_removexattr,
};

static int studiofs_linux_fuse_set_feature_flag_compat(struct fuse_conn_info *conn,
                                                       uint64_t flag) {
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 17)
    return fuse_set_feature_flag(conn, flag);
#else
    if (flag > UINT32_MAX || (conn->capable & (uint32_t)flag) == 0) {
        return 0;
    }
    conn->want |= (uint32_t)flag;
    return 1;
#endif
}

static int studiofs_linux_fuse_get_feature_flag_compat(struct fuse_conn_info *conn,
                                                       uint64_t flag) {
    return (studiofs_linux_fuse_enabled_features(conn) & flag) != 0;
}

static uint64_t studiofs_linux_fuse_capable_features(const struct fuse_conn_info *conn) {
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 17)
    return conn->capable_ext;
#else
    return conn->capable;
#endif
}

static uint64_t studiofs_linux_fuse_enabled_features(const struct fuse_conn_info *conn) {
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 17)
    return conn->want_ext;
#else
    return conn->want;
#endif
}

static void studiofs_linux_fuse_set_capable_features(struct fuse_conn_info *conn,
                                                     uint64_t features) {
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 17)
    conn->capable_ext = features;
#else
    conn->capable = (uint32_t)features;
#endif
}

static void studiofs_linux_fuse_init(void *userdata, struct fuse_conn_info *conn) {
    if (conn == NULL) {
        return;
    }
    const uint32_t requested_max_read = conn->max_read;
    conn->max_read = requested_max_read == 0
                         ? STUDIOFS_LINUX_FUSE_MAX_READ_SIZE
                         : (requested_max_read < STUDIOFS_LINUX_FUSE_MAX_READ_SIZE
                                ? requested_max_read
                                : STUDIOFS_LINUX_FUSE_MAX_READ_SIZE);
    if (conn->max_readahead > STUDIOFS_LINUX_FUSE_MAX_READ_SIZE) {
        conn->max_readahead = STUDIOFS_LINUX_FUSE_MAX_READ_SIZE;
    }
    conn->max_background = studiofs_linux_fuse_connection_max_background(userdata);
    conn->congestion_threshold = studiofs_linux_fuse_connection_congestion_threshold(userdata);
    if (studiofs_linux_fuse_connection_wants_async_dio(userdata)) {
        (void)studiofs_linux_fuse_set_feature_flag_compat(conn, FUSE_CAP_ASYNC_DIO);
    }
    if (studiofs_linux_fuse_connection_wants_writeback_cache(userdata)) {
        (void)studiofs_linux_fuse_set_feature_flag_compat(conn, FUSE_CAP_WRITEBACK_CACHE);
    }
#if defined(STUDIOFS_DIRECT_IO_MMAP_NATIVE)
    if (studiofs_linux_fuse_connection_wants_direct_io_mmap(userdata)) {
        (void)studiofs_linux_fuse_set_feature_flag_compat(conn, FUSE_CAP_DIRECT_IO_ALLOW_MMAP);
    }
#endif
    if (userdata != NULL) {
        studiofs_linux_fuse_record_connection(userdata,
                                              STUDIOFS_LINUX_FUSE_MAX_READ_SIZE,
                                              STUDIOFS_LINUX_FUSE_MAX_READ_SIZE,
                                              conn->max_read,
                                              conn->max_readahead,
                                              conn->max_write,
                                              conn->max_background,
                                              conn->congestion_threshold,
                                              studiofs_linux_fuse_capable_features(conn),
                                              studiofs_linux_fuse_enabled_features(conn));
    }
}

const struct fuse_lowlevel_ops *studiofs_linux_fuse_empty_low_level_ops(void) {
    return &STUDIOFS_EMPTY_LOW_LEVEL_OPS;
}

const struct fuse_lowlevel_ops *studiofs_linux_fuse_low_level_ops(void) {
    return &STUDIOFS_LOW_LEVEL_OPS;
}

size_t studiofs_linux_fuse_low_level_ops_size(void) {
    return sizeof(struct fuse_lowlevel_ops);
}

struct fuse_session *studiofs_linux_fuse_session_new(struct fuse_args *args,
                                                     const struct fuse_lowlevel_ops *ops,
                                                     size_t ops_size,
                                                     void *userdata) {
    return fuse_session_new(args, ops, ops_size, userdata);
}

#if defined(STUDIOFS_DIRECT_IO_MMAP_INIT_PATCH)
/*
 * Pre-3.17 libfuse cannot request FUSE_DIRECT_IO_ALLOW_MMAP. A pass-through
 * custom IO on the already mounted /dev/fuse fd observes the kernel INIT
 * request and, when the kernel offered the flag, adds it to libfuse's INIT
 * reply. Every other request and reply is forwarded unchanged. Only the wire
 * layout prefix that this patch touches is mirrored from <linux/fuse.h>.
 */
#define STUDIOFS_FUSE_OPCODE_INIT 26U
#define STUDIOFS_FUSE_INIT_EXT (1U << 30)
#define STUDIOFS_FUSE_DIRECT_IO_ALLOW_MMAP_FLAGS2 (1U << (36 - 32))
#define STUDIOFS_INIT_PATCH_SLOTS 8

struct studiofs_fuse_in_header {
    uint32_t len;
    uint32_t opcode;
    uint64_t unique;
    uint64_t nodeid;
    uint32_t uid;
    uint32_t gid;
    uint32_t pid;
    uint16_t total_extlen;
    uint16_t padding;
};

struct studiofs_fuse_init_in_prefix {
    uint32_t major;
    uint32_t minor;
    uint32_t max_readahead;
    uint32_t flags;
    uint32_t flags2;
};

struct studiofs_fuse_out_header {
    uint32_t len;
    int32_t error;
    uint64_t unique;
};

struct studiofs_fuse_init_out_prefix {
    uint32_t major;
    uint32_t minor;
    uint32_t max_readahead;
    uint32_t flags;
    uint16_t max_background;
    uint16_t congestion_threshold;
    uint32_t max_write;
    uint32_t time_gran;
    uint16_t max_pages;
    uint16_t map_alignment;
    uint32_t flags2;
};

_Static_assert(sizeof(struct studiofs_fuse_in_header) == 40, "fuse_in_header layout");
_Static_assert(offsetof(struct studiofs_fuse_init_in_prefix, flags2) == 16,
               "fuse_init_in.flags2 offset");
_Static_assert(sizeof(struct studiofs_fuse_out_header) == 16, "fuse_out_header layout");
_Static_assert(offsetof(struct studiofs_fuse_init_out_prefix, flags2) == 32,
               "fuse_init_out.flags2 offset");

/* An INIT whose reply has not been written yet, keyed by connection fd. */
struct studiofs_init_patch_slot {
    int fd;
    uint64_t unique;
};

static pthread_mutex_t studiofs_init_patch_lock = PTHREAD_MUTEX_INITIALIZER;
static struct studiofs_init_patch_slot studiofs_init_patch_slots[STUDIOFS_INIT_PATCH_SLOTS];
static int studiofs_init_patch_pending;

static void studiofs_init_patch_observe_request(int fd, const void *buf, ssize_t len) {
    if (len < (ssize_t)(sizeof(struct studiofs_fuse_in_header) +
                        sizeof(struct studiofs_fuse_init_in_prefix))) {
        return;
    }
    const struct studiofs_fuse_in_header *in = buf;
    if (in->opcode != STUDIOFS_FUSE_OPCODE_INIT) {
        return;
    }
    const struct studiofs_fuse_init_in_prefix *init = (const void *)(in + 1);
    if ((init->flags & STUDIOFS_FUSE_INIT_EXT) == 0 ||
        (init->flags2 & STUDIOFS_FUSE_DIRECT_IO_ALLOW_MMAP_FLAGS2) == 0) {
        return;
    }
    pthread_mutex_lock(&studiofs_init_patch_lock);
    int free_slot = -1;
    for (int i = 0; i < STUDIOFS_INIT_PATCH_SLOTS; i++) {
        if (studiofs_init_patch_slots[i].fd == fd + 1) {
            free_slot = i;
            break;
        }
        if (free_slot < 0 && studiofs_init_patch_slots[i].fd == 0) {
            free_slot = i;
        }
    }
    if (free_slot >= 0) {
        /* fd + 1 keeps zero as the empty-slot marker. */
        if (studiofs_init_patch_slots[free_slot].fd == 0) {
            __atomic_add_fetch(&studiofs_init_patch_pending, 1, __ATOMIC_RELEASE);
        }
        studiofs_init_patch_slots[free_slot].fd = fd + 1;
        studiofs_init_patch_slots[free_slot].unique = in->unique;
    }
    pthread_mutex_unlock(&studiofs_init_patch_lock);
}

static void studiofs_init_patch_reply(int fd, struct iovec *iov, int count) {
    if (__atomic_load_n(&studiofs_init_patch_pending, __ATOMIC_ACQUIRE) == 0 || count < 1 ||
        iov[0].iov_len < sizeof(struct studiofs_fuse_out_header)) {
        return;
    }
    const struct studiofs_fuse_out_header *out = iov[0].iov_base;
    if (out->unique == 0) {
        /* Kernel notifications carry no request and never answer INIT. */
        return;
    }
    pthread_mutex_lock(&studiofs_init_patch_lock);
    for (int i = 0; i < STUDIOFS_INIT_PATCH_SLOTS; i++) {
        if (studiofs_init_patch_slots[i].fd != fd + 1) {
            continue;
        }
        /*
         * The kernel dispatches nothing else until INIT is answered, so the
         * first reply on this fd settles the slot even when it does not match.
         */
        studiofs_init_patch_slots[i].fd = 0;
        __atomic_sub_fetch(&studiofs_init_patch_pending, 1, __ATOMIC_RELEASE);
        /* libfuse sends the INIT reply as header + fuse_init_out. */
        if (studiofs_init_patch_slots[i].unique == out->unique && out->error == 0 &&
            count >= 2 &&
            iov[1].iov_len >= sizeof(struct studiofs_fuse_init_out_prefix)) {
            struct studiofs_fuse_init_out_prefix *init = iov[1].iov_base;
            if ((init->flags & STUDIOFS_FUSE_INIT_EXT) != 0) {
                init->flags2 |= STUDIOFS_FUSE_DIRECT_IO_ALLOW_MMAP_FLAGS2;
            }
        }
        break;
    }
    pthread_mutex_unlock(&studiofs_init_patch_lock);
}

static ssize_t studiofs_init_patch_read(int fd, void *buf, size_t buf_len, void *userdata) {
    (void)userdata;
    ssize_t res = read(fd, buf, buf_len);
    if (res > 0) {
        int saved_errno = errno;
        studiofs_init_patch_observe_request(fd, buf, res);
        errno = saved_errno;
    }
    return res;
}

static ssize_t studiofs_init_patch_writev(int fd, struct iovec *iov, int count, void *userdata) {
    (void)userdata;
    studiofs_init_patch_reply(fd, iov, count);
    return writev(fd, iov, count);
}

static ssize_t studiofs_init_patch_splice(int fdin, off_t *offin, int fdout, off_t *offout,
                                          size_t len, unsigned int flags, void *userdata) {
    (void)userdata;
    return splice(fdin, offin, fdout, offout, len, flags);
}

static const struct fuse_custom_io STUDIOFS_INIT_PATCH_IO = {
    .writev = studiofs_init_patch_writev,
    .read = studiofs_init_patch_read,
    /* Keep libfuse's splice capabilities unchanged. */
    .splice_receive = studiofs_init_patch_splice,
    .splice_send = studiofs_init_patch_splice,
};
#endif

/* How this build requests FUSE_DIRECT_IO_ALLOW_MMAP: 2 native, 1 patch, 0 none. */
int studiofs_linux_fuse_direct_io_mmap_strategy(void) {
#if defined(STUDIOFS_DIRECT_IO_MMAP_NATIVE)
    return 2;
#elif defined(STUDIOFS_DIRECT_IO_MMAP_INIT_PATCH)
    return 1;
#else
    return 0;
#endif
}

/*
 * Arrange for FUSE_DIRECT_IO_ALLOW_MMAP on a mounted session before its loop
 * runs. Returns studiofs_linux_fuse_direct_io_mmap_strategy() on success and a
 * negative errno when installing the INIT reply patch failed.
 */
int studiofs_linux_fuse_session_enable_direct_io_mmap(struct fuse_session *session) {
#if defined(STUDIOFS_DIRECT_IO_MMAP_INIT_PATCH)
    int fd = fuse_session_fd(session);
    if (fd < 0) {
        return -EBADF;
    }
    int result = fuse_session_custom_io(session, &STUDIOFS_INIT_PATCH_IO, fd);
    return result == 0 ? 1 : (result < 0 ? result : -EIO);
#else
    (void)session;
    return studiofs_linux_fuse_direct_io_mmap_strategy();
#endif
}

/* Test hook: the INIT reply flags2 after the patch, or -1 without the patch. */
int64_t studiofs_linux_fuse_init_patch_reply_flags2_for_test(int fd,
                                                             uint32_t kernel_flags,
                                                             uint32_t kernel_flags2,
                                                             uint64_t request_unique,
                                                             uint32_t request_opcode,
                                                             uint64_t reply_unique,
                                                             int32_t reply_error,
                                                             uint32_t reply_flags,
                                                             uint32_t reply_flags2) {
#if defined(STUDIOFS_DIRECT_IO_MMAP_INIT_PATCH)
    struct {
        struct studiofs_fuse_in_header header;
        struct studiofs_fuse_init_in_prefix init;
    } request = {0};
    request.header.len = sizeof(request);
    request.header.opcode = request_opcode;
    request.header.unique = request_unique;
    request.init.major = 7;
    request.init.minor = 39;
    request.init.flags = kernel_flags;
    request.init.flags2 = kernel_flags2;
    studiofs_init_patch_observe_request(fd, &request, sizeof(request));

    struct studiofs_fuse_out_header header = {
        .len = sizeof(header),
        .error = reply_error,
        .unique = reply_unique,
    };
    struct {
        struct studiofs_fuse_init_out_prefix init;
        uint32_t unused[7];
    } reply = {0};
    reply.init.flags = reply_flags;
    reply.init.flags2 = reply_flags2;
    struct iovec iov[2] = {
        {.iov_base = &header, .iov_len = sizeof(header)},
        {.iov_base = &reply, .iov_len = sizeof(reply)},
    };
    studiofs_init_patch_reply(fd, iov, 2);
    return reply.init.flags2;
#else
    (void)fd;
    (void)kernel_flags;
    (void)kernel_flags2;
    (void)request_unique;
    (void)request_opcode;
    (void)reply_unique;
    (void)reply_error;
    (void)reply_flags;
    (void)reply_flags2;
    return -1;
#endif
}

/*
 * libfuse's exit handler only sets the session's exited flag. The multi-thread
 * loop notices it when the signal interrupts its own sem_wait(), and libfuse
 * workers block exit signals. A process-directed signal usually lands on the
 * main thread instead (for example the tray event loop), leaving the loop
 * asleep. The wrapper chains to libfuse's handler and then re-sends the signal
 * to the loop thread so its sem_wait() returns EINTR.
 */
static void (*studiofs_linux_fuse_exit_handlers[NSIG])(int);
static pthread_t studiofs_linux_fuse_loop_thread;
static volatile sig_atomic_t studiofs_linux_fuse_loop_thread_active;

static void studiofs_linux_fuse_forward_exit_signal(int sig) {
    int saved_errno = errno;
    void (*handler)(int) = studiofs_linux_fuse_exit_handlers[sig];
    if (handler != NULL) {
        handler(sig);
    }
    if (studiofs_linux_fuse_loop_thread_active &&
        !pthread_equal(pthread_self(), studiofs_linux_fuse_loop_thread)) {
        /* pthread_kill() is async-signal-safe. */
        (void)pthread_kill(studiofs_linux_fuse_loop_thread, sig);
    }
    errno = saved_errno;
}

/* Wrap the plain handler installed for `sig`; returns 1 when wrapped. */
int studiofs_linux_fuse_wrap_exit_signal(int sig) {
    struct sigaction current;
    if (sig <= 0 || sig >= NSIG || sigaction(sig, NULL, &current) != 0) {
        return 0;
    }
    if ((current.sa_flags & SA_SIGINFO) != 0 || current.sa_handler == SIG_DFL ||
        current.sa_handler == SIG_IGN ||
        current.sa_handler == studiofs_linux_fuse_forward_exit_signal) {
        return 0;
    }
    studiofs_linux_fuse_exit_handlers[sig] = current.sa_handler;
    struct sigaction wrapped = current;
    wrapped.sa_handler = studiofs_linux_fuse_forward_exit_signal;
    if (sigaction(sig, &wrapped, NULL) != 0) {
        studiofs_linux_fuse_exit_handlers[sig] = NULL;
        return 0;
    }
    return 1;
}

/* Reinstall the wrapped handler so libfuse can recognize and remove it. */
void studiofs_linux_fuse_unwrap_exit_signal(int sig) {
    struct sigaction current;
    if (sig <= 0 || sig >= NSIG || studiofs_linux_fuse_exit_handlers[sig] == NULL ||
        sigaction(sig, NULL, &current) != 0) {
        return;
    }
    if ((current.sa_flags & SA_SIGINFO) == 0 &&
        current.sa_handler == studiofs_linux_fuse_forward_exit_signal) {
        current.sa_handler = studiofs_linux_fuse_exit_handlers[sig];
        (void)sigaction(sig, &current, NULL);
    }
    studiofs_linux_fuse_exit_handlers[sig] = NULL;
}

void studiofs_linux_fuse_enter_loop_thread(void) {
    studiofs_linux_fuse_loop_thread = pthread_self();
    studiofs_linux_fuse_loop_thread_active = 1;
}

void studiofs_linux_fuse_leave_loop_thread(void) {
    studiofs_linux_fuse_loop_thread_active = 0;
}

static int studiofs_linux_fuse_run_session_loop_mt(struct fuse_session *session,
                                                   unsigned int max_threads,
                                                   unsigned int max_idle_threads) {
#if FUSE_USE_VERSION >= FUSE_MAKE_VERSION(3, 12)
    struct fuse_loop_config *config = fuse_loop_cfg_create();
    if (config == NULL) {
        return -ENOMEM;
    }
    fuse_loop_cfg_set_clone_fd(config, 0);
    fuse_loop_cfg_set_idle_threads(config, max_idle_threads);
    fuse_loop_cfg_set_max_threads(config, max_threads);
    int result = fuse_session_loop_mt(session, config);
    fuse_loop_cfg_destroy(config);
    return result;
#else
    /* Pre-3.12 public config has max_idle_threads but no max_threads field. */
    (void)max_threads;
    struct fuse_loop_config config = {
        .clone_fd = 0,
        .max_idle_threads = max_idle_threads,
    };
    return fuse_session_loop_mt(session, &config);
#endif
}

int studiofs_linux_fuse_session_loop_mt(struct fuse_session *session,
                                        unsigned int max_threads,
                                        unsigned int max_idle_threads) {
    studiofs_linux_fuse_enter_loop_thread();
    int result =
        studiofs_linux_fuse_run_session_loop_mt(session, max_threads, max_idle_threads);
    studiofs_linux_fuse_leave_loop_thread();
    return result;
}

int studiofs_linux_fuse_loop_has_worker_limit(void) {
#if FUSE_USE_VERSION >= FUSE_MAKE_VERSION(3, 12)
    return 1;
#else
    return 0;
#endif
}

int studiofs_linux_fuse_notify_inval_inode(struct fuse_session *session,
                                           uint64_t ino,
                                           off_t offset,
                                           off_t len) {
    return fuse_lowlevel_notify_inval_inode(session, ino, offset, len);
}

void studiofs_linux_fuse_dir_file_info_init(struct fuse_file_info *info, uint64_t fh) {
    info->fh = fh;
    info->keep_cache = 1;
    // Directory contents can change on another StudioFS client. The bridge
    // currently invalidates inode data/attributes, not individual FUSE
    // dirents, so a kernel readdir snapshot would retain stale namespace.
    info->cache_readdir = 0;
}

int studiofs_linux_fuse_dir_file_info_cache_readdir_after_init(void) {
    struct fuse_file_info info = {0};
    studiofs_linux_fuse_dir_file_info_init(&info, 7);
    return info.cache_readdir ? 1 : 0;
}

void studiofs_linux_fuse_file_info_init(struct fuse_file_info *info,
                                        uint64_t fh,
                                        int direct_io,
                                        int keep_cache,
                                        int parallel_direct_writes) {
    info->fh = fh;
    info->direct_io = direct_io != 0;
    info->keep_cache = keep_cache != 0;
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 15)
    info->parallel_direct_writes = parallel_direct_writes != 0;
#else
    (void)parallel_direct_writes;
#endif
}

uint64_t studiofs_linux_fuse_file_info_fh(const struct fuse_file_info *info) {
    return info->fh;
}

int studiofs_linux_fuse_file_info_flags(const struct fuse_file_info *info) {
    return info->flags;
}

int studiofs_linux_fuse_low_level_ops_has_metadata_callbacks(const struct fuse_lowlevel_ops *ops) {
    return ops->lookup && ops->getattr && ops->setattr && ops->opendir &&
           ops->readdir && ops->readdirplus && ops->releasedir && ops->statfs &&
           ops->access == NULL;
}

int studiofs_linux_fuse_low_level_ops_has_forget_callback(const struct fuse_lowlevel_ops *ops) {
    return ops->forget != NULL;
}

int studiofs_linux_fuse_init_sets_bounded_background(void *userdata,
                                                    uint32_t max_background,
                                                    uint32_t congestion_threshold) {
    struct fuse_conn_info conn = {0};
    studiofs_linux_fuse_init(userdata, &conn);
    return conn.max_background == max_background &&
           conn.congestion_threshold == congestion_threshold;
}

uint32_t studiofs_linux_fuse_init_effective_max_read(uint32_t offered) {
    struct fuse_conn_info conn = {0};
    conn.max_read = offered;
    studiofs_linux_fuse_init(NULL, &conn);
    return conn.max_read;
}

uint32_t studiofs_linux_fuse_init_effective_max_readahead(uint32_t offered) {
    struct fuse_conn_info conn = {0};
    conn.max_readahead = offered;
    studiofs_linux_fuse_init(NULL, &conn);
    return conn.max_readahead;
}

int studiofs_linux_fuse_loop_config_uses_loop_config_api(void) {
    return FUSE_USE_VERSION >= 32;
}

int studiofs_linux_fuse_conn_wants_writeback_cache_after_set(uint64_t capable_ext) {
    struct fuse_conn_info conn = {0};
    studiofs_linux_fuse_set_capable_features(&conn, capable_ext);
    return studiofs_linux_fuse_set_feature_flag_compat(&conn, FUSE_CAP_WRITEBACK_CACHE) &&
           studiofs_linux_fuse_get_feature_flag_compat(&conn, FUSE_CAP_WRITEBACK_CACHE);
}

int studiofs_linux_fuse_conn_wants_async_dio_after_set(uint64_t capable_ext) {
    struct fuse_conn_info conn = {0};
    studiofs_linux_fuse_set_capable_features(&conn, capable_ext);
    return studiofs_linux_fuse_set_feature_flag_compat(&conn, FUSE_CAP_ASYNC_DIO) &&
           studiofs_linux_fuse_get_feature_flag_compat(&conn, FUSE_CAP_ASYNC_DIO);
}

/* 1 when init requests ALLOW_MMAP natively; -1 when this libfuse has no cap. */
int studiofs_linux_fuse_init_wants_direct_io_allow_mmap(void *userdata) {
#if defined(STUDIOFS_DIRECT_IO_MMAP_NATIVE)
    struct fuse_conn_info conn = {0};
    studiofs_linux_fuse_set_capable_features(&conn, FUSE_CAP_DIRECT_IO_ALLOW_MMAP);
    studiofs_linux_fuse_init(userdata, &conn);
    return studiofs_linux_fuse_get_feature_flag_compat(&conn, FUSE_CAP_DIRECT_IO_ALLOW_MMAP);
#else
    (void)userdata;
    return -1;
#endif
}

int studiofs_linux_fuse_low_level_ops_has_regular_file_callbacks(const struct fuse_lowlevel_ops *ops) {
    return ops->create && ops->open && ops->read && ops->write &&
           ops->flush && ops->fsync && ops->release;
}

int studiofs_linux_fuse_low_level_ops_has_namespace_mutation_callbacks(const struct fuse_lowlevel_ops *ops) {
    return ops->mkdir && ops->mknod && ops->unlink && ops->rmdir && ops->rename;
}

int studiofs_linux_fuse_low_level_ops_has_posix_compat_callbacks(const struct fuse_lowlevel_ops *ops) {
    return ops->symlink && ops->readlink && ops->link &&
           ops->setxattr && ops->getxattr && ops->listxattr && ops->removexattr;
}

int studiofs_linux_fuse_file_info_direct_io_after_init(void) {
    struct fuse_file_info info = {0};
    studiofs_linux_fuse_file_info_init(&info, 7, 1, 0, 0);
    return info.fh == 7 && info.direct_io == 1 && info.keep_cache == 0;
}

struct fuse_file_info *studiofs_linux_fuse_file_info_new_for_test(uint64_t fh) {
    struct fuse_file_info *info = calloc(1, sizeof(*info));
    if (info) {
        info->fh = fh;
    }
    return info;
}

int studiofs_linux_fuse_file_info_flags_after_init(int direct_io,
                                                   int keep_cache,
                                                   int parallel_direct_writes) {
    struct fuse_file_info info = {0};
    studiofs_linux_fuse_file_info_init(&info, 7, direct_io, keep_cache, parallel_direct_writes);
    int flags = (info.direct_io ? 1 : 0) | (info.keep_cache ? 2 : 0);
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 15)
    flags |= info.parallel_direct_writes ? 4 : 0;
#endif
    return flags;
}

int studiofs_linux_fuse_file_info_has_parallel_direct_writes(void) {
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 15)
    return 1;
#else
    return 0;
#endif
}
