#include <errno.h> /* IWYU pragma: keep */
#include <stddef.h>
#include <fuse_lowlevel.h>

static const struct fuse_lowlevel_ops STUDIOFS_EMPTY_LOW_LEVEL_OPS = {0};
static void studiofs_linux_fuse_init(void *userdata, struct fuse_conn_info *conn);
extern uint32_t studiofs_linux_fuse_connection_max_background(void *userdata);
extern uint32_t studiofs_linux_fuse_connection_congestion_threshold(void *userdata);
extern int studiofs_linux_fuse_connection_wants_async_dio(void *userdata);
extern int studiofs_linux_fuse_connection_wants_writeback_cache(void *userdata);
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
extern void studiofs_linux_fuse_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_statfs(fuse_req_t req, fuse_ino_t ino);
extern void studiofs_linux_fuse_access(fuse_req_t req, fuse_ino_t ino, int mask);
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
    .releasedir = studiofs_linux_fuse_releasedir,
    .statfs = studiofs_linux_fuse_statfs,
    .access = studiofs_linux_fuse_access,
    .setxattr = studiofs_linux_fuse_setxattr,
    .getxattr = studiofs_linux_fuse_getxattr,
    .listxattr = studiofs_linux_fuse_listxattr,
    .removexattr = studiofs_linux_fuse_removexattr,
};

static void studiofs_linux_fuse_init(void *userdata, struct fuse_conn_info *conn) {
    if (conn == NULL) {
        return;
    }
    conn->max_background = studiofs_linux_fuse_connection_max_background(userdata);
    conn->congestion_threshold = studiofs_linux_fuse_connection_congestion_threshold(userdata);
    if (studiofs_linux_fuse_connection_wants_async_dio(userdata)) {
        (void)fuse_set_feature_flag(conn, FUSE_CAP_ASYNC_DIO);
    }
    if (studiofs_linux_fuse_connection_wants_writeback_cache(userdata)) {
        (void)fuse_set_feature_flag(conn, FUSE_CAP_WRITEBACK_CACHE);
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

int studiofs_linux_fuse_session_loop_mt(struct fuse_session *session,
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

int studiofs_linux_fuse_notify_inval_inode(struct fuse_session *session,
                                           uint64_t ino,
                                           off_t offset,
                                           off_t len) {
    return fuse_lowlevel_notify_inval_inode(session, ino, offset, len);
}

void studiofs_linux_fuse_dir_file_info_init(struct fuse_file_info *info, uint64_t fh) {
    info->fh = fh;
    info->keep_cache = 1;
    info->cache_readdir = 1;
}

void studiofs_linux_fuse_file_info_init(struct fuse_file_info *info,
                                        uint64_t fh,
                                        int direct_io,
                                        int keep_cache,
                                        int parallel_direct_writes) {
    info->fh = fh;
    info->direct_io = direct_io != 0;
    info->keep_cache = keep_cache != 0;
    info->parallel_direct_writes = parallel_direct_writes != 0;
}

uint64_t studiofs_linux_fuse_file_info_fh(const struct fuse_file_info *info) {
    return info->fh;
}

int studiofs_linux_fuse_file_info_flags(const struct fuse_file_info *info) {
    return info->flags;
}

int studiofs_linux_fuse_low_level_ops_has_metadata_callbacks(const struct fuse_lowlevel_ops *ops) {
    return ops->lookup && ops->getattr && ops->setattr && ops->opendir &&
           ops->readdir && ops->releasedir && ops->statfs && ops->access;
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

int studiofs_linux_fuse_loop_config_uses_loop_config_api(void) {
    return FUSE_USE_VERSION >= 32;
}

int studiofs_linux_fuse_conn_wants_writeback_cache_after_set(uint64_t capable_ext) {
    struct fuse_conn_info conn = {0};
    conn.capable_ext = capable_ext;
    return fuse_set_feature_flag(&conn, FUSE_CAP_WRITEBACK_CACHE) &&
           fuse_get_feature_flag(&conn, FUSE_CAP_WRITEBACK_CACHE);
}

int studiofs_linux_fuse_conn_wants_async_dio_after_set(uint64_t capable_ext) {
    struct fuse_conn_info conn = {0};
    conn.capable_ext = capable_ext;
    return fuse_set_feature_flag(&conn, FUSE_CAP_ASYNC_DIO) &&
           fuse_get_feature_flag(&conn, FUSE_CAP_ASYNC_DIO);
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
    return info.fh == 7 && info.direct_io == 1 && info.keep_cache == 0 &&
           info.parallel_direct_writes == 0;
}

int studiofs_linux_fuse_file_info_flags_after_init(int direct_io,
                                                   int keep_cache,
                                                   int parallel_direct_writes) {
    struct fuse_file_info info = {0};
    studiofs_linux_fuse_file_info_init(&info, 7, direct_io, keep_cache, parallel_direct_writes);
    return (info.direct_io ? 1 : 0) | (info.keep_cache ? 2 : 0) |
           (info.parallel_direct_writes ? 4 : 0);
}
