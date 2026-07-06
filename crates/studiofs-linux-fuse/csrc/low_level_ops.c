#include <stddef.h>
#include <fuse_lowlevel.h>

static const struct fuse_lowlevel_ops STUDIOFS_EMPTY_LOW_LEVEL_OPS = {0};
static void studiofs_linux_fuse_init(void *userdata, struct fuse_conn_info *conn);
extern uint32_t studiofs_linux_fuse_connection_max_background(void *userdata);
extern uint32_t studiofs_linux_fuse_connection_congestion_threshold(void *userdata);
extern void studiofs_linux_fuse_destroy(void *userdata);
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

int studiofs_linux_fuse_session_loop_mt(struct fuse_session *session, int clone_fd) {
    return fuse_session_loop_mt(session, clone_fd);
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

void studiofs_linux_fuse_file_info_init(struct fuse_file_info *info, uint64_t fh) {
    info->fh = fh;
    info->direct_io = 1;
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

int studiofs_linux_fuse_init_sets_bounded_background(void *userdata,
                                                    uint32_t max_background,
                                                    uint32_t congestion_threshold) {
    struct fuse_conn_info conn = {0};
    studiofs_linux_fuse_init(userdata, &conn);
    return conn.max_background == max_background &&
           conn.congestion_threshold == congestion_threshold;
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
    studiofs_linux_fuse_file_info_init(&info, 7);
    return info.fh == 7 && info.direct_io == 1 && info.keep_cache == 0;
}
