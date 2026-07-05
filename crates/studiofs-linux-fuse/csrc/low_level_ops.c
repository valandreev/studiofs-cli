#include <stddef.h>
#include <string.h>
#include <fuse_lowlevel.h>

static const struct fuse_lowlevel_ops STUDIOFS_EMPTY_LOW_LEVEL_OPS = {0};
extern void studiofs_linux_fuse_destroy(void *userdata);
extern void studiofs_linux_fuse_lookup(fuse_req_t req, fuse_ino_t parent, const char *name);
extern void studiofs_linux_fuse_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_create(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_statfs(fuse_req_t req, fuse_ino_t ino);
extern void studiofs_linux_fuse_access(fuse_req_t req, fuse_ino_t ino, int mask);

static const struct fuse_lowlevel_ops STUDIOFS_LOW_LEVEL_OPS = {
    .destroy = studiofs_linux_fuse_destroy,
    .lookup = studiofs_linux_fuse_lookup,
    .getattr = studiofs_linux_fuse_getattr,
    .setattr = studiofs_linux_fuse_setattr,
    .create = studiofs_linux_fuse_create,
    .open = studiofs_linux_fuse_open,
    .read = studiofs_linux_fuse_read,
    .write = studiofs_linux_fuse_write,
    .flush = studiofs_linux_fuse_flush,
    .fsync = studiofs_linux_fuse_fsync,
    .release = studiofs_linux_fuse_release,
    .opendir = studiofs_linux_fuse_opendir,
    .readdir = studiofs_linux_fuse_readdir,
    .releasedir = studiofs_linux_fuse_releasedir,
    .statfs = studiofs_linux_fuse_statfs,
    .access = studiofs_linux_fuse_access,
};

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

void studiofs_linux_fuse_dir_file_info_init(struct fuse_file_info *info, uint64_t fh) {
    memset(info, 0, sizeof(*info));
    info->fh = fh;
    info->keep_cache = 1;
    info->cache_readdir = 1;
}

void studiofs_linux_fuse_file_info_init(struct fuse_file_info *info, uint64_t fh, int direct_io) {
    memset(info, 0, sizeof(*info));
    info->fh = fh;
    info->direct_io = direct_io ? 1 : 0;
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

int studiofs_linux_fuse_low_level_ops_has_regular_file_callbacks(const struct fuse_lowlevel_ops *ops) {
    return ops->create && ops->open && ops->read && ops->write &&
           ops->flush && ops->fsync && ops->release;
}

int studiofs_linux_fuse_file_info_direct_io_after_init(void) {
    struct fuse_file_info info;
    studiofs_linux_fuse_file_info_init(&info, 7, 1);
    return info.fh == 7 && info.direct_io == 1 && info.keep_cache == 0;
}
