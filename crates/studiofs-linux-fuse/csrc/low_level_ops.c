#include <stddef.h>
#include <string.h>
#include <fuse_lowlevel.h>

static const struct fuse_lowlevel_ops STUDIOFS_EMPTY_LOW_LEVEL_OPS = {0};
extern void studiofs_linux_fuse_destroy(void *userdata);
extern void studiofs_linux_fuse_lookup(fuse_req_t req, fuse_ino_t parent, const char *name);
extern void studiofs_linux_fuse_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void studiofs_linux_fuse_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set, struct fuse_file_info *fi);
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

void studiofs_linux_fuse_file_info_init(struct fuse_file_info *info, uint64_t fh) {
    memset(info, 0, sizeof(*info));
    info->fh = fh;
    info->keep_cache = 1;
    info->cache_readdir = 1;
}

uint64_t studiofs_linux_fuse_file_info_fh(const struct fuse_file_info *info) {
    return info->fh;
}

int studiofs_linux_fuse_low_level_ops_has_metadata_callbacks(const struct fuse_lowlevel_ops *ops) {
    return ops->lookup && ops->getattr && ops->setattr && ops->opendir &&
           ops->readdir && ops->releasedir && ops->statfs && ops->access;
}
