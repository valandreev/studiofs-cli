#include "fuse_lowlevel_compat.h"
#include <stdint.h>
#include <string.h>

static const struct fuse_lowlevel_ops STUDIOFS_EMPTY_LOW_LEVEL_OPS = {0};

extern void studiofs_macfuse_lookup(fuse_req_t req, fuse_ino_t parent, const char *name);
extern void studiofs_macfuse_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void studiofs_macfuse_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode);
extern void studiofs_macfuse_symlink(fuse_req_t req,
                                     const char *target,
                                     fuse_ino_t parent,
                                     const char *name);
extern void studiofs_macfuse_mknod(fuse_req_t req,
                                   fuse_ino_t parent,
                                   const char *name,
                                   mode_t mode,
                                   dev_t rdev);
extern void studiofs_macfuse_create(fuse_req_t req,
                                    fuse_ino_t parent,
                                    const char *name,
                                    mode_t mode,
                                    struct fuse_file_info *fi);
extern void studiofs_macfuse_unlink(fuse_req_t req, fuse_ino_t parent, const char *name);
extern void studiofs_macfuse_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name);
extern void studiofs_macfuse_rename(fuse_req_t req,
                                    fuse_ino_t parent,
                                    const char *name,
                                    fuse_ino_t newparent,
                                    const char *newname);
extern void studiofs_macfuse_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void studiofs_macfuse_read(fuse_req_t req,
                                  fuse_ino_t ino,
                                  size_t size,
                                  off_t off,
                                  struct fuse_file_info *fi);
extern void studiofs_macfuse_write(fuse_req_t req,
                                   fuse_ino_t ino,
                                   const char *buf,
                                   size_t size,
                                   off_t off,
                                   struct fuse_file_info *fi);
extern void studiofs_macfuse_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void studiofs_macfuse_fsync(fuse_req_t req,
                                   fuse_ino_t ino,
                                   int datasync,
                                   struct fuse_file_info *fi);
extern void studiofs_macfuse_setattr(fuse_req_t req,
                                     fuse_ino_t ino,
                                     struct stat *attr,
                                     int to_set,
                                     struct fuse_file_info *fi);
extern void studiofs_macfuse_setattr_x(fuse_req_t req,
                                       fuse_ino_t ino,
                                       struct setattr_x *attr,
                                       int to_set,
                                       struct fuse_file_info *fi);
extern void studiofs_macfuse_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void studiofs_macfuse_readdir(fuse_req_t req,
                                     fuse_ino_t ino,
                                     size_t size,
                                     off_t off,
                                     struct fuse_file_info *fi);
extern void studiofs_macfuse_readlink(fuse_req_t req, fuse_ino_t ino);
extern void studiofs_macfuse_statfs(fuse_req_t req, fuse_ino_t ino);
extern void studiofs_macfuse_setxattr(fuse_req_t req,
                                      fuse_ino_t ino,
                                      const char *name,
                                      const char *value,
                                      size_t size,
                                      int flags,
                                      uint32_t position);
extern void studiofs_macfuse_getxattr(fuse_req_t req,
                                      fuse_ino_t ino,
                                      const char *name,
                                      size_t size,
                                      uint32_t position);
extern void studiofs_macfuse_listxattr(fuse_req_t req, fuse_ino_t ino, size_t size);
extern void studiofs_macfuse_removexattr(fuse_req_t req, fuse_ino_t ino, const char *name);
extern void studiofs_macfuse_init(void *userdata, struct fuse_conn_info *conn);

static const struct fuse_lowlevel_ops STUDIOFS_READ_ONLY_LOW_LEVEL_OPS = {
    .init = studiofs_macfuse_init,
    .lookup = studiofs_macfuse_lookup,
    .getattr = studiofs_macfuse_getattr,
    .open = studiofs_macfuse_open,
    .read = studiofs_macfuse_read,
    .release = studiofs_macfuse_release,
    .readdir = studiofs_macfuse_readdir,
    .readlink = studiofs_macfuse_readlink,
    .statfs = studiofs_macfuse_statfs,
    .getxattr = studiofs_macfuse_getxattr,
    .listxattr = studiofs_macfuse_listxattr,
};

static const struct fuse_lowlevel_ops STUDIOFS_LOW_LEVEL_OPS = {
    .init = studiofs_macfuse_init,
    .lookup = studiofs_macfuse_lookup,
    .getattr = studiofs_macfuse_getattr,
    .mkdir = studiofs_macfuse_mkdir,
    .symlink = studiofs_macfuse_symlink,
    .mknod = studiofs_macfuse_mknod,
    .create = studiofs_macfuse_create,
    .unlink = studiofs_macfuse_unlink,
    .rmdir = studiofs_macfuse_rmdir,
    .rename = studiofs_macfuse_rename,
    .setattr = studiofs_macfuse_setattr,
    .setattr_x = studiofs_macfuse_setattr_x,
    .open = studiofs_macfuse_open,
    .read = studiofs_macfuse_read,
    .write = studiofs_macfuse_write,
    .flush = studiofs_macfuse_flush,
    .fsync = studiofs_macfuse_fsync,
    .release = studiofs_macfuse_release,
    .readdir = studiofs_macfuse_readdir,
    .readlink = studiofs_macfuse_readlink,
    .statfs = studiofs_macfuse_statfs,
    .setxattr = studiofs_macfuse_setxattr,
    .getxattr = studiofs_macfuse_getxattr,
    .listxattr = studiofs_macfuse_listxattr,
    .removexattr = studiofs_macfuse_removexattr,
};

const struct fuse_lowlevel_ops *studiofs_macfuse_empty_low_level_ops(void) {
    return &STUDIOFS_EMPTY_LOW_LEVEL_OPS;
}

const struct fuse_lowlevel_ops *studiofs_macfuse_read_only_low_level_ops(void) {
    return &STUDIOFS_READ_ONLY_LOW_LEVEL_OPS;
}

const struct fuse_lowlevel_ops *studiofs_macfuse_low_level_ops(void) {
    return &STUDIOFS_LOW_LEVEL_OPS;
}

size_t studiofs_macfuse_low_level_ops_size(void) {
    return sizeof(struct fuse_lowlevel_ops);
}

int studiofs_macfuse_low_level_ops_has_xattr_callbacks(const struct fuse_lowlevel_ops *ops) {
    return ops != NULL && ops->getxattr != NULL && ops->listxattr != NULL &&
           ops->setxattr != NULL && ops->removexattr != NULL;
}

int studiofs_macfuse_low_level_ops_has_read_only_xattr_callbacks(const struct fuse_lowlevel_ops *ops) {
    return ops != NULL && ops->getxattr != NULL && ops->listxattr != NULL &&
           ops->setxattr == NULL && ops->removexattr == NULL;
}

int studiofs_macfuse_low_level_ops_has_init_callback(const struct fuse_lowlevel_ops *ops) {
    return ops != NULL && ops->init != NULL;
}

int studiofs_macfuse_low_level_ops_has_rmdir_callback(const struct fuse_lowlevel_ops *ops) {
    return ops != NULL && ops->rmdir != NULL;
}

int studiofs_macfuse_low_level_ops_has_mknod_callback(const struct fuse_lowlevel_ops *ops) {
    return ops != NULL && ops->mknod != NULL;
}

void studiofs_macfuse_conn_info_configure(struct fuse_conn_info *conn,
                                          unsigned max_write,
                                          unsigned max_readahead) {
    if (conn == NULL) {
        return;
    }
    /* A zero argument means "leave the macFUSE-provided value unchanged". */
    if (max_write > 0) {
        conn->max_write = max_write;
    }
    if (max_readahead > 0) {
        conn->max_readahead = max_readahead;
    }
}

void studiofs_macfuse_file_info_init(struct fuse_file_info *info,
                                     int flags,
                                     uint64_t file_handle,
                                     int direct_io,
                                     int keep_cache) {
    memset(info, 0, sizeof(*info));
    info->flags = flags;
    info->fh = file_handle;
    info->direct_io = direct_io != 0;
    info->keep_cache = keep_cache != 0;
}

int studiofs_macfuse_file_info_direct_io(const struct fuse_file_info *info) {
    return info->direct_io != 0;
}

int studiofs_macfuse_file_info_keep_cache(const struct fuse_file_info *info) {
    return info->keep_cache != 0;
}

int studiofs_macfuse_setattr_size_mask(void) {
    return FUSE_SET_ATTR_SIZE;
}

int studiofs_macfuse_setattr_fh_mask(void) {
#ifdef FUSE_SET_ATTR_FH
    return FUSE_SET_ATTR_FH;
#else
    return 0;
#endif
}

int studiofs_macfuse_setattr_mode_mask(void) {
    return FUSE_SET_ATTR_MODE;
}

int studiofs_macfuse_setattr_uid_mask(void) {
    return FUSE_SET_ATTR_UID;
}

int studiofs_macfuse_setattr_gid_mask(void) {
    return FUSE_SET_ATTR_GID;
}

int studiofs_macfuse_setattr_atime_mask(void) {
    return FUSE_SET_ATTR_ATIME;
}

int studiofs_macfuse_setattr_mtime_mask(void) {
    return FUSE_SET_ATTR_MTIME;
}

int studiofs_macfuse_setattr_atime_now_mask(void) {
    return FUSE_SET_ATTR_ATIME_NOW;
}

int studiofs_macfuse_setattr_mtime_now_mask(void) {
    return FUSE_SET_ATTR_MTIME_NOW;
}

int studiofs_macfuse_setattr_crtime_mask(void) {
#ifdef FUSE_SET_ATTR_CRTIME
    return FUSE_SET_ATTR_CRTIME;
#else
    return 0;
#endif
}

int studiofs_macfuse_setattr_chgtime_mask(void) {
#ifdef FUSE_SET_ATTR_CHGTIME
    return FUSE_SET_ATTR_CHGTIME;
#elif defined(FUSE_SET_ATTR_CTIME)
    return FUSE_SET_ATTR_CTIME;
#else
    return 0;
#endif
}

int studiofs_macfuse_setattr_bkuptime_mask(void) {
#ifdef FUSE_SET_ATTR_BKUPTIME
    return FUSE_SET_ATTR_BKUPTIME;
#else
    return 0;
#endif
}

int studiofs_macfuse_setattr_flags_mask(void) {
#ifdef FUSE_SET_ATTR_FLAGS
    return FUSE_SET_ATTR_FLAGS;
#else
    return 0;
#endif
}
