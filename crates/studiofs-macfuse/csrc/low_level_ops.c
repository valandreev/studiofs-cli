#include <fuse_lowlevel.h>
#include <stdint.h>
#include <string.h>

static const struct fuse_lowlevel_ops STUDIOFS_EMPTY_LOW_LEVEL_OPS = {0};

extern void studiofs_macfuse_lookup(fuse_req_t req, fuse_ino_t parent, const char *name);
extern void studiofs_macfuse_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
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
extern void studiofs_macfuse_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void studiofs_macfuse_readdir(fuse_req_t req,
                                     fuse_ino_t ino,
                                     size_t size,
                                     off_t off,
                                     struct fuse_file_info *fi);
extern void studiofs_macfuse_readlink(fuse_req_t req, fuse_ino_t ino);
extern void studiofs_macfuse_statfs(fuse_req_t req, fuse_ino_t ino);

static const struct fuse_lowlevel_ops STUDIOFS_READ_ONLY_LOW_LEVEL_OPS = {
    .lookup = studiofs_macfuse_lookup,
    .getattr = studiofs_macfuse_getattr,
    .open = studiofs_macfuse_open,
    .read = studiofs_macfuse_read,
    .release = studiofs_macfuse_release,
    .readdir = studiofs_macfuse_readdir,
    .readlink = studiofs_macfuse_readlink,
    .statfs = studiofs_macfuse_statfs,
};

// SFS-482 intentionally enables existing-file writeback only. Namespace
// mutation callbacks (create, mkdir, unlink, rmdir, rename) remain out of this
// table until the follow-up bridge phase wires those semantics explicitly.
static const struct fuse_lowlevel_ops STUDIOFS_LOW_LEVEL_OPS = {
    .lookup = studiofs_macfuse_lookup,
    .getattr = studiofs_macfuse_getattr,
    .setattr = studiofs_macfuse_setattr,
    .open = studiofs_macfuse_open,
    .read = studiofs_macfuse_read,
    .write = studiofs_macfuse_write,
    .flush = studiofs_macfuse_flush,
    .fsync = studiofs_macfuse_fsync,
    .release = studiofs_macfuse_release,
    .readdir = studiofs_macfuse_readdir,
    .readlink = studiofs_macfuse_readlink,
    .statfs = studiofs_macfuse_statfs,
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

int studiofs_macfuse_setattr_to_set_has_size(int to_set) {
    return (to_set & FUSE_SET_ATTR_SIZE) != 0;
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
