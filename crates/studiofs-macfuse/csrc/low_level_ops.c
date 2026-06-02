#include <fuse_lowlevel.h>
#include <stdint.h>
#include <string.h>

static const struct fuse_lowlevel_ops STUDIOFS_EMPTY_LOW_LEVEL_OPS = {0};

const struct fuse_lowlevel_ops *studiofs_macfuse_empty_low_level_ops(void) {
    return &STUDIOFS_EMPTY_LOW_LEVEL_OPS;
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
