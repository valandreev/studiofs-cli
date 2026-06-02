#include <fuse_lowlevel.h>

static const struct fuse_lowlevel_ops STUDIOFS_EMPTY_LOW_LEVEL_OPS = {0};

const struct fuse_lowlevel_ops *studiofs_macfuse_empty_low_level_ops(void) {
    return &STUDIOFS_EMPTY_LOW_LEVEL_OPS;
}

size_t studiofs_macfuse_low_level_ops_size(void) {
    return sizeof(struct fuse_lowlevel_ops);
}
