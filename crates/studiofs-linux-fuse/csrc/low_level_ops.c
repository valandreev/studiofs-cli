#include <fuse_lowlevel.h>
#include <stddef.h>

static const struct fuse_lowlevel_ops STUDIOFS_EMPTY_LOW_LEVEL_OPS = {0};

const struct fuse_lowlevel_ops *studiofs_linux_fuse_empty_low_level_ops(void) {
    return &STUDIOFS_EMPTY_LOW_LEVEL_OPS;
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
