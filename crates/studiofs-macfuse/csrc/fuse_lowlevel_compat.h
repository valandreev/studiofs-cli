#pragma once

#if defined(__has_include)
#  if __has_include(<fuse_lowlevel.h>)
#    include <fuse_lowlevel.h>
#    define STUDIOFS_MACFUSE_HAVE_REAL_HEADERS 1
#  endif
#endif

#ifndef STUDIOFS_MACFUSE_HAVE_REAL_HEADERS
#  if defined(STUDIOFS_REAL_MACFUSE_HEADERS_REQUIRED)
#    error "fuse_lowlevel.h not found; install macFUSE headers or configure PKG_CONFIG_PATH for studiofs-macfuse"
#  endif

#  include <stddef.h>
#  include <stdint.h>
#  include <sys/stat.h>
#  include <sys/types.h>
#  include <time.h>

struct fuse_req;
typedef struct fuse_req *fuse_req_t;
typedef uint64_t fuse_ino_t;

struct fuse_conn_info {
    unsigned proto_major;
    unsigned proto_minor;
    unsigned async_read;
    unsigned max_write;
    unsigned max_readahead;
};

struct fuse_file_info {
    int flags;
    unsigned long fh_old;
    int writepage;
    unsigned int direct_io : 1;
    unsigned int keep_cache : 1;
    unsigned int flush : 1;
    unsigned int nonseekable : 1;
    unsigned int flock_release : 1;
    unsigned int padding : 27;
    uint64_t fh;
    uint64_t lock_owner;
};

struct setattr_x {
    int32_t valid;
    mode_t mode;
    uid_t uid;
    gid_t gid;
    off_t size;
    struct timespec acctime;
    struct timespec modtime;
    struct timespec crtime;
    struct timespec chgtime;
    struct timespec bkuptime;
    uint32_t flags;
};

struct fuse_lowlevel_ops {
    void (*init)(void *userdata, struct fuse_conn_info *conn);
    void (*lookup)(fuse_req_t req, fuse_ino_t parent, const char *name);
    void (*getattr)(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
    void (*mkdir)(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode);
    void (*symlink)(fuse_req_t req, const char *target, fuse_ino_t parent, const char *name);
    void (*mknod)(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev);
    void (*create)(fuse_req_t req,
                   fuse_ino_t parent,
                   const char *name,
                   mode_t mode,
                   struct fuse_file_info *fi);
    void (*unlink)(fuse_req_t req, fuse_ino_t parent, const char *name);
    void (*rmdir)(fuse_req_t req, fuse_ino_t parent, const char *name);
    void (*rename)(fuse_req_t req,
                   fuse_ino_t parent,
                   const char *name,
                   fuse_ino_t newparent,
                   const char *newname);
    void (*setattr)(fuse_req_t req,
                    fuse_ino_t ino,
                    struct stat *attr,
                    int to_set,
                    struct fuse_file_info *fi);
    void (*setattr_x)(fuse_req_t req,
                      fuse_ino_t ino,
                      struct setattr_x *attr,
                      int to_set,
                      struct fuse_file_info *fi);
    void (*open)(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
    void (*read)(fuse_req_t req,
                 fuse_ino_t ino,
                 size_t size,
                 off_t off,
                 struct fuse_file_info *fi);
    void (*write)(fuse_req_t req,
                  fuse_ino_t ino,
                  const char *buf,
                  size_t size,
                  off_t off,
                  struct fuse_file_info *fi);
    void (*flush)(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
    void (*fsync)(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi);
    void (*release)(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
    void (*opendir)(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
    void (*readdir)(fuse_req_t req,
                    fuse_ino_t ino,
                    size_t size,
                    off_t off,
                    struct fuse_file_info *fi);
    void (*releasedir)(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
    void (*readlink)(fuse_req_t req, fuse_ino_t ino);
    void (*statfs)(fuse_req_t req, fuse_ino_t ino);
    void (*setxattr)(fuse_req_t req,
                     fuse_ino_t ino,
                     const char *name,
                     const char *value,
                     size_t size,
                     int flags,
                     uint32_t position);
    void (*getxattr)(fuse_req_t req,
                     fuse_ino_t ino,
                     const char *name,
                     size_t size,
                     uint32_t position);
    void (*listxattr)(fuse_req_t req, fuse_ino_t ino, size_t size);
    void (*removexattr)(fuse_req_t req, fuse_ino_t ino, const char *name);
};

#  ifndef FUSE_SET_ATTR_MODE
#    define FUSE_SET_ATTR_MODE (1 << 0)
#  endif
#  ifndef FUSE_SET_ATTR_UID
#    define FUSE_SET_ATTR_UID (1 << 1)
#  endif
#  ifndef FUSE_SET_ATTR_GID
#    define FUSE_SET_ATTR_GID (1 << 2)
#  endif
#  ifndef FUSE_SET_ATTR_SIZE
#    define FUSE_SET_ATTR_SIZE (1 << 3)
#  endif
#  ifndef FUSE_SET_ATTR_ATIME
#    define FUSE_SET_ATTR_ATIME (1 << 4)
#  endif
#  ifndef FUSE_SET_ATTR_MTIME
#    define FUSE_SET_ATTR_MTIME (1 << 5)
#  endif
#  ifndef FUSE_SET_ATTR_FH
#    define FUSE_SET_ATTR_FH (1 << 6)
#  endif
#  ifndef FUSE_SET_ATTR_ATIME_NOW
#    define FUSE_SET_ATTR_ATIME_NOW (1 << 7)
#  endif
#  ifndef FUSE_SET_ATTR_MTIME_NOW
#    define FUSE_SET_ATTR_MTIME_NOW (1 << 8)
#  endif
#  ifndef FUSE_SET_ATTR_CRTIME
#    define FUSE_SET_ATTR_CRTIME (1 << 9)
#  endif
#  ifndef FUSE_SET_ATTR_CHGTIME
#    define FUSE_SET_ATTR_CHGTIME (1 << 10)
#  endif
#  ifndef FUSE_SET_ATTR_CTIME
#    define FUSE_SET_ATTR_CTIME FUSE_SET_ATTR_CHGTIME
#  endif
#  ifndef FUSE_SET_ATTR_BKUPTIME
#    define FUSE_SET_ATTR_BKUPTIME (1 << 11)
#  endif
#  ifndef FUSE_SET_ATTR_FLAGS
#    define FUSE_SET_ATTR_FLAGS (1 << 12)
#  endif
#endif
