#!/usr/bin/env python3

from __future__ import with_statement

import os
import sys
import errno
import argparse
import math 
import hashlib 
import json 
import numpy as np

from fuse import FUSE, FuseOSError, Operations
from PyMimircache.cache.lru import LRU
from PyMimircache.cacheReader.requestItem import Req

from KubeCache import KubeCache 

class KubeCacheFS(Operations):
    """ KubeCacheFS is a FS that has highly customizable I/O cache """

    def __init__(self, storage_path, cache_path, config_file):
        self.root = storage_path 
        self.cache_path = cache_path 
        self.kubecache = KubeCache(KubeCacheFS._get_config_from_file(config_file))

    @staticmethod
    def _get_config_from_file(config_file):
        config = {}
        with open(config_file) as f:
            config = json.load(f)
        return config

    # Helper Functions 
    def _full_path(self, partial):
        """ Get the path to the directory we use for persistent storage based on mountpoint path 

            :param partial: path relative to mountpoint 

            :return path: the path to the persistent storage 
        """

        if partial.startswith("/"):
            partial = partial[1:]
        return os.path.join(self.root, partial)


    # Filesystem methods
    # ==================
    def access(self, path, mode):
        full_path = self._full_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        full_path = self._full_path(path)
        return os.chmod(full_path, mode)

    def chown(self, path, uid, gid):
        full_path = self._full_path(path)
        return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):
        full_path = self._full_path(path)
        st = os.lstat(full_path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    def readdir(self, path, fh):
        full_path = self._full_path(path)

        dirents = ['.', '..']
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        for r in dirents:
            yield r

    def readlink(self, path):
        pathname = os.readlink(self._full_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    def mknod(self, path, mode, dev):
        return os.mknod(self._full_path(path), mode, dev)

    def rmdir(self, path):
        full_path = self._full_path(path)
        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        return os.mkdir(self._full_path(path), mode)

    def statfs(self, path):
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def unlink(self, path):
        return os.unlink(self._full_path(path))

    def symlink(self, name, target):
        return os.symlink(target, self._full_path(name))

    def rename(self, old, new):
        return os.rename(self._full_path(old), self._full_path(new))

    def link(self, target, name):
        return os.link(self._full_path(name), self._full_path(target))

    def utimens(self, path, times=None):
        return os.utime(self._full_path(path), times)

    # File methods
    # ============
    def open(self, path, flags):
        full_path = self._full_path(path)
        return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        full_path = self._full_path(path)
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        full_path = self._full_path(path)
        return self.kubecache.read(full_path, length, offset, fh)

    def write(self, path, buf, offset, fh):
        full_path = self._full_path(path)
        return self.kubecache.write(full_path, buf, offset, fh)

    def truncate(self, path, length, fh=None):
        full_path = self._full_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        return os.fsync(fh)

    def release(self, path, fh):
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)

def main(mountpoint, root, cache_path, cache_config_file):
    FUSE(KubeCacheFS(root, cache_path, cache_config_file), 
        mountpoint, 
        nothreads=True, 
        foreground=True, 
        allow_other=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mountpoint", 
        help="The mountpoint of the filesystem.") 
    parser.add_argument("-s", "--storage",
        help="The directory used as persistent storage on a slower device.")
    parser.add_argument("-c", "--cache",
        help="The directory used as a cache on a faster storage device.")
    parser.add_argument("-k", "--kcacheconfig",
        help="The configuration file for KubeCache.")
    args = parser.parse_args()

    main(args.mountpoint, args.storage, args.cache, args.kcacheconfig)
