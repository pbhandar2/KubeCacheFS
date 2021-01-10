#!/usr/bin/env python3

from __future__ import with_statement

import os
import sys
import errno
import argparse
import math 
import hashlib 
import numpy as np

from fuse import FUSE, FuseOSError, Operations
from PyMimircache.cache.lru import LRU
from PyMimircache.cacheReader.requestItem import Req

from KubeCache import KubeCache 

class KubeCacheFS(Operations):
    """ KubeCacheFS is a FS that has highly customizable I/O cache """

    def __init__(self, storage_path, cache_path, cache_config):
        self.storage_path = storage_path 
        self.cache_path = cache_path 
        self.cache = KubeCache()

    # Helper Functions 
    def _full_path(self, partial):
        """ Get the path to the directory we use for persistent storage based on mountpoint path 

            :param partial: path relative to mountpoint 

            :return path: the path to the persistent storage 
        """

        if partial.startswith("/"):
            partial = partial[1:]
        return os.path.join(self.root, partial)

    def _get_pages(self, offset, length):
        """ Get all the relevant pages for given a size request and offset 

            :param offset: the offset at which the read/write begins 
            :param length: the length of the request 
        """

        start_page = math.floor(offset/self.page_size)
        end_page = math.floor((offset+length-1)/self.page_size)
        
        page_array = np.empty(shape=(end_page-start_page+1, 2))
        for page_array_index, page_index in enumerate(range(start_page, end_page+1)):
            page_start_offset = page_index*self.page_size
            page_array[page_array_index] = np.array([page_index,page_start_offset])
        return page_array

    def _get_page_id(self, path, page_index):
        """ Get the id of the page based on the path and page being requested  

            :param path: the path of the file being accessed 
            :param page_index: the index of the page 

            :return page_id: the id of the page which is also the name of file/page in the cache 
        """

        hash_object = hashlib.md5(path.encode())
        hash_val = hash_object.hexdigest()
        return "{}_{}".format(hash_val, page_index)

    def _read_page(self, page_path):
        """ Return the content of the given page 

            :param path: the path to the page 

            :return None
        """
        
        read_fh = os.open(page_path, os.O_RDONLY)
        read_bytes = os.read(read_fh, self.page_size)
        os.close(read_fh)
        return read_bytes

    def _update_page(self, page_path, page_offset, req_offset, buf):
        """ On a write hit, overwrite the page that was hit. 
            
            :param page_path: the path of the page file 
            :param page_offset: the offset of the page
            :param req_offset: the offset of the request 
            :param buf: bytes to be written to the page 

            :return: None
        """

        fh = os.open(cache_path, os.O_WRONLY)

        # check if you need to seek before writing 
        if req_offset>page_offset:
            os.lseek(fh, req_offset-page_offset, os.SEEK_SET)

        os.write(fh, buf)
        os.close(fh) 

    def _flush_page(self, cache_req):
        """ Flush the content of the page to persistent storage. 

            :param cache_req: the request to be flushed 

            :return None
        """

        page_data = self._read_page(os.path.join(self.cache_path, cache_req.item_id))
        fh = os.open(cache_req.path, os.O_WRONLY)
        page_index = int(cache_req.item_id.split("_")[1])
        os.lseek(fh, page_index*self.page_size, os.SEEK_SET)
        os.write(fh, page_data)
        os.close(fh)

    def _evict(self):
        """ Evict a page from cache. 

            :return None
        """
        evicted_req = self.cache.evict()

        # check if page is dirty 
        if evicted_req.op == 1:
            self._flush_page(evicted_req)
        os.remove(os.path.join(self.cache_path, evicted_req.req_id))
        

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
        
        bytes_read = bytes()
        page_array = self._get_pages(offset, length)
        for page_index, page_start_offset in page_array:

            page_id = self._get_page_id(path, page_index)
            page_path = os.path.join(self.cache_path, page_id)

            if os.path.isfile(cache_path):
                self.cache._update(cache_file_name)
                page_data = self._read_page(cache_path, page_index)
            else:
                if len(self.cache) == self.cachesize:
                    self._evict()
                
                cache_req = Req(cache_file_name, self.page_size, 0, path)
                self.cache._insert(cache_req)

                # read the page from file 
                os.lseek(fh, page_start_offset, os.SEEK_SET)
                page_data = os.read(fh, self.page_size)

                # write the page to cache 
                page_fh = os.open(cache_path, os.O_CREAT|os.O_WRONLY)
                os.write(page_fh, page_data)
                os.close(page_fh)

            # Decide what bytes of the page need to be returned 
            # Case 1: This is the first and last page. 
            if page_index==0 and len(page_array)==1:
                bytes_read += page_data[offset-start_offset:offset-start_offset+length]
            # Case 2: This is the first page. 
            elif page_index==0:
                bytes_read += page_data[offset-start_offset:]
            # Case 3: This is the last page. 
            elif page_index==len(page_array)-1:
                bytes_read += page_data[:end_offset+1]
            # Case 4: This is the middle page 
            else:
                bytes_read += page_data

        os.close(fh)
        return bytes_read

    def write(self, path, buf, offset, fh):

        cur_buf_index = 0
        bytes_written = 0 
        write_len = len(buf)
        page_array = self._get_pages(offset, len(buf))
        for page_index, page_start_offset in page_array:

            cache_file_name = self._get_cache_name(path, page_index)
            cache_path = os.path.join(self.cache_path, cache_file_name)

            # Find the amount of data to be written to the page 
            # Case 1: This is the first and last page. 
            if page_index==0 and len(page_array)==1:
                len_write_data = len(buf)
            # Case 2: This is the first page. 
            elif page_index==0:
                len_write_data = end_offset - offset
            # Case 3: This is the last page. 
            elif page_index==len(page_array)-1:
                len_write_data = offset+write_len - start_offset
            # Case 4: This is the middle page 
            else:
                len_write_data = self.page_size

            if os.path.isfile(cache_path):
                self.cache._update(cache_file_name)
                self._update_page(cache_path, start_offset, offset, buf[cur_buf_index:cur_buf_index+len_write_data])
                self.cache.cacheline_dict[cache_file_name]['op'] = 1
                cur_buf_index += len_write_data
            else:

                if len(self.cache) == self.cachesize:
                    self._evict()

                cache_req = Req(cache_file_name, self.page_size, 1, path)
                self.cache._insert(cache_req)

                # if page aligned, just write to cache 
                if start_offset==offset and len_write_data==self.page_size:
                    page_fh = os.open(cache_path, os.O_CREAT|os.O_WRONLY)
                    os.write(page_fh, buf[cur_buf_index:cur_buf_index+len_write_data])
                    os.close(page_fh)
                # if not page aligned, fetch the page first then update it 
                else:
                    # fetch the page 
                    file_fh = os.open(path, os.O_RDONLY)
                    os.lseek(file_fh, start_offset, os.SEEK_SET)
                    stale_page_data = os.read(file_fh, self.page_size)
                    os.close(file_fh)

                    # write stale data to cache 
                    stale_page_fh = os.open(cache_path, os.O_CREAT|os.O_WRONLY)
                    os.write(stale_page_fh, stale_page_data)
                    os.close(stale_page_fh)

                    # now update this page 
                    self._update(cache_path, start_offset, offset, buf[cur_buf_index:cur_buf_index+len_write_data])
            bytes_written += len_write_data

        #print("Bytes written {} Bytes requested {}".format(bytes_written, len(buf)))
        return bytes_written

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

def main(mountpoint, root, cache_path):
    FUSE(KubeCacheFS(root, cache_path), 
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
    parser.add_argument("-l", "--log",
        help="The path of the log file.")
    args = parser.parse_args()

    main(args.mountpoint, args.storage, args.cache)
