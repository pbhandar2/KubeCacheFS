from enum import Enum
import json 
import os 
import math 
import hashlib 
import numpy as np 

from PyMimircache.cache.lru import LRU
from PyMimircache.cacheReader.requestItem import Req

class ReplacementPolicy(Enum):
    LRU = 1
    LFU = 2
    MRU = 3

class KubeCache:
    """ KubeCache handles caching for KubeCacheFS """

    def __init__(self, config):
        self.cache_dir = config["cache_dir"]
        self.page_size = config["page_size"]
        self.ignore_dir_list = config["ignore_dir"] if "ignore_dir" in config else []
        self.cache_config_list = config["caches"]
        self.cache_list = KubeCache._get_cache_list_from_config(config)

    @staticmethod
    def _get_cache_list_from_config(config):
        cache_list = []
        print(config)
        for cache in config["caches"]:
            if cache["replacement_policy"]=="LRU":
                cache_list.append(LRU(cache["size"]))
        return cache_list 

    def _get_pages(self, offset, length):
        """ Get all the relevant pages for a file at an offset and length 

            :param offset: the offset at which the read/write begins 
            :param length: the length of the request """

        start_page = math.floor(offset/self.page_size)
        end_page = math.floor((offset+length-1)/self.page_size)
        
        page_array = np.empty(shape=(end_page-start_page+1, 2), dtype=int)
        for page_array_index, page_index in enumerate(range(start_page, end_page+1)):
            page_start_offset = page_index*self.page_size
            page_array[page_array_index] = np.array([page_index,page_start_offset])
        return page_array

    def _get_page_id(self, path, page_index):
        """ Get the id of the page based on the path and page being requested  

            :param path: the path of the file being accessed 
            :param page_index: the index of the page 

            :return page_id: the id/filename of the page in cache """

        hash_object = hashlib.md5(path.encode())
        hash_val = hash_object.hexdigest()
        return "{}_{}".format(hash_val, page_index)

    def _read_page(self, page_path):
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

            :return: None """

        fh = os.open(page_path, os.O_WRONLY)

        # check if you need to seek before writing 
        if req_offset>page_offset:
            os.lseek(fh, req_offset-page_offset, os.SEEK_SET)
        os.write(fh, buf)
        os.close(fh) 

    def _flush_page(self, cache_req):
        """ Flush the content of the page to persistent storage. 

            :param cache_req: the request to be flushed 

            :return None """

        page_data = self._read_page(os.path.join(self.cache_dir, cache_req.item_id))
        fh = os.open(cache_req.path, os.O_WRONLY)
        page_index = int(cache_req.item_id.split("_")[1])
        os.lseek(fh, page_index*self.page_size, os.SEEK_SET)
        os.write(fh, page_data)
        os.close(fh)

    def _evict(self, cache_index):
        """ Evict a page from cache. 

            :param cache_index: the index of the cache to be evicted from 

            :return None """
        evicted_id, evicted_req = self.cache_list[cache_index].evict()

        if evicted_req.op == 1:
            self._flush_page(evicted_req)
        os.remove(os.path.join(self.cache_dir, evicted_id))


    def read(self, path, length, offset, fh):      
        bytes_read = bytes()

        # check if the any directory in the path is in the ignore list  
        for ignore_dir in self.ignore_dir_list:
            if ignore_dir in path:
                os.lseek(fh, offset, os.SEEK_SET)
                return os.read(fh, length)

        cache_index = None 
        for cur_cache_index, cache in enumerate(self.cache_config_list):
            if cache["dir"] == "*" and cache_index is None:
                cache_index = cur_cache_index
            elif cache["dir"] in path:
                cache_index = cur_cache_index
        
        if cache_index is None:
            os.lseek(fh, offset, os.SEEK_SET)
            return os.read(fh, length)

        page_array = self._get_pages(offset, length)
        for page_index, page_start_offset in page_array:

            page_id = self._get_page_id(path, page_index)
            page_path = os.path.join(self.cache_dir, page_id)

            if os.path.isfile(page_path):
                self.cache_list[cache_index]._update(page_id)
                page_data = self._read_page(page_path)
            else:
                if len(self.cache_list[cache_index]) == self.cache_config_list[cache_index]["size"]:
                    self._evict(cache_index)
                
                cache_req = Req(page_id, self.page_size, 0, path)
                self.cache_list[cache_index]._insert(cache_req)

                # read the page from file 
                os.lseek(fh, page_start_offset, os.SEEK_SET)
                page_data = os.read(fh, self.page_size)

                # write the page to cache 
                page_fh = os.open(page_path, os.O_CREAT|os.O_WRONLY)
                os.write(page_fh, page_data)
                os.close(page_fh)

            # Decide what bytes of the page need to be returned 
            # Case 1: This is the first and last page. 
            if page_index==0 and len(page_array)==1:
                bytes_read += page_data[offset-page_start_offset:offset-page_start_offset+length]
            # Case 2: This is the first page. 
            elif page_index==0:
                bytes_read += page_data[offset-page_start_offset:]
            # Case 3: This is the last page. 
            elif page_index==len(page_array)-1:
                bytes_read += page_data[:offset+length-page_start_offset+1]
            # Case 4: This is the middle page 
            else:
                bytes_read += page_data

        # setting the seek where it needs to be 
        os.lseek(fh, offset+length, os.SEEK_SET)
        return bytes_read

    def write(self, path, buf, offset, fh):

        # check if the any directory in the path is in the ignore list  
        for ignore_dir in self.ignore_dir_list:
            if ignore_dir in path:
                os.lseek(fh, offset, os.SEEK_SET)
                return os.write(fh, buf)

        cache_index = None 
        for cur_cache_index, cache in enumerate(self.cache_config_list):
            if cache["dir"] == "*" and cache_index is None:
                cache_index = cur_cache_index
            elif cache["dir"] in path:
                cache_index = cur_cache_index
        
        if cache_index is None:
            os.lseek(fh, offset, os.SEEK_SET)
            return os.write(fh, buf)

        cur_buf_index = 0
        bytes_written = 0 
        write_len = len(buf)
        page_array = self._get_pages(offset, len(buf))
        """
            We need page_index and page_array_index. page_array_index is needed in order to know 
            if it is the first page of the write request. The first and last page of the write re
            quest is not written totally so we need to handle the case where we partially write 
            a page. We need page_index to generate out page_path. 
        """
        for page_array_index, (page_index, page_start_offset) in enumerate(page_array):

            page_id = self._get_page_id(path, page_index)
            page_path = os.path.join(self.cache_dir, page_id)

            # Find the amount of data to be written to the page 
            # Case 1: This is the first and last page. 
            if page_array_index==0 and len(page_array)==1:
                len_write_data = len(buf)
            # Case 2: This is the first page. 
            elif page_array_index==0:
                len_write_data = page_start_offset + self.page_size - offset
            # Case 3: This is the last page. 
            elif page_array_index==len(page_array)-1:
                len_write_data = offset+write_len - page_start_offset
            # Case 4: This is the middle page 
            else:
                len_write_data = self.page_size

            if os.path.isfile(page_path):
                self.cache_list[cache_index]._update(page_id)
                cur_req = self.cache_list[cache_index].cacheline_dict[page_id]
                new_req = Req(cur_req.item_id, self.page_size, 1, path)
                self.cache_list[cache_index].cacheline_dict[page_id] = new_req
                self._update_page(page_path, page_start_offset, offset, buf[cur_buf_index:cur_buf_index+len_write_data])
                cur_buf_index += len_write_data
            else:
                if len(self.cache_list[cache_index]) == self.cache_config_list[cache_index]["size"]:
                    self._evict(cache_index)

                cache_req = Req(page_id, self.page_size, 1, path)
                self.cache_list[cache_index]._insert(cache_req)

                # if page aligned, just write to cache 
                if page_start_offset==offset and len_write_data==self.page_size:
                    page_fh = os.open(page_path, os.O_CREAT|os.O_WRONLY)
                    os.write(page_fh, buf[cur_buf_index:cur_buf_index+len_write_data])
                    os.close(page_fh)
                # if not page aligned, fetch the page first then update it 
                else:
                    # fetch the page 
                    file_fh = os.open(path, os.O_RDONLY)
                    os.lseek(file_fh, page_start_offset, os.SEEK_SET)
                    stale_page_data = os.read(file_fh, self.page_size)
                    os.close(file_fh)

                    # write stale data to cache 
                    stale_page_fh = os.open(page_path, os.O_CREAT|os.O_WRONLY)
                    os.write(stale_page_fh, stale_page_data)
                    os.close(stale_page_fh)

                    # now update this page 
                    self._update_page(page_path, page_start_offset, offset, buf[cur_buf_index:cur_buf_index+len_write_data])
            bytes_written += len_write_data

        # setting the seek where it needs to be 
        os.lseek(fh, offset+len(buf), os.SEEK_SET)
        return bytes_written

    @staticmethod 
    def get_config_from_file(config_file):
        config = {}
        with open(config_file) as f:
            config = json.loads(f)
        return config 

    @staticmethod 
    def get_cache_from_config(cache_config):
        cache = []
        total_size = 0
        for cache in cache_config:
            if cache["replacement_policy"] == "LRU":
                cache.append(LRU(cache["size"]))
        assert(total_size==self.size)
        return cache 

    @staticmethod
    def get_config_and_cache_from_config_file(config_file):
        cache = []
        config = {}
        if config_file == -1:
            cache.append(LRU(self.size))
            config = {
                "replacement_policy": "LRU",
                "size": self.size 
            }
        elif os.path.isfile(config_file):
            config = get_config_from_file(config_file)
            cache = get_cache_from_config(config)
        else:
            raise ValueError("The path entered for cache configuration file does not exist or is not a file.")
        return config, cache 

    def __repr__(self):
        pass 

    def __str__(self):
        pass         