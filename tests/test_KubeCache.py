import unittest
import os, shutil, sys 
sys.path.insert(1, '../KubeCacheFS')
sys.path.insert(2, '../../PyMimircache')

from PyMimircache.cache.lru import LRU
from KubeCache import KubeCache

CACHE_DIR = "./cache"
STORAGE_DIR = "./storage"

def setup_folders():
    check_cache_dir_and_empty_it(CACHE_DIR)
    check_cache_dir_and_empty_it(STORAGE_DIR)

def clean_folders():
    remove_dir(CACHE_DIR)
    remove_dir(STORAGE_DIR)

def check_cache_dir_and_empty_it(dir_path):
    if os.path.isdir(dir_path):
        if os.listdir(dir_path):
            for filename in os.listdir(dir_path):
                file_path = os.path.join(dir_path, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print('Failed to delete %s. Reason: %s' % (file_path, e))
    else:
        os.mkdir(dir_path)


def remove_dir(dir_path):
    shutil.rmtree(dir_path)  


def create_file(file_path, file_size_mb):
    data_to_write=[]
    for i in range(97, 97+26):
        data_to_write.append(chr(i))
    for i in range(65, 65+6):
        data_to_write.append(chr(i))

    num_iter = 32*1024*file_size_mb
    with open(file_path, "w+") as f:
        for i in range(num_iter):
            f.write("".join(data_to_write))


def create_dir_and_fill_with_files(dir_path, file_array):
    os.mkdir(dir_path)
    for filename, size_mb in file_array:
        filepath = os.path.join(dir_path, filename)
        create_file(filepath, size_mb)


def read_file(kcache, path, offset, length):
    fh = os.open(path, os.O_RDWR)
    read_bytes = kcache.read(path, length, offset, fh)
    os.close(fh)


class TestKubeCache(unittest.TestCase):

    def test_dual_cache(self):
        setup_folders()

        page_size = 4096
        cache_size = 1
        cache_config = {
            "cache_dir": CACHE_DIR,
            "page_size": page_size,
            "caches": [{
                "replacement_policy": "LRU",
                "size": cache_size,
                "dir": "dir1"
            }, {
                "replacement_policy": "LRU",
                "size": cache_size,
                "dir": "dir2"
            }]}
        kcache = KubeCache(cache_config)

        dir1_path = os.path.join(STORAGE_DIR, "dir1")
        dir2_path = os.path.join(STORAGE_DIR, "dir2")
        dir3_path = os.path.join(STORAGE_DIR, "dir3")
        create_dir_and_fill_with_files(dir1_path, [["file1", 1]])
        create_dir_and_fill_with_files(dir2_path, [["file1", 1]])
        create_dir_and_fill_with_files(dir3_path, [["file1", 1]])

        # even after 3 pages are read from dir1, there is only 1 cache entry 
        read_file(kcache, os.path.join(dir1_path, "file1"), 0, 10)
        self.assertEqual(len(os.listdir(CACHE_DIR)), 1)
        read_file(kcache, os.path.join(dir1_path, "file1"), 4098, 8192)
        self.assertEqual(len(os.listdir(CACHE_DIR)), 1)

        # no cache entry for reading from dir3
        read_file(kcache, os.path.join(dir3_path, "file1"), 0, 10)
        self.assertEqual(len(os.listdir(CACHE_DIR)), 1)

        # cache entry increases from dir2 but is limited to 2 even if you read more than 2 pages 
        read_file(kcache, os.path.join(dir2_path, "file1"), 0, 10)
        self.assertEqual(len(os.listdir(CACHE_DIR)), 2)
        read_file(kcache, os.path.join(dir2_path, "file1"), 4098, 10000)
        self.assertEqual(len(os.listdir(CACHE_DIR)), 2)

        clean_folders()

    def test_ignore_dir(self):
        setup_folders()

        ignore_dir = "ignore"
        ignore_dir_path = os.path.join(STORAGE_DIR, ignore_dir)
        os.mkdir(ignore_dir_path)
        ignore_filename = "ignore_file"
        ignore_file_path = os.path.join(ignore_dir_path, ignore_filename)
        ignore_filesize_mb = 1

        create_file(ignore_file_path, ignore_filesize_mb)
        self.assertEqual(os.stat(ignore_file_path).st_size, ignore_filesize_mb*1024*1024)

        page_size = 4096
        cache_size = 2
        cache_config = {
            "ignore_dir": ignore_dir,
            "cache_dir": CACHE_DIR,
            "page_size": page_size,
            "caches": [{
                "replacement_policy": "LRU",
                "size": cache_size
            }]}
        kcache = KubeCache(cache_config)

        fh = os.open(ignore_file_path, os.O_RDWR)
        read_bytes = kcache.read(ignore_file_path, 10, 0, fh)
        self.assertFalse(os.listdir(CACHE_DIR))

        string = "string-inserting"
        byte_array = bytearray(string, 'utf-8')
        bytes_written = kcache.write(ignore_file_path, byte_array, 4095, fh)
        self.assertEqual(bytes_written, len(string))

        read_bytes = kcache.read(ignore_file_path, len(string), 4095, fh)
        self.assertEqual(read_bytes, byte_array)

        self.assertEqual(len(os.listdir(CACHE_DIR)), 0) # no caching read or write 

        clean_folders()


    def test_file_being_cached(self):
        setup_folders()

        data_filename = "data_file"
        data_file_path = os.path.join(STORAGE_DIR, data_filename)
        data_filesize_mb = 1
        create_file(data_file_path, data_filesize_mb)
        self.assertEqual(os.stat(data_file_path).st_size, data_filesize_mb*1024*1024)

        page_size = 4096
        cache_size = 2
        cache_config = {
            "cache_dir": CACHE_DIR,
            "page_size": page_size,
            "caches": [{
                "replacement_policy": "LRU",
                "size": cache_size,
                "dir": "*"
            }]}
        kcache = KubeCache(cache_config)

        fh = os.open(data_file_path, os.O_RDWR)
        read_bytes = kcache.read(data_file_path, 10, 0, fh)
        self.assertEqual(len(os.listdir(CACHE_DIR)), 1)
        os.close(fh)

        data_filename = "write_data_file"
        data_file_path = os.path.join(STORAGE_DIR, data_filename)
        data_filesize_mb = 2
        create_file(data_file_path, data_filesize_mb)
        self.assertEqual(os.stat(data_file_path).st_size, data_filesize_mb*1024*1024)

        fh = os.open(data_file_path, os.O_RDWR)
        string = "string-inserting"
        byte_array = bytearray(string, 'utf-8')
        bytes_written = kcache.write(data_file_path, byte_array, 4095, fh)
        self.assertEqual(bytes_written, len(string))
        self.assertEqual(len(os.listdir(CACHE_DIR)), 2)

        bytes_written = kcache.write(data_file_path, byte_array, 8192, fh)
        self.assertEqual(bytes_written, len(string))
        self.assertEqual(len(os.listdir(CACHE_DIR)), 2)

        os.close(fh)
        clean_folders()

    def test_load_config(self):
        setup_folders()
        page_size = 4096
        cache_size = 2
        cache_config = {
            "cache_dir": CACHE_DIR,
            "page_size": page_size,
            "caches": [{
                "replacement_policy": "LRU",
                "size": cache_size
            }]}
        kcache = KubeCache(cache_config)
        self.assertEqual(len(kcache.cache_list), 1)
        self.assertEqual(kcache.cache_list[0].cache_size, cache_size)
        self.assertIs(LRU, type(kcache.cache_list[0]))
        self.assertEqual(kcache.page_size, page_size)
        self.assertEqual(kcache.cache_dir, CACHE_DIR)
        clean_folders()


if __name__ == '__main__':
    unittest.main()