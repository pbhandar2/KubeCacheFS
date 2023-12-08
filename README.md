#KubeCacheFS

KubeCacheFS is a userspace filesystem built using FUSE.
It is designed to decouple I/O caching from the underlying
filesystem for predictable and effecient I/O caching compared
to that of the page cache. It bypasses the page cache by using
direct I/O and utilizes tmpfs to cache I/O.
