rm -rf /mountpoint/*
rm -rf /cache/*
rm -rf /storage/*
cd /storage 
fio --name=test --filesize=500Mi --rw=randread 
sync; echo 3 > /proc/sys/vm/drop_caches 
mount -t tmpfs -o size=548m tmpfs /cache 
