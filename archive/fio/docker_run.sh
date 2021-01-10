docker run -it --privileged -v /home/page_cache_exp:/home -m=1Gi ubuntu 

apt-get update 
apt install -y fio stress-ng vim

dd if=/dev/urandom of=/home/basic.0.0 bs=1M count=1024 oflag=direct 


