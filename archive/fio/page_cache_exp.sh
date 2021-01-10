i=$1
echo "Current: " $i
sync; echo 3 > /proc/sys/vm/drop_caches

stress-ng --vm 9 --vm-bytes $i% --timeout 500s &
fio --name=basic --directory=/home/ --size=5Gi --bs=4Ki --filesize=1Gi --rw=randread --output-format=json --output=/home/out_$i.json &
sleep 500
