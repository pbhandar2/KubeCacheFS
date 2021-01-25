working_set_size_gb=$1
io_size_gb=$2

echo $working_set_size_gb GB, $io_size_gb GB

mkdir /fiobench 
rm -rf /fiobench/*
#docker run -v /fiobench:/data ljishen/fio --name=fiotest --directory=/data --size=${working_set_size_gb}Gi --nrfiles=10 --rw=write --direct=1

echo "Files Laid! Now clearing the buffer cache again and reading these files!"
#sync; echo 3 > /proc/sys/vm/drop_caches 

output_file_name=$(awk '/^Mem/ {printf($4"_"$6"_"$7);}' <(free -m))

echo "$output_file_name is the output!" 
docker run -v /fiobench:/data ljishen/fio --name=fiotest --directory=/data --rw=randread --size=${working_set_size_gb}Gi --nrfiles=10 --io_size=${io_size_gb}Gi --output-format=json --output=/data/${output_file_name}.json 
