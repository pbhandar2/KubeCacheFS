jobfile_dir=$1
workload_name=$2
workload_file_dir=$3
output_dir=$4

fio $1/$2.job --output-format=json --output=$4/$2.json -directory=$3