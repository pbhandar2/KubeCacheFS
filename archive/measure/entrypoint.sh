#!/bin/bash

fs_dir=$1 
jobfile_dir=$2
output_dir=$3
experiment_name=$4

mkdir $output_dir/$experiment_name

for job_filename in $jobfile_dir/*; do 
    sync; echo 3 > /proc/sys/vm/drop_caches 
    job_name=$(echo $(basename $job_filename) | cut -f 1 -d '.')
    ./run_workload.sh $jobfile_dir $job_name $fs_dir $output_dir/$experiment_name
done