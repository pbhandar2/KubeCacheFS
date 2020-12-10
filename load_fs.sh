#!/bin/bash

create_dir_if_not_exist () {
    if [ -d $1 ]
    then 
        echo "Directory" $1 "exists!"
    else
        echo "Directory" $1 " does not exist! Will Create."
        mkdir $1 
    fi
}

if [ $# -ne 3 ]; then
    echo "Provide three arguments: application name and file with list \
    of its mounpoints, and the name of the workload to run"
    exit 1
fi

base_dir=/KubeCache
fs_traces_dir=/KubeCache/fs-traces
output_log_dir=/KubeCache/output-log

create_dir_if_not_exist $base_dir
create_dir_if_not_exist $fs_traces_dir
create_dir_if_not_exist $output_log_dir

app_name=$1
mountpoint_list_file=$2
workload_name=$3

app_fs_traces_dir=$fs_traces_dir/$app_name
app_workload_fs_traces_dir=$fs_traces_dir/$app_name/$workload_name
app_output_log_dir=$output_log_dir/$app_name
app_workload_output_log_dir=$output_log_dir/$app_name/$workload_name

create_dir_if_not_exist $app_fs_traces_dir
create_dir_if_not_exist $app_workload_fs_traces_dir

create_dir_if_not_exist $app_output_log_dir
create_dir_if_not_exist $app_workload_output_log_dir

while read mountpoint; do

    create_dir_if_not_exist $mountpoint
    rm -rf $mountpoint/*

    trace_file_path=$app_workload_fs_traces_dir/$mountpoint
    output_file_path=$app_workload_output_log_dir/$mountpoint

    storage_dir=$mountpoint-storage 
    cache_dir=$mountpoint-cache 

    create_dir_if_not_exist $storage_dir
    create_dir_if_not_exist $cache_dir

    echo "Mounting KubeCacheFS. Mountpoint:" $mountpoint \
        "Storage:" $storage_dir "Cache:" $cache_dir \
        "LogFile:" $trace_file_path

    python3 passthrough.py -m $mountpoint -s $storage_dir \
        -c $cache_dir -l $trace_file_path &

    echo "Done Mounting!"

done < $mountpoint_list_file