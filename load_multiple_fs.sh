## declare an array variable
declare -a mountpoint_arr=(
    "/user-mongodb" 
    "/media-mongodb" 
    "/url-shorten-mongodb"
    "/user-timeline-mongodb"
    "/post-storage-mongodb"
    "/social-graph-mongodb"
)

declare log_storage_path="/kubecachelogs"
declare workload_name=$1

## first create the storage directory and the cache directory 
for mnt in "${mountpoint_arr[@]}"
do
    
    rm -rf $mnt/*
    
    storage_path="$mnt"-storage
    cache_path="$mnt"-cache
    workload_log_path="$log_storage_path"/"$workload_name"
    log_file_path="$workload_log_path""$mnt"

    if [-d $log_file_path]
    then 
        echo "Directory" $log_storage_path "exists."
    else
        echo "Directory" $log_storage_path "does not exist. Creating a new directory ..."
        mkdir $log_storage_path
    fi

    if [-d $workload_log_path]
    then 
        echo "Directory" $workload_log_path "exists."
    else
        echo "Directory" $workload_log_path "does not exist. Creating a new directory ..."
        mkdir $workload_log_path
    fi

    if [ -d $storage_path ] 
    then
        echo "Directory" $storage_path "exists. Deleting all files in the directory..." 
        rm -rf $storage_path/*
    else
        echo "Directory" $storage_path " does not exists. Creating a new directory..." 
        mkdir $storage_path
    fi

    if [ -d $cache_path ] 
    then
        echo "Directory" $cache_path "exists. Deleting all files in the directory..." 
        rm -rf $cache_path/*
    else
        echo "Directory" $cache_path " does not exists. Creating a new directory..." 
        mkdir $cache_path
    fi

    echo "Mounting KubeCacheFS. Mountpoint:" $mnt ", Storage:" $storage_path \
        "Cache:" $cache_path "LogFile:" $log_file_path

    python3 passthrough.py -m $mnt -s $storage_path -c $cache_path -l $log_file_path &

    echo "Done Mounting!"
done