## declare an array variable
declare -a mountpoint_arr=(
    "/user-mongodb" 
    "/media-mongodb" 
    "/url-shorten-mongodb"
    "/user-timeline-mongodb"
    "/post-storage-mongodb"
    "/social-graph-mongodb"
)

for mnt in "${mountpoint_arr[@]}"
do
    umount $mnt
done