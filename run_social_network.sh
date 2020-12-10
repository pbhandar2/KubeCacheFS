output_log_dir=/KubeCache/output-log

app_directory=$1
app_name=$2
workload_name=$3
time=$(($4*60))
w1=$5
w2=$6
w3=$7

app_workload_output_log_dir=$output_log_dir/$app_name/$workload_name

cd $app_directory
docker-compose up -d 
sleep 20

cd $app_directory/wrk2
$app_directory/wrk2/wrk -D exp -t 1 -c 10 -d $time \
    -L -s ./scripts/social-network/compose-post.lua \
    http://localhost:8080/wrk2-api/post/compose -R $w1 >> $app_workload_output_log_dir/compose &

$app_directory/wrk2/wrk -D exp -t 1 -c 10 -d $time \
    -L -s ./scripts/social-network/read-home-timeline.lua \
    http://localhost:8080/wrk2-api/home-timeline/read -R $w2 >> $app_workload_output_log_dir/read-home &

$app_directory/wrk2/wrk -D exp -t 1 -c 10 -d $time \
    -L -s ./scripts/social-network/read-user-timeline.lua \
    http://localhost:8080/wrk2-api/user-timeline/read -R $w3 >> $app_workload_output_log_dir/read-user &