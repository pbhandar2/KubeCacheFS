import itertools 
import os 

workload_dir = "./workloads"
size_array = [96] # in GB 
nrfiles_array = [1]
blocksize_array = [4] # in KB

def write(job_file_handle, metric_name, metric_value):
    job_file_handle.write("{}={} \n".format(metric_name, metric_value))

def init_workload(job_file_handle, workload_name):
    job_file_handle.write("[{}] \n".format(workload_name))

def main():
    workload_features = [
        size_array,
        nrfiles_array,
        blocksize_array,
        read_ratio
    ]

    for index, workload in enumerate(list(itertools.product(*workload_features))):
        workload_name = "w{}".format(index)
        job_file_path = os.path.join(workload_dir, "{}.job".format(workload_name))

        with open(job_file_path, "w+") as job_file_handle:
            init_workload(job_file_handle, workload_name)

            # write the size first 
            write(job_file_handle, "size", "{}Gi".format(workload[0]))
            write(job_file_handle, "nrfiles", "{}".format(workload[1]))
            write(job_file_handle, "bs", "{}Ki".format(workload[2]))
            write(job_file_handle, "rw", "randread")
            write(job_file_handle, "rwmixread", workload[3])

if __name__=="__main__":
    main()

