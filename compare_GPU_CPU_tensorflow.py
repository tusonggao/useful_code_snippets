from __future__ import print_function
import matplotlib
import matplotlib.pyplot as plt
import tensorflow as tf
import time

# 去除警告信息
import os
os.environ['TF_CPP_MIN_LOG_LEVEL']='2'

def get_cpu_gpu_times(maximum_time):
    device_times = {
        "/gpu:0":[],
        "/cpu:0":[]
    }
    matrix_sizes = range(500,50000,50)

    for size in matrix_sizes:
        for device_name in device_times.keys():

            print("####### Calculating on the " + device_name + " #######")

            shape = (size,size)
            data_type = tf.float16
            with tf.device(device_name):
                r1 = tf.random_uniform(shape=shape, minval=0, maxval=1, dtype=data_type)
                r2 = tf.random_uniform(shape=shape, minval=0, maxval=1, dtype=data_type)
                dot_operation = tf.matmul(r2, r1)


            with tf.Session(config=tf.ConfigProto(log_device_placement=True)) as session:
                    start_time = time.time()
                    result = session.run(dot_operation)
                    time_taken = time.time() - start_time
                    print(result)
                    device_times[device_name].append(time_taken)

            print(device_times)

            if time_taken > maximum_time:
                return device_times, matrix_sizes


def get_gpu_times(maximum_time):
    device_times = {
        "/gpu:0":[],
        "/cpu:0":[]
    }

    gpu_times = []
    sizes_lst = []
    matrix_sizes = range(500,50000,50)

    for size in matrix_sizes:
        print("####### Calculating on the matrix size of ", size, " #######")

        shape = (size,size)
        data_type = tf.float16
        with tf.device('/gpu:0'):
            r1 = tf.random_uniform(shape=shape, minval=0, maxval=1, dtype=data_type)
            r2 = tf.random_uniform(shape=shape, minval=0, maxval=1, dtype=data_type)
            dot_operation = tf.matmul(r2, r1)

        with tf.Session(config=tf.ConfigProto(log_device_placement=True)) as session:
            start_time = time.time()
            result = session.run(dot_operation)
            time_taken = time.time() - start_time
            print(result)
            gpu_times.append(time_taken)
            sizes_lst.append(size)

        print(gpu_times)

        if time_taken > maximum_time:
            return gpu_times, sizes_lst


###############################################################################

# device_times, matrix_sizes = get_cpu_gpu_times(1.5)
# gpu_times = device_times["/gpu:0"]
# cpu_times = device_times["/cpu:0"]
#
# plt.plot(matrix_sizes[:len(gpu_times)], gpu_times, 'o-')
# plt.plot(matrix_sizes[:len(cpu_times)], cpu_times, 'o-')
# plt.ylabel('Time')
# plt.xlabel('Matrix size')
# plt.show()

###############################################################################

# gpu_times, sizes_lst = get_gpu_times(0.35)
#
# plt.plot(sizes_lst, gpu_times, 'o-')
# plt.ylabel('Time')
# plt.xlabel('Matrix size')
# plt.show()

###############################################################################