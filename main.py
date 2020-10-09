from queries.query_one import QueryOne
from queries.query_two import QueryTwo
from queries.custom_query import CustomQuery
from queries.query_one_wo_kafka import QueryOneWOKafka
from queries.query_two_wo_kafka import QueryTwoWOKafka

from benchmark.cpu_profiler import HardwareMonitor

import time
import tracemalloc
import psutil
import os


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # print(">> Starting query 1...")
    # start_time = time.time()
    # pid = os.getpid()
    # # profile = HardwareMonitor().profile(pid)
    # QueryOne().run()
    #
    # end_time = time.time()
    # print(f">> Done with query 1! Total runtime: {round(end_time - start_time, 4)} seconds.")

    # print(">> Starting query 2...")
    # start_time = time.time()
    # QueryTwo().run()
    # end_time = time.time()
    # print(f">> Done with query 2! Total runtime: {round(end_time - start_time, 4)} seconds.")

    # print(">> Starting query 3...")
    # start_time = time()
    # monitor = CPUMonitor().profile(target=CustomQuery().run())
    # end_time = time()
    # print(f">> Done with query 3! Total runtime: {round(end_time - start_time, 4)} seconds.")
    
    # print("Starting query 1 without Kafka...")
    # start_time = time.time()
    # QueryOneWOKafka().run()
    # end_time = time.time()
    # print("Completed query 1 without Kafka!")

    print("Starting query 2 without Kafka...")
    start_time = time.time()
    QueryTwoWOKafka().run()
    end_time = time.time()
    print("Completed query 2 without Kafka!")
