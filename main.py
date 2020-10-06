from queries.query_one import QueryOne
from queries.query_two import QueryTwo
from queries.custom_query import CustomQuery
from benchmark.cpu_profiler import HardwareMonitor

from time import time
import tracemalloc


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print(">> Starting query 1...")
    start_time = time()
    monitor = HardwareMonitor().profile(target=QueryOne().run())
    end_time = time()
    print(f">> Done with query 1! Total runtime: {round(end_time - start_time, 4)} seconds.")

    # print(">> Starting query 2...")
    # start_time = time()
    # QueryTwo().run()
    # end_time = time()
    # print(f">> Done with query 2! Total runtime: {round(end_time - start_time, 4)} seconds.")

    # print(">> Starting query 3...")
    # start_time = time()
    # monitor = CPUMonitor().profile(target=CustomQuery().run())
    # end_time = time()
    # print(f">> Done with query 3! Total runtime: {round(end_time - start_time, 4)} seconds.")
    
    for element in monitor:
        print(f"{element['cpu']}, {element['memory'][0]}, {element['memory'][1]}, {element['memory'][2]}")

