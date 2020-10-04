from queries.query_one import QueryOne
from queries.query_two import QueryTwo
from queries.custom_query import CustomQuery

from time import time


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # print(">> Starting query 1...")
    # start_time = time()
    # QueryOne().run()
    # end_time = time()
    # print(f">> Done with query 1! Total runtime: {round(end_time - start_time, 4)} seconds.")
    #
    # print(">> Starting query 2...")
    # start_time = time()
    # QueryTwo().run()
    # end_time = time()
    # print(f">> Done with query 2! Total runtime: {round(end_time - start_time, 4)} seconds.")

    print(">> Starting query 3...")
    start_time = time()
    CustomQuery().run()
    end_time = time()
    print(f">> Done with query 3! Total runtime: {round(end_time - start_time, 4)} seconds.")

