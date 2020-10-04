from query_one import QueryOne

from time import time


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print(">> Starting query 1...")
    start_time = time()
    QueryOne().run()
    end_time = time()
    print(f">> Done with query 1! Total runtime: {round(end_time - start_time, 4)} seconds.")

