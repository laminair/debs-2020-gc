import kafka
import time
import math

import numpy as np


class KafkaProducer():
    
    def __init__(self):
        self.end_time_dict = {}
        self.results = {}
    
    def sink(self):
        pass
    
    def benchmark_sink(self, x):
        self.end_time_dict[x["s"]] = time.time()
        self.results[x["s"]] = int(x["d"])
        print(f"Processing: {x['s']}")

    def show_benchmark_results(self, start_time_dict):
        latency_dict = {}
        for k, v in self.end_time_dict.items():
            if k in start_time_dict.keys():
                latency_dict[k] = round((v - start_time_dict[k]) * 1000, 2)

        latency_mean_dict = {}
        for k, v in latency_dict.items():
            base_key = math.floor(k / 100) * 100
            if base_key in latency_mean_dict.keys():
                latency_mean_dict[base_key].append(v)
            else:
                latency_mean_dict[base_key] = [v, ]

        for k, v in latency_mean_dict.items():
            latency_mean_dict[k] = float(np.mean(v))

        events = {}
        for k, v in self.results.items():
            base_key = math.floor(k/100) * 100
            if base_key in events.keys():
                if self.results[k] == 1:
                    events[base_key] += 1
            else:
                if self.results[k] == 1:
                    events[base_key] = 1

        for k, v in latency_mean_dict.items():
            if k in events.keys():
                print(f"{k}, {v}, {events[k]}")
            else:
                print(f"{k}, {v}, 0")
