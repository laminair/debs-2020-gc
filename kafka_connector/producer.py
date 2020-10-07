import kafka
import time


class KafkaProducer():
    
    def __init__(self):
        self.end_time_dict = {}
        self.results = {}
    
    def sink(self):
        pass
    
    def benchmark_sink(self, x):
        print(x)
        self.end_time_dict[x["s"]] = time.time()
        self.results[x["s"]] = int(x["d"])
        
    def show_benchmark_results(self, start_time_dict):
        latency_dict = {}
        for k, v in self.end_time_dict.items():
            if k in start_time_dict.keys():
                latency_dict[k] = round((v - start_time_dict[k]) * 1000, 2)
                
        for k, v in latency_dict.items():
            if k in self.results.keys():
                print(f"{k}, {v}, {self.results[k]}")
