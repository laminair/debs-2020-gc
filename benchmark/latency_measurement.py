import rx
from time import time
from kafka import KafkaConsumer

import numpy as np


class LatencyBenchmark():
    
    def __init__(self):
        self.latency = []
    
    def inject_time(self, label):
        def _inject(source):
            def subscribe(observer, scheduler):
                def on_next(obj):
                    
                    assert type(obj) == dict
                    
                    obj[label] = time()
                    
                    observer.on_next(obj)

                return source.subscribe(
                    on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler
                )
            return rx.create(subscribe)
        return _inject

    def get_latency(self, label):
        def _get(source):
            def subscribe(observer, scheduler):
                def on_next(obj):
                    assert type(obj) == dict
                    
                    if label in obj.keys():
                        obj["latency"] = time() - obj[label]
                
                    observer.on_next(obj)
            
                return source.subscribe(
                    on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler
                )
        
            return rx.create(subscribe)
    
        return _get
    
    def calc_mean_latency(self, x):
        self.latency.append(x["latency"])
        mean_latency = np.mean(self.latency)
        idx = x['i'] if 'i' in x.keys() else x['s']

        if idx % 100 == 0 or idx % 14999 == 0:
            print(f"{idx}, {mean_latency}, {sum(self.latency)}")
            self.latency = []

    def append_kafka_metrics(self):
        def _append(source):
            def subscribe(observer, scheduler):
                kafka_client = [KafkaConsumer()]

                def on_next(obj):
                    nonlocal kafka_client

                    if obj["s"] % 1000 == 0 or obj["s"] == 14999:
                        obj["kafka_metrics"] = kafka_client[0].metrics()

                    observer.on_next(obj)

                return source.subscribe(
                    on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler
                )
            return rx.create(subscribe)
        return _append

    def measure_kafka(self, x):
        if "kafka_metrics" in x.keys():
            print(x["kafka_metrics"]["consumer-metrics"])

    
    def get_message_size(self):
        pass