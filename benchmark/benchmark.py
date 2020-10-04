import rx
from time import time

import numpy as np


class Benchmark():
    
    def __init__(self):
        self.latecy = []
    
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
        self.latecy.append(x["latency"])
        mean_latency = np.mean(self.latecy)
        
        print(f"Mean latency @ {x['i'] if 'i' in x.keys() else x['s']}: {round(float(mean_latency) * 1000, 2) } ms. "
              f"Processing time w/o setup: {np.sum(self.latecy)}")
    
    def get_message_size(self):
        pass