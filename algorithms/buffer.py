import rx
import math
import numpy as np
import pandas as pd

from queue import PriorityQueue



class CustomBuffer():
    
    def __init__(self):
        self.buffer = dict()
        self.sort_q = dict()
        self.current_idx = 0
        self.current_sort_idx = 0
        
    def buffer_and_manage(self, time_out_window = 20):
        def _buffer(source):
            def subscribe(observer, scheduler):
                
                def on_next(obj):
                    nonlocal self
                    
                    if len(obj["records"]) > 1000:
                        
                        records_one = obj["records"][:1000]
                        records = obj["records"][1001:]
                        
                        observer.on_next({"i": obj["i"], "records": records_one})
                        
                        if len(records) == 1000:
                            base_idx = math.floor(records[0]["i"]/1000)
                            observer.on_next({"i": base_idx, "records": records})
                        else:
                            start_idx = records[0]["i"]
                            end_idx = records[-1]["i"]
                            base_idx = math.floor(start_idx / 1000)
                            
                            if base_idx in self.buffer.keys():
                                obj = {"start_idx": start_idx, "end_idx": end_idx, "records": records}
                                res = self.queue_down(obj=obj, base_idx=base_idx, end_idx=end_idx)
                                observer.on_next(res)
                            else:
                                self.queue_up(obj=obj, base_idx=base_idx, start_idx=start_idx, end_idx=end_idx)
                                
                    elif len(obj["records"]) < 1000:
                        start_idx = obj["records"][0]["i"]
                        end_idx = obj["records"][-1]["i"]
                        base_idx = math.floor(start_idx / 1000)
                        
                        if base_idx in self.buffer.keys():
                            records = self.queue_down(obj=obj, base_idx=base_idx, end_idx=end_idx)
                            observer.on_next(records)
                        else:
                            self.queue_up(obj=obj, base_idx=base_idx, start_idx=start_idx, end_idx=end_idx)
                        
                    else:
                        observer.on_next(obj)
                        
                    timeout = time_out_window - 1
                    timeout_idx = self.current_idx - timeout
                    if timeout_idx > 0 and timeout_idx in self.buffer.keys():
                        records = self.queue_down(base_idx=timeout_idx)
                        observer.on_next(records)

                    self.current_idx += 1
                    
                def on_completed():
                    nonlocal self

                    while len(self.buffer.keys()) > 0:
                        key = list(self.buffer.keys())[0]
                        buffered = self.buffer[key]
                        
                        if len(buffered["records"]) != 1000:
                            df = pd.DataFrame(buffered["records"])
                            mean = {
                                "i": int(df["i"].mean()),
                                "voltage": float(df["voltage"].mean()),
                                "current": float(df["current"].mean())
                            }
                            while len(buffered["records"]) < 1000:
                                buffered["records"].append(mean)
    
                        del self.buffer[key]
                        res = {"i": key, "records": buffered["records"]}
                        observer.on_next(res)
                    
                    observer.on_completed()
                    
                return source.subscribe(
                    on_next,
                    observer.on_error,
                    on_completed,
                    scheduler
                )
            return rx.create(subscribe)
        return _buffer
    
    def sort_buffer(self):
        def _sort(source):
            def subscribe(observer, scheduler):
                
                def on_next(obj):
                    nonlocal self
                    if self.current_sort_idx in self.sort_q:
                        res = self.sort_q[self.current_sort_idx]
                        observer.on_next(res)
                        del self.sort_q[res["i"]]
                        self.current_sort_idx += 1

                    if obj["i"] != self.current_sort_idx:
                        self.sort_q[obj["i"]] = obj

                    if obj["i"] == self.current_sort_idx:
                        observer.on_next(obj)
                        self.current_sort_idx += 1
                        
                def on_completed():
                    nonlocal self
                    remaining_idx = [key for key in self.sort_q.keys() if key >= self.current_sort_idx]
                    new_sort_q = {idx: self.sort_q[idx] for idx in remaining_idx}
                    
                    while len(new_sort_q.keys()) > 0:
                        if self.current_sort_idx in new_sort_q.keys():
                            observer.on_next(new_sort_q[self.current_sort_idx])
                            del new_sort_q[self.current_sort_idx]
                            self.current_sort_idx += 1
                    
                    observer.on_completed()

                return source.subscribe(
                    on_next,
                    observer.on_error,
                    on_completed,
                    scheduler
                )
            return rx.create(subscribe)
        return _sort
    
    def check_order(self):
        def _check(source):
            def subscribe(observer, scheduler):
                def on_next(obj):
                    print(f"{obj['i']}: {len(obj['records'])}")

                return source.subscribe(
                    on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler
                )
            return rx.create(subscribe)
        return _check
        
    def queue_up(self, obj, base_idx, start_idx, end_idx):
        
        if not base_idx + 19 < self.current_idx:
            self.buffer[base_idx] = {"start_idx": start_idx, "end_idx": end_idx,
                                     "records": obj["records"]}
        
    def queue_down(self, base_idx, end_idx=None, obj=None):
        buffered = self.buffer[base_idx]
        
        if obj:
            if end_idx < buffered["start_idx"]:
                records = obj["records"] + buffered["records"]
            else:
                records = buffered["records"] + obj["records"]
        else:
            records = buffered["records"]

        if len(records) != 1000:
            df = pd.DataFrame(records)
            mean = {
                "i": int(df["i"].mean()),
                "voltage": float(df["voltage"].mean()),
                "current": float(df["current"].mean())
            }
            while len(records) < 1000:
                records.append(mean)

        del self.buffer[base_idx]
        return {"i": base_idx, "records": records}
    
    def sort_time_stamps(self, obj):
    
        if self.current_sort_idx in self.sort_q:
            res = self.sort_q[self.current_sort_idx]
            del self.sort_q[res["i"]]
            self.current_sort_idx += 1
            return res
    
        if obj["i"] != self.current_sort_idx:
            self.sort_q[obj["i"]] = obj
            return None
    
        if obj["i"] == self.current_sort_idx:
            self.current_sort_idx += 1
            return obj