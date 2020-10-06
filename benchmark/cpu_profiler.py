import time
import multiprocessing as mp
import psutil
import numpy as np


class HardwareMonitor():
    
    def __init__(self):
        self.monitor = []
        
    def profile(self, target):
        worker = mp.Process(target=target)
        worker.start()
        p = psutil.Process(worker.pid)
        
        while worker.is_alive():
            self.monitor.append({
                "cpu": p.cpu_percent(),
                "memory": (psutil.virtual_memory()[0], p.memory_percent() * psutil.virtual_memory()[0], p.memory_percent())
            })
            time.sleep(1)
    
        worker.join()
        return self.monitor
