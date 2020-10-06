import time
import multiprocessing as mp
import psutil
import numpy as np


class HardwareMonitor():
    
    def __init__(self):
        self.monitor = []
        
    def profile(self, pid):
        proc = psutil.Process(pid)

        while proc.is_running():
            self.monitor.append((
                psutil.virtual_memory()[0],
                proc.memory_percent() * psutil.virtual_memory()[0],
                proc.memory_percent()
            ))
            time.sleep(1)

        return self.monitor
