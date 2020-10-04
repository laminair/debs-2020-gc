import numpy as np
import pandas as pd
import rx

from queue import Queue
from scipy.signal import savgol_filter


class ChangePointDetector():
    
    def __init__(self, window_size):
        self.window_size = window_size
    
    def apply_filter(self, filter, **kwargs):
        
        available_choices = {
            "sma": self.simple_moving_average(),
            "moving_median": self.moving_median(),
            "sav_gol": self.savitzky_golay()
        }
        
        if not filter in available_choices.keys():
            raise AssertionError(f"Make sure to choose an available filter mechanism. "
                                 f"Choices are: {available_choices.keys()}")
        else:
            return available_choices[filter]
    
    def simple_moving_average(self):
        def _sma(source):
            def subscribe(observer, scheduler):
                window = [Queue(maxsize=self.window_size), ]
                
                def on_next(obj):
                    nonlocal window
                    
                    if type(obj) != dict:
                        observer.on_error("Error occurred when applying SMA de-noising. Make sure to pass dict "
                                          "elements.")
                    else:
                        if window[0].full():
                            window[0].get_nowait()
                            window[0].put_nowait((obj['i'], obj))
                        else:
                            window[0].put_nowait((obj['i'], obj))
                        
                        dl = []
                        for el in window[0].queue:
                            dl.append(el[1])
                        
                        data = pd.DataFrame(dl, columns=['p', 's', 'q'])
                        
                        obj['p_smooth'] = np.mean(np.array(data['p']))
                        obj['q_smooth'] = np.mean(np.array(data['q']))
                        
                        observer.on_next(obj)
                
                return source.subscribe(
                    on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler
                )
            
            return rx.create(subscribe)
        
        return _sma
    
    def moving_median(self):
        def _mm(source):
            def subscribe(observer, scheduler):
                window = [Queue(), ]
                
                def on_next(obj):
                    nonlocal window
                    
                    if type(obj) != dict:
                        observer.on_error("Error occurred when applying SMA de-noising. Make sure to pass dict "
                                          "elements.")
                    else:
                        if len(window[0].queue) == self.window_size:
                            window[0].get_nowait()
                            window[0].put_nowait((obj['i'], obj))
                        else:
                            window[0].put_nowait((obj['i'], obj))
                        
                        dl = []
                        for el in window[0].queue:
                            dl.append(el[1])
                        
                        data = pd.DataFrame(dl, columns=['p', 's', 'q'])
                        
                        obj['p_smooth'] = np.median(np.array(data['p']))
                        obj['q_smooth'] = np.median(np.array(data['q']))
                        
                        observer.on_next(obj)
                
                return source.subscribe(
                    on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler
                )
            
            return rx.create(subscribe)
        
        return _mm
    
    def savitzky_golay(self):
        def _savgol(source):
            def subscribe(observer, scheduler):
                window = [Queue(), ]
                
                def on_next(obj):
                    nonlocal window
                    
                    if type(obj) != dict:
                        observer.on_error("Error occurred when applying SMA de-noising. Make sure to pass dict "
                                          "elements.")
                    else:
                        if len(window[0].queue) == self.window_size:
                            window[0].get_nowait()
                            window[0].put_nowait((obj['i'], obj))
                        else:
                            window[0].put_nowait((obj['i'], obj))
                        
                        dl = []
                        for el in window[0].queue:
                            dl.append(el[1])
                        
                        data = pd.DataFrame(dl, columns=['p', 's', 'q'])
                        
                        if len(window[0].queue) > 10:
                            filter_window_size = len(window[0].queue) / 2
                        else:
                            filter_window_size = len(window[0].queue)
                        
                        if filter_window_size % 2 == 0:
                            filter_window_size = filter_window_size - 1
                        else:
                            filter_window_size = filter_window_size
                        
                        obj['p_smooth'] = np.array(savgol_filter(x=data['p'],
                                                                 window_length=filter_window_size,
                                                                 polyorder=0)[-1])
                        
                        obj['q_smooth'] = np.array(savgol_filter(x=data['q'],
                                                                 window_length=filter_window_size,
                                                                 polyorder=0)[-1])
                        
                        observer.on_next(obj)
                
                return source.subscribe(
                    on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler
                )
            
            return rx.create(subscribe)
        
        return _savgol
    
    def calc_p_q_diff(self):
        def _calc(source):
            def subscribe(observer, scheduler):
                def on_next(obj):
                    obj['diff'] = obj['p'] - obj['q']
                    obj['diff_smooth'] = obj['p_smooth'] - obj['q_smooth']
                    observer.on_next(obj)
                
                return source.subscribe(
                    on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler
                )
            
            return rx.create(subscribe)
        
        return _calc
    
    def mark_sign_change_points(self):
        def _mark(source):
            def subscribe(observer, scheduler):
                predecessor = [0]
                
                def on_next(obj):
                    nonlocal predecessor
                    current_sign = np.sign(obj['diff_smooth'])
                    
                    if predecessor[0] == 0:
                        predecessor[0] = current_sign
                    
                    if predecessor[0] != current_sign:
                        obj["change_point"] = True
                    else:
                        obj["change_point"] = False
                    
                    predecessor[0] = current_sign
                    observer.on_next(obj)
                
                return source.subscribe(
                    on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler
                )
            
            return rx.create(subscribe)
        
        return _mark