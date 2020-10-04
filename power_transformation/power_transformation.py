import numpy as np
import rx

import project_settings as settings

class ElectricTransformation():
    
    def active_power(self):
        def _calc(source):
            def subscribe(observer, scheduler):
            
                def on_next(obj):
                
                    if not "records" in obj.keys():
                        observer.on_error("Input tuple must contain pandas data frame.")
                
                    voltage = np.array(obj["records"]['voltage'].values).flatten()
                    current = np.array(obj["records"]['current'].values).flatten()
                
                    if not len(voltage) == len(current):
                        observer.on_error(f"Invalid current and voltage input for data point {obj['i']}")
                    else:
                        power = voltage * current
                        p_len = settings.PERIOD_LENGTH
                    
                        p = []
                        for i in range(0, len(power), p_len):
                            if i + p_len <= len(power):
                                p_sig = power[i:int(i + p_len)]
                                p.append(np.mean(p_sig))
                    
                        obj['p'] = np.array(p)
                        obj['p_log'] = np.log(p)
                        observer.on_next(obj)
            
                return source.subscribe(
                    on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler
                )
        
            return rx.create(subscribe)
    
        return _calc

    def apparent_power(self):
        def _calc(source):
            def subscribe(observer, scheduler):
            
                def on_next(obj):
                
                    if not "records" in obj.keys():
                        observer.on_error("Input tuple must contain pandas data frame.")
                
                    voltage = np.array(obj["records"]['voltage'].values).flatten()
                    current = np.array(obj["records"]['current'].values).flatten()
                
                    if not len(voltage) == len(current):
                        observer.on_error(f"Invalid current and voltage input for data point {obj['i']}")
                    else:
                        rms_v = self.calculate_rms(voltage)
                        rms_c = self.calculate_rms(current)
                    
                        obj['s'] = rms_v * rms_c
                    
                        observer.on_next(obj)
            
                return source.subscribe(
                    on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler
                )
        
            return rx.create(subscribe)
    
        return _calc

    def reactive_power(self):
        def _calc(source):
            def subscribe(observer, scheduler):
            
                def on_next(obj):
                
                    if not 'p' in obj.keys() and not 's' in obj.keys():
                        observer.on_error("Input tuple must contain P & S to calculate Q.")
                
                    if not len(obj['p']) == len(obj['s']):
                        observer.on_error(f"Invalid P & S input for data point {obj['i']}")
                    else:
                        obj['q'] = np.sqrt(np.square(obj['s']) - np.square(obj['p']))
                        obj['q_log'] = np.log(obj['q'])
                        observer.on_next(obj)
            
                return source.subscribe(
                    on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler
                )
        
            return rx.create(subscribe)
    
        return _calc

    def calculate_rms(self, dim):
    
        rms = []
        for i in range(0, len(dim), settings.PERIOD_LENGTH):
            if i + settings.PERIOD_LENGTH <= len(dim):
                p_sig = dim[i:int(i + settings.PERIOD_LENGTH)]
                rms_1 = np.sqrt(np.mean(np.square(p_sig)))
                rms.append(rms_1)
        return np.array(rms)