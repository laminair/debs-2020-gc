import rx
import pandas as pd


class DataTransformation():
    
    def transform_to_pandas(self):
        def _transform(source):
            def subscribe(observer, scheduler):
                def on_next(obj):
                    df = pd.DataFrame.from_dict(obj["records"])

                    obj_2 = {
                        "i": obj['i'],
                        "records": df
                    }

                    if "start_time" in obj.keys():
                        obj_2["start_time"] = obj["start_time"]
                    
                    observer.on_next(obj_2)
                
                return source.subscribe(
                    on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler
                )
            
            return rx.create(subscribe)
        
        return _transform

    def remove_payload(self, keys):
    
        assert type(keys) == list
    
        def _rm(source):
            def subscribe(observer, scheduler):
            
                def on_next(obj):
                
                    if type(obj) is not dict:
                        observer.on_next(obj)
                    else:
                        for key in keys:
                            obj.pop(key, None)
                    
                        observer.on_next(obj)
            
                return source.subscribe(
                    on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler
                )
        
            return rx.create(subscribe)
    
        return _rm