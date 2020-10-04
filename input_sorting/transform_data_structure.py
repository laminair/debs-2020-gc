import rx
import pandas as pd


class DataTransformation():
    
    def transform_to_pandas(self):
        def _transform(source):
            def subscribe(observer, scheduler):
                def on_next(obj):
                    df = pd.DataFrame.from_dict(obj["records"])
                    
                    obj = {
                        "i": obj['i'],
                        "records": df
                    }
                    
                    observer.on_next(obj)
                
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