import requests
import project_settings as settings
from datetime import datetime
import time
import socket
import json
import rx
import math


class HttpConnector():
    def __init__(self, protocol, host, port, path):
        self.protocol = protocol
        self.host = host
        self.port = int(port)
        self.path = path
        self.url = f"{self.protocol}://{self.host}:{self.port}"
        self.link = f"{self.protocol}://{self.host}:{self.port}/{self.path}"
        self.start_time_dict = {}
        
    def connect(self, observer, scheduler=None):
        self.wait_for_data_source()
        stop = False

        while not stop:
            try:
                req = requests.get(self.link, timeout=600)
                observer.on_next(req)
                
                if req.status_code == 404:
                    print("404 observed!")
                    observer.on_completed()
                    stop = True
                    break
                
                if req.status_code == 500:
                    observer.on_completed()
                    stop = True
                    break
                
            except requests.HTTPError as e:
                observer.on_next(f"HTTP error occurred when fetching input tuples. {e}")
            except requests.ConnectionError as e:
                observer.on_next(f"Could not connect to input tuple source. {e}")
            except requests.RequestException as e:
                observer.on_next(f"Error processing input tuple request. {e}")
                
    def submit(self, x):
        headers = {'Content-type': 'application/json'}
        req = requests.post(self.link, json=x, headers=headers)
        print(f"Status code: {req.status_code}: {x}")

    def wait_for_data_source(self):
        """
        Initializes a waiting pattern to ensure proper system functionality. Works as a process blocker while unable to
        establish a connection to the input host.
        :return: None.
        """
        start = time.time()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
        while start + settings.HTTP_TIMEOUT > time.time():
            try:
                sock.connect_ex((self.host, self.port))
                sock.shutdown(2)
                print("Connection to input tuple source established.")
                return
            except:
                time.sleep(5)
    
        print("No input connection found. Please check configuration.")

    def transform_http_request(self):
        def _transform(source):
            def subscribe(observer, scheduler=None):
            
                def on_next(obj):
                
                    if obj.status_code != 200 and obj.status_code != 404:
                        err = f"Error transforming request due to code {obj.status_code}.", obj.status_code
                        print(err)
                        observer.on_error(err)
                        observer.on_completed()
                    elif obj.status_code == 404:
                        self.result = obj.content
                        observer.on_completed()
                    else:
                        try:
                            obj = json.loads(obj.content)["records"]
                            idx = int(obj[0]['i'] / 1000)
                            res = {
                                'i': idx,
                                'records': obj
                            }
                            self.start_time_dict[idx] = time.time()
                            observer.on_next(res)
                        except:
                            observer.on_completed()
            
                return source.subscribe(
                    on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler
                )
        
            return rx.create(subscribe)
    
        return _transform
