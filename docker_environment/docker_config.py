import time
import socket

import project_settings as settings


def wait_for_kafka(host, port):
    """
    USE THE WAIT FUNCTION IN CASE THE KAFKA PRODUCER IN THIS SERVICE IS DIFFERENT FROM THE INPUT CONNECTION.
    Process blocker to allow kafka to become available. Since Kafka uses a binary TCP connection
    (https://kafka.apache.org/protocol) using a quick socket ping does the job. Once it is able to connect the function
    unblocks.
    :param host: String from settings.KAFKA_HOST
    :param port: Int from settings.KAFKA_POST
    :return: None.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    start = time.time()
    
    while start + settings.HTTP_TIMEOUT > time.time():
        try:
            sock.connect((host, int(port)))
            sock.shutdown(2)
            print("Connection to Kafka established.")
            return
        except:
            time.sleep(5)
    
    print("No Kafka connection found. Please check configuration.")