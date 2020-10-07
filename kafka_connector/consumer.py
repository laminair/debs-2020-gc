from kafka import KafkaConsumer as KC, errors
from docker_environment.docker_config import wait_for_kafka

import json


class KafkaConsumer(KC):
    
    def __init__(self):
        wait_for_kafka("localhost", 9092)
        super(KafkaConsumer, self).__init__(
            group_id=None,
            bootstrap_servers=['localhost:9092'],
            enable_auto_commit=True,
            auto_commit_interval_ms=3 * 1000,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            key_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=3 * 1000
        )
        
    def subscribe(self, topics=(), pattern=None, listener=None):
        self._subscription.subscribe(topics=topics, pattern=pattern, listener=listener)
    
        if pattern is not None:
            self._client.cluster.need_all_topic_metadata = True
            self._client.set_topics([])
            self._client.cluster.request_update()
        else:
            self._client.cluster.need_all_topic_metadata = False
            self._client.set_topics(self._subscription.group_subscription())
            
    def create_subscription(self, topics, observer, scheduler=None):
        self.subscribe(topics=topics)

        try:
            for msg in self:
                observer.on_next(msg)
                
            observer.on_completed()

        except errors.BrokerNotAvailableError as e:
            err = "No Kafka Broker available. Please check your config."
            observer.on_error(err)
        except errors.KafkaConfigurationError as e:
            err = f"Error connecting to Kafka. Details: {e}"
            observer.on_error(err)
        except errors.KafkaTimeoutError as e:
            err = f"Connection to Kafka timed out. Details: {e}"
            observer.on_error(err)
        except errors.KafkaError as e:
            err = f"General Kafka Error. Details: {e}"
            observer.on_error(err)
