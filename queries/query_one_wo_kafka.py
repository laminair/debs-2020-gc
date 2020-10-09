from kafka_connector.consumer import KafkaConsumer
from kafka_connector.producer import KafkaProducer
from input_sorting.separate_payload_from_kafka_message import split_payload
from input_sorting.transform_data_structure import DataTransformation
from power_transformation.power_transformation import ElectricTransformation
from algorithms.barsim_algorithm import BarsimAlgorithm

from benchmark.latency_measurement import LatencyBenchmark
from http_connector.http_connector import HttpConnector

import rx


class QueryOneWOKafka():
    
    def __init__(self):
        self.et = ElectricTransformation()
        self.dt = DataTransformation()
        self.bf = BarsimAlgorithm(
            q_size=100,
        )
        self.bm = LatencyBenchmark()
        self.kp = KafkaProducer()
        self.hc = HttpConnector(protocol="HTTP", host="localhost", port=8000, path="data/1/")
        
    def run(self):
        rx.create(
            lambda o, s: self.hc.connect(observer=o, scheduler=s)
        ).pipe(
            self.hc.transform_http_request(),
            self.dt.transform_to_pandas(),
            self.et.active_power(),
            self.et.apparent_power(),
            self.et.reactive_power(),
            self.dt.remove_payload(keys=["records"]),
            self.bf.build_window(),
            self.bf.predict(),
            self.bf.check_event_model_constraints(0.8),
            self.bf.compute_loss(),
            self.bf.process_detected_event(),
            self.bf.prepare_result(),
            # self.bm.append_kafka_metrics()
        ).subscribe(
            on_next=lambda x: self.hc.submit(x),  # self.bm.calc_mean_latency(x),
            on_error=lambda error: print(error),
            on_completed=lambda: self.kp.show_benchmark_results(start_time_dict=self.hc.start_time_dict)
        )
