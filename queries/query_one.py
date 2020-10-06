from kafka_connector.consumer import KafkaConsumer
from input_sorting.separate_payload_from_kafka_message import split_payload
from input_sorting.transform_data_structure import DataTransformation
from power_transformation.power_transformation import ElectricTransformation
from algorithms.barsim_algorithm import BarsimAlgorithm

from benchmark.latency_measurement import LatencyBenchmark

import rx


class QueryOne():
    
    def __init__(self):
        self.et = ElectricTransformation()
        self.dt = DataTransformation()
        self.bf = BarsimAlgorithm(
            q_size=100,
        )
        self.bm = LatencyBenchmark()
    
    def run(self):
        rx.create(
            lambda o, s: KafkaConsumer().create_subscription(topics=["Input", ], observer=o, scheduler=s)
        ).pipe(
            split_payload(),
            # self.bm.inject_time("start_time"),
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
            # self.bm.get_latency("start_time")
        ).subscribe(
            on_next=lambda x: x,
            on_error=lambda error: print(error),
            on_completed=lambda: print("Query 1 done!")
        )
