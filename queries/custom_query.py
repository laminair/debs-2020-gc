from kafka_connector.consumer import KafkaConsumer
from input_sorting.separate_payload_from_kafka_message import split_payload
from input_sorting.transform_data_structure import DataTransformation
from power_transformation.power_transformation import ElectricTransformation
from algorithms.custom_change_point_detector import ChangePointDetector

from benchmark.latency_measurement import LatencyBenchmark

import rx
from rx import operators as ops


class CustomQuery():
    
    def __init__(self):
        self.et = ElectricTransformation()
        self.dt = DataTransformation()
        self.cd = ChangePointDetector(window_size=5)
        self.bm = LatencyBenchmark()
    
    def run(self):
        rx.create(
            lambda o, s: KafkaConsumer().create_subscription(topics=["Input", ], observer=o, scheduler=s)
        ).pipe(
            split_payload(),
            # self.bm.inject_time(label="start_time"),
            self.dt.transform_to_pandas(),
            self.et.active_power(),
            self.et.apparent_power(),
            self.et.reactive_power(),
            self.dt.remove_payload(keys=["records"]),
            self.cd.apply_filter(filter_class="sma", q_size=20),
            self.cd.apply_filter(filter_class="sma", q_size=10),
            self.cd.calc_p_q_diff(),
            self.cd.mark_sign_change_points(),
            # self.bm.get_latency(label="start_time"),
            # self.cd.produce_result()
        ).subscribe(
            on_next=lambda x: print(f"{x['i']}, {int(x['change_point'])}, {int(x['p_smooth'])}, {int(x['q_smooth'])}"),
            # print(f"{x['i']}, {int(x['change_point'])}, {int(x['p_smooth'])}, {int(x['q_smooth'])}, {int(x['p'])}, {int(x['q'])}"),
            on_error=lambda error: print(error),
            on_completed=lambda: print("Query 1 done!")
        )