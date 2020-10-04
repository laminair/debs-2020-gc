from queue import Queue
import pandas as pd
import numpy as np

from sklearn.utils.validation import check_is_fitted
from sklearn import utils
from sklearn.cluster import DBSCAN

import rx
import itertools
import math

class BarsimAlgorithm():
    
    def __init__(self, q_size):
        self.reset_window = False
        self.q_size = q_size
        self.q = Queue(maxsize=self.q_size)
        self.is_fitted = True
        self.clustering = {}
        self.backward_clustering_structure = {}
    
    def build_window(self):
        def _build(source):
            def subscribe(observer, scheduler):
                
                def on_next(obj):
                    nonlocal self
                    
                    if self.reset_window or self.q.full():
                        self.q = Queue(maxsize=self.q_size)
                        self.reset_window = False
                        self.q.put_nowait(obj)
                    else:
                        self.q.put_nowait(obj)
                        
                    df = pd.DataFrame(self.q.queue)
                        
                    observer.on_next(df)

                return source.subscribe(
                    on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler
                )

            return rx.create(subscribe)
        return _build
    
    def predict(self):
        def _predict(source):
            def subscribe(observer, scheduler):
                def on_next(obj):
                    nonlocal self
                    X = obj[["p_log", "q_log"]]
                    check_is_fitted(self, ["is_fitted"])
                    utils.assert_all_finite(X)
                    X = utils.as_float_array(X)
                    self._update_clustering(X)
                    
                    obj = {
                        "i_min": np.min(obj[["i"]]),
                        "i_max": np.max(obj[["i"]]),
                        "cluster": self.clustering,
                        "X": X
                    }

                    observer.on_next(obj)

                return source.subscribe(
                    on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler
                )

            return rx.create(subscribe)
        return _predict
    
    def check_event_model_constraints(self, temp_eps):
        def _check(source):
            def subscribe(observer, scheduler):
                def on_next(obj):
                    nonlocal self
                    recent_idx = int(obj["i_max"])
                    
                    checked_clusters = self._check_model_constraints(obj=obj, temp_eps=0.8)
                    
                    if checked_clusters:
                        # If a checked cluster has been found the backward step is triggered.
                        obj["approved_clusters"] = checked_clusters
                        # Trigger to reset the moving window since an event has been detected.
                        self.reset_window = True
                        observer.on_next(obj)
                    else:
                        observer.on_next({"s": int(recent_idx), "d": False, "s_event":-1})

                return source.subscribe(
                    on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler
                )
            return rx.create(subscribe)
        return _check
        
    def compute_loss(self):
        def _compute(source):
            def subscribe(observer, scheduler):
                def on_next(obj):
                    if "approved_clusters" in obj.keys():
                        loss = self._compute_loss(obj=obj, loss_threshold=40)
                        if loss:
                            obj["cluster_combination"] = loss
                            # Trigger used to reset the sliding window since an event has been detected.
                            self.reset_window = True
                            observer.on_next(obj)
                        else:
                            observer.on_next({"s": obj["i_max"], "d": False, "s_event": -1})
                    else:
                        observer.on_next(obj)
                        
                return source.subscribe(
                    on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler
                )
            return rx.create(subscribe)
        return _compute

    def process_detected_event(self):
        def _process(source):
            def subscribe(observer, scheduler):
                def on_next(obj):
                    nonlocal self
                    
                    if "cluster_combination" in obj.keys():
                        X = obj["X"]
                        self.backward_cluster_structure = self.clustering
                        cluster_combination_balanced = obj["cluster_combination"]
                        
                        for i in range(1, len(X) - 1):
                            X_new = X[i:]
                            self._update_clustering(X_new)  # Pushes the new clustering structure into self.clustering
                            checked_clusters = self._check_model_constraints(obj=obj, temp_eps=0.8)
                            if not checked_clusters:
                                status = "break"
                                cluster_combination_balanced = self._rollback(
                                    status=status,
                                    cluster_combination_balanced=cluster_combination_balanced,
                                    i=i
                                )
                                break
                            else:
                                cluster_combination_below_loss = self._compute_loss(obj, loss_threshold=40)
                                if not cluster_combination_below_loss:
                                    status = "break"
                                    cluster_combination_balanced = self._rollback(
                                        status=status,
                                        cluster_combination_balanced=cluster_combination_balanced,
                                        i=i
                                    )
                                    break
                                else:
                                    status = "continue"
                                    cluster_combination_balanced = self._rollback(
                                        status=status,
                                        cluster_combination_balanced=cluster_combination_balanced,
                                        cluster_combination_below_loss=cluster_combination_below_loss,
                                        i=i
                                    )
                                    continue
                                
                        event_start = cluster_combination_balanced[2][0]
                        event_end = cluster_combination_balanced[2][-1]
                        obj["event_edges"] = (event_start, event_end)
                    
                    observer.on_next(obj)
                    
                return source.subscribe(
                    on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler
                )
            return rx.create(subscribe)
        return _process
    
    def prepare_result(self):
        def _prep(source):
            def subscribe(observer, scheduler):
                def on_next(obj):
                    if "event_edges" in obj.keys():
                        mean_idx = math.floor((obj["event_edges"][0] + obj["event_edges"][1]) / 2)
                        start_idx = obj["i_min"]
                        event_idx = int(start_idx + mean_idx + 1)
                        
                        observer.on_next({"s": int(obj["i_max"]), "d": True, "s_event": event_idx + 1})
                    else:
                        observer.on_next(obj)

                return source.subscribe(
                    on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler
                )
            return rx.create(subscribe)
        return _prep
    
    def fit(self):
        self.is_fitted = True
        
    def _update_clustering(self, X):
        dbscan = DBSCAN(eps=0.03, min_samples=2, n_jobs=-1).fit(X)
        cluster_labels = np.array(dbscan.labels_)
        unique_labels = np.unique(cluster_labels)
        
        struct = {}
        for cluster in unique_labels:
            partial_struct = {}
            members = np.where(cluster_labels == cluster)[0]
            indices = partial_struct["indices"] = np.array(members)
            u = partial_struct["u"] = np.min(indices)
            v = partial_struct["v"] = np.max(indices)
            partial_struct["loc"] = len(indices) / (v-u + 1)
            struct[cluster] = partial_struct
            
        self.clustering = struct
        return None
    
    def _check_model_constraints(self, obj, temp_eps):
        recent_idx = int(obj["i_max"])
        cluster_len = len(obj["cluster"])
    
        # Check Barsim and Yang's first model constraint: At least 3 clusters including a noise cluster.
        if cluster_len > 2 and -1 in obj["cluster"].keys():
            cluster_two = {}
            for cluster_key, cluster in obj["cluster"].items():
                if cluster_key != -1:
                    if cluster["loc"] >= 1 - temp_eps:
                        cluster_two[cluster_key] = cluster
            if len(cluster_two) >= 2:
                cluster_combinations = itertools.combinations(cluster_two, 2)  # Combinations w/o replacement
                approved_clusters = []
                for i, j in cluster_combinations:
                    if cluster_two[i]["u"] < cluster_two[j]["u"]:
                        c1 = i
                        c2 = j
                    else:
                        c1 = j
                        c2 = i
                
                    if cluster_two[c1]["v"] < cluster_two[c2]["u"]:
                        c0_indices = obj["cluster"][-1]["indices"]
                        value_condition = (
                                (c0_indices > cluster_two[c1]["v"]) &
                                (c0_indices < cluster_two[c2]["u"])
                        )
                        interval_indices = c0_indices[value_condition]
                        if len(interval_indices) != 0:
                            approved_clusters.append((c1, c2, interval_indices))
            
                if len(approved_clusters) > 0:
                    return approved_clusters
                
        return None
    
    def _compute_loss(self, obj, loss_threshold=40):
        loss_list = []
        for c1, c2, interval_indices in obj["approved_clusters"]:
            u = interval_indices[0] - 1
            v = interval_indices[-1] + 1
            c1 = obj["cluster"][c1]["indices"]
            c2 = obj["cluster"][c2]["indices"]
            a = len(np.where(c1 >= v)[0])
            b = len(np.where(c2 <= u)[0])
            indices = np.concatenate([c1, c2])
            c = len(np.where((indices > u) & (indices < v))[0])
            
            model_loss = a + b + c
            loss_list.append(model_loss)
        
        # Returns the minimum loss index.
        min_loss = np.argmin(loss_list)
        
        if loss_list[int(min_loss)] <= loss_threshold:
            """
            Starting point for the backward pass since an event has been detected.
            """
            return obj["approved_clusters"][int(min_loss)]

        return None
    
    def _rollback(self, status, cluster_combination_balanced, i, cluster_combination_below_loss=None):
        
        if status == "break":
            cluster_combination_balanced = list(cluster_combination_balanced)
            cluster_combination_balanced[2] = cluster_combination_balanced[2] + i
            cluster_combination_balanced = tuple(cluster_combination_balanced)
            
            for idx, struct in self.backward_clustering_structure.items():
                struct["indices"] = struct["indices"] + int(i - 1)
                struct["u"] = struct["u"] + int(i - 1)
                struct["v"] = struct["v"] + int(i - 1)
                
                self.backward_clustering_structure[idx] = struct
                
            return cluster_combination_balanced
            
        elif status == "continue":
            self.backward_clustering_structure = self.clustering
            cluster_combination_balanced = cluster_combination_below_loss
            return cluster_combination_balanced
        
        else:
            raise IndexError("Status code not in available choices. Either choose {break} or {continue}.")
        