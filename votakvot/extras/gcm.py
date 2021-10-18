import logging
import time

from functools import cached_property
from typing import Dict, Iterable, Optional

import google.auth
from google.cloud import monitoring
from google.protobuf.timestamp_pb2 import Timestamp

import prometheus_client as pc

from votakvot import core
from votakvot.data import FancyDict
from votakvot.extras.prometheus import PrometheusBaseBridgeHook, _as_registry


logger = logging.getLogger(__name__)


class PrometheusGCMBridge(PrometheusBaseBridgeHook):

    def __init__(
        self,
        project_id: str,
        period,
        credentials=None,
        extra_labels=None,
        **kwargs,
    ):
        super().__init__(period=period, **kwargs)
        self.project_id = project_id
        self.resource_labels = {'project_id': project_id}
        self.extra_labels = dict(extra_labels or ())
        self.throttle = 15
        self.metric_names = set()
        self.credentials = credentials
        assert period >= self.throttle

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop('client', None)
        return state

    @cached_property
    def client(self):
        return monitoring.MetricServiceClient(credentials=self.credentials)

    def _create_series(self, tracker: core.Context, interval, m, s):

        series = monitoring.TimeSeries()
        name = self._metric_name(m, s)
        self.metric_names.add(name)
        series.metric.type = name

        series.metric.labels.update(s.labels)
        series.metric.labels.update(self.extra_labels)
        series.metric.labels['votakvot__tid'] = tracker.tid

        series.resource.type = 'global'
        series.resource.labels.update(self.resource_labels)

        point = monitoring.Point()
        point.interval = interval
        point.value.double_value = float(s.value)

        series.points = [point]
        return series

    def _timestamp(self, ts) -> Timestamp:
        seconds = int(ts)
        nanos = int((ts - seconds) * 10 ** 9)
        return Timestamp(seconds=seconds, nanos=nanos)

    def _metric_name(self, m, s):
        if m.name == s.name:
            return f"custom.googleapis.com/votakvot/{m.name}"
        else:
            sf: str = s.name
            prefix = m.name + "_"
            if sf.startswith(prefix):
                sf = sf[len(prefix):]
            return f"custom.googleapis.com/votakvot/{m.name}/{sf}"

    def do_export(self, tracker: core.Context):
        now = time.time()
        if (
            not tracker.__first_metrics_export
            and now < tracker.__last_metrics_export + self.throttle
        ):
            return

        metrics = self.registry.collect()

        interval = monitoring.TimeInterval()
        # interval.start_time = self._timestamp(tracker.__last_metrics_export)
        interval.end_time = self._timestamp(now)

        try:
            request = {
                "name": f"projects/{self.project_id}",
                "time_series": [
                    self._create_series(tracker, interval, m, s)
                    for m in metrics
                    for s in m.samples
                ],
            }
            logger.debug("send metrics to gcm: %s", request)
            self.client.create_time_series(request=request)
        except Exception:
            logger.exception("failed to do_export metrics to GCM")

        tracker.__last_metrics_export = now
        tracker.__first_metrics_export = False

    def on_tracker_flush(self, tracker: core.ATracker):
        tracker.meta.setdefault('gcm', FancyDict())
        tracker.meta.gcm.metrics = sorted(self.metric_names)
        super().on_tracker_flush(tracker)

    def on_tracker_infused(self, tracker: core.ATracker):
        tracker.__last_metrics_export = time.time()
        tracker.__first_metrics_export = True
        super().on_tracker_infused(tracker)

    def on_tracker_start(self, tracker: core.ATracker):
        tracker.__last_metrics_export = time.time()
        tracker.__first_metrics_export = True
        tracker.meta.setdefault('gcm', FancyDict())
        tracker.meta.gcm.project = self.project_id
        self.metric_names = set(
            self._metric_name(m, s)
            for m in self.registry.collect()
            for s in m.samples
        )
        super().on_tracker_start(tracker)


def _defult_gcp_project():
    _, project_id = google.auth.default()
    logger.debug("use default GCP project %s", project_id)
    return project_id


def export_metrics_to_gcm(
    metrics: Iterable[pc.Metric] = (),
    project_id: str = None,
    period: float = 60,
    registry: Optional[pc.CollectorRegistry] = None,
    extra_labels: Optional[Dict] = None,
) -> core.TrackingHook:

    project_id = project_id or _defult_gcp_project()
    logger.info("do_export metrics to GCM, project %s", project_id)

    return PrometheusGCMBridge(
        project_id=project_id,
        extra_labels=dict(extra_labels or ()),
        period=period,
        registry=_as_registry(metrics, registry),
    )
