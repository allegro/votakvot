import logging
import threading
import sched

from typing import Iterable, Optional

import prometheus_client as pc

from votakvot import core, hook


logger = logging.getLogger(__name__)


class _SchedulerThread(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(
            self,
            daemon=True,
            name="votakvot-prometheus-bridge",
        )
        self.sched = sched.scheduler()
        self.cond = threading.Condition()
        self.start()

    def repeat(self, period, callback, priority=0):
        def doit():
            try:
                if callback():
                    self.sched.enter(period, priority, doit)
            except Exception:
                logger.exception("unhandled exception")

        doit()
        with self.cond:
            self.cond.notify()

    def run(self):
        while True:
            with self.cond:
                self.cond.wait()
            self.sched.run()


class PrometheusBaseBridgeHook(hook.Hook):

    _sched_thread = _SchedulerThread()

    def __init__(
        self,
        period: float,
        registry: pc.CollectorRegistry,
    ):
        self.registry = registry
        self.period = period

    def on_tracker_start(self, context: core.ATracker):
        logger.info("start exporting prometheus metrics for context %s", context)
        context.__dumper_done = threading.Event()
        self._sched_thread.repeat(self.period, lambda: self._dump_loop(context))

    def on_tracker_finish(self, context: core.ATracker):
        logger.debug("stop exporting metrics for context %s", context)
        context.__dumper_done.set()
        self.do_export(context)

    def on_tracker_infused(self, context: core.ATracker):
        logger.debug("start exporting prometheus metrics for infused context %s", context)
        self.on_tracker_start(context)

    def _dump_loop(self, context: core.ATracker):
        done = context.__dumper_done
        if not done.is_set():
            logger.info("export metrics for context %s", context)
            self.do_export(context)
            return True

    def do_export(
        self,
        context: 'core.Tracker',
    ):
        raise NotImplementedError


class PrometheusDumper(PrometheusBaseBridgeHook):
    def __init__(
        self,
        format: str = 'jsonl',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.format = format

    def _fmt_sample(self, sample):
        if not sample.labels:
            name = sample.name
        else:
            labels = sample.labels
            labels_str = "|".join(f"{k}={labels[k]}" for k in sorted(labels))
            name = f"{sample.name}[{labels_str}]"
        return name, sample.value

    def do_export(
        self,
        context: core.ATracker,
    ):
        metrics = self.registry.collect()
        d = dict(
            self._fmt_sample(s)
            for m in metrics
            for s in m.samples
        )
        context.meter('prometheus', d, format=self.format)


def _as_registry(metrics, registry):
    assert not registry or not metrics, "Parameters 'registry' and 'metrics' are mutually exclusive"
    if registry is not None:
        return registry
    elif metrics:
        registry = pc.CollectorRegistry()
        for m in metrics:
            registry.register(m)
        return registry
    else:
        return pc.REGISTRY


def capture_prometheus_metrics(
    metrics: Iterable[pc.Metric] = (),
    period: float = 20,
    registry: Optional[pc.CollectorRegistry] = None,
) -> hook.Hook:
    return PrometheusDumper(
        period=period,
        registry=_as_registry(metrics, registry),
    )
