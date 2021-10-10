"""Votakvot -- simple tool for tracking information during code testing and researching."""
from __future__ import annotations

import contextlib
import datetime
import logging
import os
import pickle
import traceback
import typing
import uuid
import contextvars

from functools import cached_property
from typing import Callable, Dict, Iterable, List, Optional

import fsspec
import pandas as pd

import votakvot
import votakvot.data
import votakvot.metrics
import votakvot.hook

from votakvot.data import FancyDict, dump_yaml_file, path_fs


logger = logging.getLogger(__name__)


T = typing.TypeVar('T')


class ATracker(typing.Protocol):

    uid: str | None
    tid: str | None

    def attach(self, name: str, mode: str, **kwargs) -> fsspec.core.OpenFile:
        ...

    def inform(self, **kwargs) -> None:
        ...

    def call(self, tid: str, func: Callable[..., T], params: Dict) -> T:
        ...

    def meter(self, series: str | None, metrics: Dict, format: str | None) -> None:
        ...

    def flush(self) -> None:
        ...

    def snapshot(self) -> None:
        ...


tracker_var = contextvars.ContextVar("votakvot.core.tracker_var")

def current_tracker() -> ATracker:
    return tracker_var.get(None) or NopeTracker()


@contextlib.contextmanager
def with_tracker(tracker: ATracker):
    try:
        logger.debug("enter tracker %s", tracker)
        t = tracker_var.set(tracker)
        if isinstance(tracker, InfusedTracker):
            tracker.activate()
        yield
    finally:
        logger.debug("exit tracker %s", tracker)
        tracker.flush()
        tracker_var.reset(t)


def set_global_tracker(tracker: ATracker):
    assert tracker_var.get(None) is None, "ATracker is already configured"
    tracker_var.set(tracker)
    if isinstance(tracker, InfusedTracker):
        tracker.activate()


class NopeTracker(ATracker):

    uid = None
    tid = None

    def attach(self, name, mode='w', **kwargs):
        logger.info("attach: %s", name)
        if 'r' in mode:
            raise FileNotFoundError(f"attachement {name} is not available")
        return path_fs("file").open(os.devnull, **kwargs)

    def inform(self, **kwargs):
        logger.info("info: %s", kwargs)

    def call(self, tid, func, params):
        logger.info("call %s: %s -> %s", func, tid, params)
        func = _desuspect_func(func)
        return func(**params)

    def snapshot(self):
        logger.debug("snapshot - do nothing")

    def meter(self, series: Optional[str], metrics: Dict, format: str):
        for k, v in metrics.items():
            logger.debug("metric[%s] %s = %s", series, k, v)


class BaseTracker(NopeTracker):

    def __init__(self, path, uid, tid, metrics, hook=None):
        self.path = path
        self.uid = uid
        self.tid = tid
        self.metrics = metrics
        self.hook = votakvot.hook.coerce_to_hook(hook)

    def attach(self, name, mode='w', autocommit='onclose', **kwargs):
        fn = f"{self.path}/{name}"
        logger.debug("open attachement %s (resolved to %s)", name, fn)
        if 'w' in mode and autocommit == 'onclose':
            # f = path_fs(fn).open(fn, mode=mode, autocommit=False, **kwargs)
            f = path_fs(fn).open(fn, mode=mode, autocommit=True, **kwargs)
            logger.debug("wrap file object for an autocommit")
            return votakvot.data.AutoCommitableFileWrapper(f)
        else:
            return path_fs(fn).open(fn, mode=mode, autocommit=autocommit, **kwargs)

    def meter(self, series: Optional[str], metrics: Dict, format=None):
        self.metrics.meter(series, metrics, format)


class Tracker(BaseTracker):

    def __init__(self, path, meta, func, params, tid, hook):
        BaseTracker.__init__(
            self,
            path=path,
            tid=tid,
            uid=uuid.uuid1().hex,
            hook=hook,
            metrics=votakvot.metrics.MetricsExporter(),
        )
        self.func = func
        self.params = params
        self.info = {}
        self.data = FancyDict()
        self.meta = meta
        self.iter = None

    def inform(self, **kwargs):
        for k in self.info.keys() & kwargs.keys():
            if self.keys[k] != kwargs[k]:
                logger.warning("overwrite informed field %r: %r -> %r", k, self.info[k], kwargs[k])
        self.info.update(kwargs)

    def dump_trial(self):
        self.hook.trial_presave(self)
        with self.attach("votakvot.yaml", mode='wt') as f:
            votakvot.data.dump_yaml_file(f, self.data)

    def snapshot(self):
        if self.iter is None:
            raise RuntimeError("function `snapshot` can be used only from tracked iterator")
        self.dump_snapshot()

    def dump_snapshot(self):
        self.flush()
        logger.debug("dump snapshot for %s", self.func)
        with self.attach("snapshot.pickle", 'wb') as f:
            pickle.dump(self, f)

    def load_snapshot(self):
        try:
            logger.debug("loading snapshot for %s", self.func)
            with self.attach("snapshot.pickle", 'rb') as f:
                other = pickle.load(f)
        except FileNotFoundError:
            logger.debug("snapshot not found")
            return False
        except Exception:
            logger.exception("failed to load snapshot")
            return False
        logger.debug("resume %s from snapshot", self.func)

        if self.params != other.params:
            raise RuntimeError("snapshot has mismatched params", self.params, other.params)

        other = dict(other.__dict__)
        other.pop('path', None)
        other.pop('hook', None)
        self.__dict__.update(other)

        return True

    def prerun(self):
        if self.load_snapshot():
            return

        self.data = FancyDict({
            'votakvot': votakvot.__version__,
            'tid': self.tid,
            'uid': self.uid,
            'meta': self.meta,
            'at': FancyDict(
                created=datetime.datetime.now(),
            ),
            'params': FancyDict(
                (k, v)
                for k, v in self.params.items()
                if not k.startswith("_")
            ),
            'state': 'wait',
        })

        self.hook.context_init(self)
        self.dump_trial()

    def _runfunc(self):

        if self.iter is None:
            r = self.func(**self.params)
        else:
            r = self.iter  # resumed

        if isinstance(r, Iterable):
            self.iter = iter(r)
            for x in self.iter:
                if x is not None:
                    return x
        else:
            return r

    def run(self):

        if self.iter:
            self.data.state = 'resumed'
            self.data.at.resumed = datetime.datetime.now()
        else:
            self.data.state = 'running'
            self.data.at.started = datetime.datetime.now()

        self.data.info = self.info
        self.dump_trial()

        with with_tracker(self):
            self.hook.trial_started(self)

            try:
                started = datetime.datetime.now()
                result = self._runfunc()
                finished = datetime.datetime.now()
            except BaseException as e:
                finished = datetime.datetime.now()
                self.data.state = 'fail'
                self.data.error = repr(e)
                with self.attach("traceback.txt") as f:
                    traceback.print_exc(file=f)
            else:
                self.data.state = 'done'
                self.data.result = result
                if 'error' in self.data:
                    del self.data['error']

            self.hook.trial_finished(self)
            self.metrics.flush()

        self.data.at.finished = datetime.datetime.now()
        self.data.at.duration = (finished - started).microseconds * 0.000001

        self.dump_trial()

    def infused_tracker(self) -> 'InfusedTracker':
        return InfusedTracker(
            path=self.path,
            tid=self.tid,
            hook=self.hook,
        )

    def flush(self):
        logger.debug("flush tracking contxt %s", self)
        self.dump_trial()
        self.metrics.flush()


class InfusedTracker(BaseTracker):

    def __init__(self, path, tid, hook):
        uid = uuid.uuid1().hex
        BaseTracker.__init__(
            self,
            path=path,
            uid=uid,
            tid=tid,
            hook=hook,
            metrics=votakvot.metrics.MetricsExporter(add_uuid=uid),
        )
        self.info = FancyDict()
        self.info_path = f"votakvot-{uid}.yaml"

    def inform(self, **kwargs):
        for k in self.info.keys() & kwargs.keys():
            if self.info[k] != kwargs[k]:
                logger.warning("overwrite informed field %r: %r -> %r", k, self.info[k], kwargs[k])
        self.info.update(kwargs)
        with self.attach(self.info_path, mode='wt') as f:
            dump_yaml_file(f, {
                'at': datetime.datetime.now(),
                'info': self.info,
            })

    def activate(self):
        logger.info("activate infused tracker for %s", self.path)
        self.hook.context_infused(self)

    def flush(self):
        logger.info("flush infused tracker %s", self)
        self.metrics.flush()


def _desuspect_func(f):
    # get original function when it is wrapped by `@votakvot.track`
    return getattr(f, '_votakvot_origin', f)


class Trial:

    def __init__(
        self,
        path,
        _fs=None,
    ):
        self.path = path
        self._fs = _fs or path_fs(path)

    def reload(self):
        p = self.path
        self.__dict__.clear()
        self.path = p

    def attach(self, name, mode='rb', **kwargs):
        p = f"{self.path}/{name}"
        return self._fs.open(p, mode=mode, **kwargs)

    @cached_property
    def attached(self) -> List[str]:
        gs = self._fs.glob(f"{self.path}/**")
        gss = [os.path.relpath(x, self.path) for x in gs]
        gss.remove("votakvot.yaml")
        return gss

    @cached_property
    def data(self):
        with self.attach("votakvot.yaml") as f:
            return votakvot.data.load_yaml_file(f)

    @property
    def tid(self):
        return self.data.tid

    @property
    def uid(self):
        return self.data.uid

    @property
    def meta(self):
        return self.data.meta

    @property
    def params(self):
        return self.data.params

    @property
    def info(self):
        return self.data.info

    @property
    def status(self):
        return self.status

    @property
    def result(self):
        if 'error' in self.data:
            try:
                logger.error("Error: %s", self.attach('traceback.txt', mode='tr').read())
            except Exception:
                pass
            raise RuntimeError(f"Trial was failed with error: {self.data.error}")
        return self.data.get('result')

    def load_metrics(self) -> pd.DataFrame:
        return votakvot.metrics.load_metrics(self)

    @cached_property
    def data_plain(self):
        r = votakvot.data.plainify_dict(self.data)
        r.pop('votakvot', None)
        return r

    def __repr__(self):
        return f"<Trial {self.uid!r}>"

    def __eq__(self, other):
        return self.uid == other.uid

    def __hash__(self):
        return hash(self.uid)
