"""Votakvot -- simple tool for tracking information during code testing and researching."""
from __future__ import annotations

import datetime
import logging
import os
import pickle
import traceback
import typing
import uuid

from functools import cached_property
from typing import Any, Dict, Iterable, Iterator, List, Optional

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

    def meter(self, metrics: Dict, series: str | None = None, format: str | None = None) -> None:
        ...

    def flush(self) -> None:
        ...

    def activate(self) -> None:
        ...


class NopeTracker(ATracker):

    uid = None
    tid = None

    def attach(self, name, mode='w', **kwargs):
        logger.warning("skip attach: %s", name)
        if 'r' in mode:
            raise FileNotFoundError(f"attachement {name} is not available")
        return path_fs("file").open(os.devnull, mode=mode, **kwargs)

    def inform(self, **kwargs):
        logger.info("info: %s", kwargs)

    def meter(self,  metrics: Dict, series=None, format=None):
        for k, v in metrics.items():
            logger.debug("metric[%s] %s = %s", series, k, v)

    def flush():
        pass

    def activate(self):
        pass


_nope_tracker = NopeTracker()


class _BaseTracker(ATracker):

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

    def meter(self, metrics, series=None, format=None):
        self.metrics.meter(metrics, series or "", format)

    def activate(self):
        pass


class Tracker(_BaseTracker):

    def __init__(self, path, meta, tid, hook=None):
        _BaseTracker.__init__(
            self,
            path=path,
            tid=tid,
            uid=uuid.uuid1().hex,
            hook=hook,
            metrics=votakvot.metrics.MetricsExporter(self),
        )
        self.info = {}
        self.data = FancyDict()
        self.meta = meta

        # support snapshottable fns
        self.iter = None

    def inform(self, **kwargs):
        for k in self.info.keys() & kwargs.keys():
            if self.keys[k] != kwargs[k]:
                logger.warning("overwrite informed field %r: %r -> %r", k, self.info[k], kwargs[k])
        self.info.update(kwargs)

    def snapshot(self):
        if self.iter is None:
            raise RuntimeError("function `snapshot` can be used only from tracked iterator")
        self.dump_snapshot()

    def dump_snapshot(self):
        self.flush()
        logger.debug("dump snapshot")
        with self.attach("snapshot.pickle", 'wb') as f:
            pickle.dump(self, f)

    def load_snapshot(self):
        try:
            logger.debug("loading snapshot for")
            with self.attach("snapshot.pickle", 'rb') as f:
                other = pickle.load(f)
        except FileNotFoundError:
            logger.debug("snapshot not found")
            return False
        except Exception:
            logger.exception("failed to load snapshot")
            return False
        logger.debug("resume from snapshot")

        if self.params != other.params:
            raise RuntimeError("snapshot has mismatched params", self.params, other.params)

        other = dict(other.__dict__)
        other.pop('path', None)
        other.pop('hook', None)
        self.__dict__.update(other)

        return True

    def start(self, params: Dict):
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
                for k, v in params.items()
                if not k.startswith("_")
            ),
            'state': 'started',
            'info': self.info,
        })
        self.hook.on_tracker_start(self)
        self.flush(metrics=False)

    def _runfunc(self, func, params):
        if self.iter is None:
            r = func(**params)
        else:
            r = self.iter  # resumed

        if isinstance(r, Iterator):
            self.iter = r
            for x in self.iter:
                if x is not None:
                    return x
        else:
            return r

    def run(self, fn, /, **params):

        # FIXME
        self.params = params
        #self.func = fn

        if self.load_snapshot():
            self.data.state = 'resumed'
            self.data.at.resumed = datetime.datetime.now()
        else:
            self.start(params)
            self.data.state = 'running'
            self.data.at.started = datetime.datetime.now()

        self.flush(metrics=False)

        try:
            result = self._runfunc(fn, params)
        except BaseException as e:
            self.finish(None, exc=e)
        else:
            self.finish(result)

    def _finish_fail(self, exc):
        self.data.state = 'fail'
        self.data.error = repr(exc)
        with self.attach("traceback.txt") as f:
            traceback.print_exc(file=f)

    def _finish_done(self, result):
        self.data.state = 'done'
        self.data.result = result
        if 'error' in self.data:
            del self.data['error']

    def finish(self, result, exc=None):
        if exc:
            assert result is None
            self._finish_fail(exc)
        else:
            self._finish_done(result)
        self.data.at.finished = datetime.datetime.now()
        self.hook.on_tracker_finish(self)
        self.flush()

    def infused_tracker(self) -> 'InfusedTracker':
        return InfusedTracker(
            path=self.path,
            tid=self.tid,
            hook=self.hook,
        )

    def flush(self, metrics=True):
        logger.debug("flush tracking contxt %s", self)
        self.data.info = self.info
        self.hook.on_tracker_flush(self)
        with self.attach("votakvot.yaml", mode='wt') as f:
            votakvot.data.dump_yaml_file(f, self.data)
        if metrics:
            self.metrics.flush()


class InfusedTracker(_BaseTracker):

    def __init__(self, path, tid, hook):
        uid = uuid.uuid1().hex
        _BaseTracker.__init__(
            self,
            path=path,
            uid=uid,
            tid=tid,
            hook=hook,
            metrics=votakvot.metrics.MetricsExporter(self, add_uuid=uid),
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
        self.hook.on_tracker_infused(self)

    def flush(self):
        logger.info("flush infused tracker %s", self)
        self.hook.on_tracker_flush(self)
        self.metrics.flush()


class TrialFailedException(Exception):
    def __init__(self, error, traceback_txt):
        Exception.__init__(self, error)
        self.error = error
        self.traceback_txt = traceback_txt


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
                trackeback_txt = self.attach('traceback.txt', mode='tr').read()
            except Exception:
                trackeback_txt = None
            else:
                logger.debug("%s", trackeback_txt)
            raise TrialFailedException(self.data.error, trackeback_txt)
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
