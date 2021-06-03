"""Votakvot -- simple tool for tracking information during code testing and researching."""

import atexit
import contextlib
import datetime
import logging
import os
import pickle
import traceback
import typing
import uuid

from functools import cached_property
from typing import Callable, Dict, Iterable, List, Optional

import fsspec
import pandas as pd

from . import data, metrics
from .data import FancyDict, dump_yaml_file, path_fs
import votakvot


logger = logging.getLogger(__name__)


T = typing.TypeVar('T')


class Context(typing.Protocol):
    uid: Optional[str]
    tid: Optional[str]
    def attach(self, name: str, mode: str, **kwargs) -> fsspec.core.OpenFile: ...
    def inform(self, **kwargs) -> None: ...
    def call(self, tid: str, func: Callable[..., T], params: Dict) -> T: ...
    def call_multi(self, func: Callable[..., T], tid_to_params: Dict[str, Dict]) -> Dict[str, T]: ...
    def meter(self, series: Optional[str], metrics: Dict, format: str) -> None: ...
    def flush(self) -> None: ...
    def snapshot(self) -> None: ...


class TrackingHook(typing.Protocol):

    def trial_started(self, context: 'TrackingContext'):
        pass

    def trial_presave(self, context: 'TrackingContext'):
        pass

    def trial_finished(self, context: 'TrackingContext'):
        pass

    def context_init(self, context: 'TrackingContext'):
        pass

    def context_infused(self, context: 'InfusedTrackingContext'):
        pass


_context = None


def current_context() -> Context:
    return _context or NoneContext()


@contextlib.contextmanager
def with_context(context: Context):
    global _context
    old = _context
    try:
        logger.debug("enter context %s", context)
        _context = context
        yield
    finally:
        logger.debug("exit context %s", context)
        _context = old


@atexit.register
def _close_global_context():
    logger.info("maybe close global context")
    if _context:
        _context.close()


def set_global_context(context: Context, force=False):

    global _context
    assert force or _context is None, "Context is already configured"

    old_context = _context
    if hasattr(old_context, '_on_unset_global_context'):
        old_context._on_unset_global_context()

    logger.info("Set global context to %s", context)
    _context = context

    if hasattr(context, '_on_set_global_context'):
        context._on_set_global_context()


class NoneContext(Context):

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

    def call_multi(self, func, tid_to_params):
        logger.info("call_multi %s: %s", func, tid_to_params)
        func = _desuspect_func(func)
        return {tid: func(**p) for tid, p in tid_to_params.items()}

    def snapshot(self):
        logger.debug("snapshot - do nothing")

    def meter(self, series: Optional[str], metrics: Dict, format: str):
        for k, v in metrics.items():
            logger.debug("metric[%s] %s = %s", series, k, v)


class BaseTrackingContext(NoneContext):

    def __init__(self, path, uid, tid, metrics, hooks):
        self.path = path
        self.uid = uid
        self.tid = tid
        self.metrics = metrics
        self.hooks = hooks or []

    def attach(self, name, mode='w', autocommit='onclose', **kwargs):
        fn = f"{self.path}/{name}"
        logger.debug("open attachement %s (resolved to %s)", name, fn)
        if 'w' in mode and autocommit == 'onclose':
            # f = path_fs(fn).open(fn, mode=mode, autocommit=False, **kwargs)
            f = path_fs(fn).open(fn, mode=mode, autocommit=True, **kwargs)
            logger.debug("wrap file object for an autocommit")
            return data.AutoCommitableFileWrapper(f)
        else:
            return path_fs(fn).open(fn, mode=mode, autocommit=autocommit, **kwargs)

    def meter(self, series: Optional[str], metrics: Dict, format=None):
        self.metrics.meter(series, metrics, format)


class TrackingContext(BaseTrackingContext):

    def __init__(self, path, meta, func, params, tid, hooks):
        BaseTrackingContext.__init__(
            self,
            path=path,
            tid=tid,
            uid=uuid.uuid1().hex,
            hooks=hooks,
            metrics=metrics.MetricsExporter(),
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
        for h in self.hooks:
            h.trial_presave(self)
        with self.attach("votakvot.yaml", mode='wt') as f:
            data.dump_yaml_file(f, self.data)

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
        other.pop('hooks', None)
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

        for h in self.hooks:
            h.context_init(self)

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

        with with_context(self):

            for h in self.hooks:
                h.trial_started(self)

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

            for h in self.hooks:
                h.trial_finished(self)

            self.metrics.close()

        self.data.at.finished = datetime.datetime.now()
        self.data.at.duration = (finished - started).microseconds * 0.000001

        self.dump_trial()

    def infused_context(self) -> 'InfusedTrackingContext':
        return InfusedTrackingContext(
            path=self.path,
            uid=self.uid,
            tid=self.tid,
            hooks=self.hooks,
        )

    def flush(self):
        logger.debug("flush tracking contxt %s", self)
        self.dump_trial()
        self.metrics.flush()


class InfusedTrackingContext(BaseTrackingContext):

    def __init__(self, uid, path, tid, hooks):
        uid = uuid.uuid1().hex
        BaseTrackingContext.__init__(
            self,
            path=path,
            uid=uid,
            tid=tid,
            hooks=hooks,
            metrics=metrics.MetricsExporter(add_uuid=uid),
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

    def _on_set_global_context(self):
        logger.info("activate infused context for %s", self.path)
        for h in self.hooks:
            h.context_infused(self)

    def flush(self):
        logger.info("flush infused context %s", self)
        self.metrics.flush()

    def close(self):
        logger.info("close infused context %s", self)
        self.metrics.close()


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
            return data.load_yaml_file(f)

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
        return metrics.load_metrics(self)

    @cached_property
    def data_plain(self):
        r = data.plainify_dict(self.data)
        r.pop('votakvot', None)
        return r

    def __repr__(self):
        return f"<Trial {self.uid!r}>"

    def __eq__(self, other):
        return self.uid == other.uid

    def __hash__(self):
        return hash(self.uid)
