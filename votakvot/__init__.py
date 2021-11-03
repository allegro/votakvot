"""Votakvot -- simple tool for tracking information during code testing and researching."""

from __future__ import annotations

import atexit
import datetime
import functools
import inspect
import typing
import uuid
import io
import contextvars
import contextlib
import logging

from os import PathLike
from typing import Any, Callable, Dict, Literal, Optional, Type, Union

try:
    import dill
except ImportError:
    dill = None

from . import core, meta, hook
from . import runner as _vtvt_runner
from .report import load_trials, load_report
from .resumable import resumable_fn


__version__ = "0.1.dev1"
__author__ = "anjensan"

__all__ = [
    'init',
    'meter',
    'inform',
    'track',
    'attach',
    'load_trials',
    'load_report',
    'resumable_fn',
]


logger = logging.getLogger(__name__)
_T = typing.TypeVar('_T')

_var_tracker = contextvars.ContextVar("votakvot._var_tracker")
_global_tracker = None
_global_runner = None


def current_tracker() -> core.ATracker:
    gt = _global_tracker
    ct = _var_tracker.get(None)
    if ct:
        if gt:
            logging.warning("Both global and context trackers are set, use context")
        return ct
    elif gt:
        return gt
    else:
        return core._nope_tracker


@contextlib.contextmanager
def using_tracker(tracker: core.ATracker, globally: bool = False):
    global _global_tracker

    if globally and _global_tracker is not None or _var_tracker.get(None) is not None:
        raise RuntimeError("A tracker is already configured")

    try:
        logger.debug("enter tracker %s", tracker)
        if globally:
            _global_tracker = tracker
        else:
            t = _var_tracker.set(tracker)
        tracker.activate()
        yield
    finally:
        logger.debug("exit tracker %s", tracker)
        try:
            tracker.flush()
        except Exception:
            logging.exception("Unable to flush tracker")
        if globally:
            _global_tracker = None
        else:
            _var_tracker.reset(t)


def init(
    path: str | PathLike = ".",
    hooks: hook.Hook | list[hook.Hook] | None = None,
    runner: str | type[_vtvt_runner.ARunner] ='inplace',
    meta_providers: dict[str, Callable[[], str]] | None = None,
    **kwargs,
) -> None:

    global _global_runner
    runner_cls = _vtvt_runner.runner_classes.get(runner, runner)
    metap = meta_providers or meta.providers

    logger.debug("Create global runner of type %s", runner_cls)
    _global_runner = runner_cls(
        metap=metap,
        path=path,
        hook=hooks,
        **kwargs,
    )

    atexit.register(lambda: _global_runner.close())


def meter(
    series: str = None,
    value: Any | None = None,
    *,
    format: Literal['csv', 'jsonl'] = 'csv',
    **kwargs,
) -> None:
    assert 'tid' not in kwargs and 'at' not in kwargs
    if value is not None:
        kwargs['value'] = value
    current_tracker().meter(kwargs, series or "", format)


def inform(**kwargs) -> None:
    assert 'tid' not in kwargs
    current_tracker().inform(**kwargs)


def run(tid: str, fn: Callable[..., _T], /, **params: Dict) -> core.Trial:
    if _global_runner is None:
        raise RuntimeError("Runner is not initialized, call `votakvot.init(...) first`")
    return _global_runner.run(tid, fn, **params)


def attach(name: str, mode: str = 'w', **kwargs) -> io.FileIO:
    return current_tracker().attach(name, mode=mode, **kwargs)


def tid() -> Optional[str]:
    return current_tracker().tid


def _default_tid(**kwargs):
    return datetime.datetime.now().strftime("%y-%m-%d/%H:%M:%S")


def track(
    name: Optional[str] = None,
    tid_pattern: Union[str, Callable, None] = None,
    rand_slug: bool = True,
):

    def wrapper(f: _T) -> _T:

        if name is None:
            name_prefix = f"{f.__module__}.{f.__qualname__}/"
        elif name:
            name_prefix = name + "/"
        else:
            name_prefix = ""

        if tid_pattern is None:
            tidp = _default_tid
        elif isinstance(tid_pattern, str):
            tidp = tid_pattern.format
        elif isinstance(tid_pattern, Callable):
            tidp = tid_pattern
        else:
            raise ValueError(f"invalid tid pattern {tid_pattern}, expected string or callable")

        if rand_slug:
            suffixc = lambda: "/" + uuid.uuid1().hex
        else:
            suffixc = lambda: ""

        sig = inspect.signature(f)

        @functools.wraps(f)
        def g(*args, **kwargs):
            params = dict(sig.bind(*args, **kwargs).arguments)
            tid = name_prefix + tidp(**params) + suffixc()
            return run(tid, captured_f, **params).result

        if dill:
            # `dill` is able to serialize mutated global function,
            # but fails to serialize recursive closure (so `g` captures itself)
            captured_f = f
        else:
            # `pickle` verify that global function remains the same
            # so `g` needs to capture link to itself
            captured_f = g

        g._votakvot_origin = f

        return g

    return wrapper
