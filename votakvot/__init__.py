"""Votakvot -- simple tool for tracking information during code testing and researching."""

import atexit
import datetime
import functools
import inspect
import typing
import uuid
import io

from typing import Callable, Dict, Optional, Union

try:
    import dill
except ImportError:
    dill = None

from . import core, meta
from . import runner as rr
from .report import load_trials, load_report
from .resumable import resumable_fn


__version__ = "0.1.dev0"
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


_T = typing.TypeVar('_T')


def init(
    path=".",
    runner='inplace',
    hooks=None,
    meta_providers=None,
    **kwargs,
):
    runner_cls = rr.runners[runner]
    metap = meta_providers or meta.providers
    r: rr.Runner = runner_cls(metap=metap, path=path, hook=hooks, **kwargs)
    atexit.register(r.close)
    core.set_global_context(rr.RunnerContext(r))


def meter(series="", value=None, *, format='csv', **kwargs):
    assert 'tid' not in kwargs and 'at' not in kwargs
    if value is not None:
        kwargs['value'] = value
    core.current_context().meter(series, kwargs, format=format)


def inform(**kwargs):
    assert 'tid' not in kwargs
    core.current_context().inform(**kwargs)


def call(tid: str, func: Callable[..., _T], params: Dict) -> _T:
    return core.current_context().call(tid, func, params)


def call_multi(func: Callable[..., _T], tid_to_params: Dict[str, Dict]) -> Dict[str, _T]:
    return core.current_context().call_multi(func, tid_to_params)


def attach(name, mode='w', **kwargs) -> io.FileIO:
    return core.current_context().attach(name, mode=mode, **kwargs)


def tid() -> Optional[str]:
    return core.current_context().tid


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

        def multi(params_list):
            tid_to_params = {name_prefix + tidp(**p) + suffixc(): p for p in params_list}
            return core.current_context().call_multi(captured_f, tid_to_params)

        @functools.wraps(f)
        def g(*args, **kwargs):
            params = dict(sig.bind(*args, **kwargs).arguments)
            tid = name_prefix + tidp(**params) + suffixc()
            return core.current_context().call(tid, captured_f, params).result

        if dill:
            # `dill` is able to serialize mutated global function,
            # but fails to serialize recursive closure (so `g` captures itself)
            captured_f = f
        else:
            # `pickle` verify that global function remains the same
            # so `g` needs to capture link to itself
            captured_f = g

        g.multi = multi
        g._votakvot_origin = f

        return g

    return wrapper
