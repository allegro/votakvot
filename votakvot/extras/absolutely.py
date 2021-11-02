#!/usr/bin/env python

"""
`votakvot-ABsolutely` is a script you can use to quickly smoke-test your application.
It run user-provided python function from many greenlets and collect time statistics.

ABsolutely was inspired by https://github.com/tarekziade/boom
It behaves similar to Apache Bench, but call python callback instead of making HTTP calls.
"""

from __future__ import annotations

import contextlib
import collections
import datetime
import argparse
import importlib
import time
import math
import collections
import functools
import logging
import sys
import os

import votakvot
import votakvot.core
import votakvot.meta

from votakvot.data import FancyDict


logger = logging.getLogger(__file__)


def _resolve_obj_rec(name: str):
    try:
        return importlib.import_module(name)
    except ImportError:
        if "." not in name:  # no chance
            raise
    ns_name, obj_name = name.rsplit(".", 1)
    mod = _resolve_obj_rec(ns_name)
    return getattr(mod, obj_name)


def resolve_obj(name: str):
    orig_sys_path = list(sys.path)
    try:
        sys.path.append(os.getcwd())
        return _resolve_obj_rec(name)
    finally:
        sys.path.clear()
        sys.path.extend(orig_sys_path)


def _calc_percentiles(data, pcts):
    data = sorted(data)
    size = len(data)
    return {
        pct: data[int(math.ceil((size * pct) / 100)) - 1]
        for pct in pcts
        if len(data) > 500 / min(pct, 100 - pct)
    }


class StatsCollector:

    _percentiles =  [5, 10, 25, 50, 75, 90, 95, 97, 98, 99, 99.5, 99.9]

    def __init__(
        self,
        tracker: votakvot.core.Tracker,
        warmup: int = 0,
        max_errors: int = 0,
        progressbar: 'tqdm.tqdm' | None = None,
        lock = None,
    ):
        self._lock = lock or contextlib.nullcontext()
        self._warmup = warmup
        self._started = time.time()
        self._finished = None
        self._progressbar = progressbar
        self.tracker = tracker
        self.results = collections.Counter()
        self.errors = collections.Counter()
        self.errors_all = collections.deque(maxlen=max_errors)
        self.total_count = 0
        self.total_time = 0
        self.errors_count = 0
        self.times_all = []

    def add_result(self, result, duration, error=None):
        with self._lock:
            self._add_result0(result, duration, error)

    def _add_result0(self, result, duration, error):

        if self._warmup > 0:
            self._warmup -= 1
            return
        elif self._warmup == 0:
            self._started = time.time()
            self._warmup = -1

        error_repr = repr(error) if error else None
        self.total_count += 1
        self.results[result] += 1

        self.tracker.meter({
            'duration': duration,
            'result': result,
            'error': repr(error) if error else None,
        })
        if duration is not None:
            self.times_all.append(duration)
            self.total_time += duration

        if error is not None:
            self.errors[error_repr] += 1
            self.errors_count += 1
            self.errors_all.append(error)

        if self._progressbar is not None:
            self._progressbar.update()

    def calculate_statistics(self):
        self._finished = self._finished or time.time()
        average = sum(self.times_all) / len(self.times_all) if self.times_all else None
        return FancyDict(
            total_count=self.total_count,
            total_time=self.total_time,
            real_rps=self.total_count / (self._finished - self._started),
            duration=FancyDict(
                average=average,
                maximum=max(self.times_all),
                minimum=min(self.times_all),
                std_dev=math.sqrt(sum((x - average) ** 2 for x in self.times_all) / len(self.times_all)),
                percentiles=_calc_percentiles(self.times_all, self._percentiles),
            ) if self.times_all else None,
            results=[
                {"result": k, "count": v}
                for k, v in self.results.most_common()
            ],
            errors_count=self.errors_count,
            errors=[
                {"error": k, "count": v}
                for k, v in self.errors.most_common()
            ],
        )


def _do_onecall(collector: StatsCollector, callback):

    duration = None
    error = None
    result = None
    start = time.time()

    try:
        result = callback()
    except Exception as e:
        error = e
    else:
        duration = time.time() - start
    finally:
        collector.add_result(result, duration, error)


class _GeventEnv:
    """incapsulates all interaction with 'gevent'"""

    @staticmethod
    def _gevent_monkey_patch():
        import gevent.monkey
        gevent.monkey.patch_all()

    def __init__(self, concurrency):
        import gevent
        import gevent.pool
        self.semaphore = gevent.lock.Semaphore
        self.pool = gevent.pool.Pool(concurrency)
        self.timeout = gevent.Timeout

    def join(self):
        self.pool.join()

    def spawn(self, function):
        self.pool.spawn(function)

    def abort_after(self, duration):
        return self.timeout(duration, False)


def run(
    path,
    callback,
    params=None,
    tid=None,
    number=1,
    warmup=0,
    duration=None,
    concurrency=1,
    meta_providers=None,
    show_progress=False,
    concurrency_env=None,
    strict=False,
    max_errors=None,
):

    if show_progress:
        import tqdm
        progressbar = tqdm.tqdm(total=number, leave=False)
    else:
        progressbar = None

    concurrency_env = concurrency_env or _GeventEnv(concurrency)
    meta = votakvot.meta.capture_meta(meta_providers)
    tracker = votakvot.core.Tracker(path=f"{path}/{tid}", meta=meta, tid=tid)

    def dorun(**params):

        if isinstance(callback, type):
            real_callback = callback(**params)
        else:
            real_callback = functools.partial(callback, **params)

        collector = StatsCollector(
            tracker,
            warmup=warmup,
            progressbar=progressbar,
            lock=concurrency_env.semaphore(),
            max_errors=max_errors,
        )
        call = functools.partial(_do_onecall, collector, real_callback)

        def spawn():
            concurrency_env.spawn(call)

        def checkerr():
            if strict and collector.errors_all:
                raise collector.errors_all[-1]

        with progressbar if progressbar is not None else contextlib.nullcontext():
            if number is None:
                with concurrency_env.abort_after(duration):
                    while True:
                        spawn()
                        checkerr()
            else:
                for _ in range(number + warmup):
                    spawn()
                    checkerr()
            concurrency_env.join()
            checkerr()

        print("calc stats...")
        return collector.calculate_statistics()

    with votakvot.using_tracker(tracker, globally=True):
        tracker.run(dorun, **(params or {}))
        return votakvot.core.Trial(tracker.path)


def main(args=None):

    _GeventEnv._gevent_monkey_patch()

    parser = argparse.ArgumentParser(description="votakvot cli runner")
    parser.add_argument("-c", "--concurrency", help="Concurrency", type=int, default=1)
    parser.add_argument("-q", "--quiet", help="Don't display progress bar", action="store_true")
    parser.add_argument("-w", "--warmup", help="Number of skipped requests", default=0, type=int)
    parser.add_argument("-p", "--path", help="Path to results storage", type=str, default=".")
    parser.add_argument("-t", "--tid", help="Tid identifier", default=None)

    parser.add_argument("--strict", help="Fail on a first error", action='store_true')
    parser.add_argument("--max-errors", help="Max number of captured errors", type=int, default=100)

    group = parser.add_mutually_exclusive_group()
    group.add_argument("-n", "--number", help="Number of requests", type=int)
    group.add_argument("-d", "--duration", help="Duration in seconds", type=int)

    parser.add_argument("callback", type=resolve_obj, help="Python callable name")
    parser.add_argument("param", metavar="KEY=VALUE", nargs="*", help="Function named argument")

    opts = parser.parse_args(args)

    if opts.number is None and opts.duration is None:
        opts.number = 1

    callback_name = f"{opts.callback.__module__}.{opts.callback.__qualname__}"
    if opts.tid is None:
        dt_suffix = datetime.datetime.now().strftime("%y-%m-%d/%H:%M:%S")
        opts.tid = f"{callback_name}/{dt_suffix}"

    params = {}
    for p in opts.param:
        k, v = p.split("=", 1)
        if k in params:
            raise ValueError("Duplicated parameter", k)
        try:
            v = eval(v, {}, {})
        except Exception:
            pass
        params[k] = v

    print("votakvot")
    print(f"run '{callback_name}(" + ", ".join(f"{k}={v!r}" for k, v in params.items()) + ")'")
    print(f"use {opts.concurrency} parallel workers")

    if opts.number:
        print(f"make {opts.number} runs")
    else:
        print(f"keep running for {round(opts.duration)} seconds")

    if opts.warmup:
        print(f"skip {opts.warmup} first runs")

    print("running...")
    trial = run(
        callback=opts.callback,
        params=params,
        path=opts.path,
        tid=opts.tid,
        number=opts.number,
        concurrency=opts.concurrency,
        duration=opts.duration,
        warmup=opts.warmup,
        show_progress=not opts.quiet,
        strict=opts.strict,
        max_errors=opts.max_errors,
        concurrency_env=_GeventEnv(opts.concurrency),
    )

    try:
        collector = trial.result
    except votakvot.core.TrialFailedException as e:
        print("absolutely fail!")
        print(e.traceback_txt.strip())
        exit(2)

    print("done")
    print("")

    def ms(t):
        return "{:.2f} ms".format(1000 * t)

    if collector.real_rps > 1000:
        print(f"warning: too high rps\nresults might be very unaccurate")
        print()

    print(f"was made  \t {collector.total_count} runs")
    if collector.duration:
        print(f"average \t {ms(collector.duration.average)}")
        print(f"std_dev \t {ms(collector.duration.std_dev)}")
        print(f"minimum \t {ms(collector.duration.minimum)}")
        print(f"maximum \t {ms(collector.duration.maximum)}")
        print(f"percentiles:")
        for pn, pv in collector.duration.percentiles.items():
            print(f"  pct {pn:02}   \t {ms(pv)}")

    if collector.results:
        print(f"results:")
        for d in collector.results:
            print(f"  {d.count} times \t {d.result!r}")
    else:
        print(f"no results")

    if collector.errors:
        print(f"errors:")
        for e in collector.errors:
            print(f"  {e.count} times \t {e.error}")
    else:
        print(f"no errors")

    print(f"more info at\n  {trial.path}")
    print("absolutely!")


if __name__ == "__main__":
    main()
