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
import threading
import time
import math
import collections
import functools
import logging
import sys
import os
import queue
import traceback

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
        lock = None,
    ):
        self._lock = lock or contextlib.nullcontext()
        self._warmup = warmup
        self._started = time.time()
        self._finished = None
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


class ConcurrencyEnv:

    def __init__(self, concurrency):
        self.global_lock = threading.RLock()
        self.concurrency = concurrency
        self.queue = queue.Queue(maxsize=concurrency * 4)
        self.done = False

    def start(self):
        for i in range(self.concurrency):
            self.start_worker()

    def worker_run(self):
        while True:
            f = self.queue.get()
            try:
                f()
            except Exception:
                traceback.print_exc()
            finally:
                self.queue.task_done()

    def shutdown(self, wait):
        self.done = True
        if wait:
            self.queue.join()

    def spawn(self, function):
        if not self.done:
            self.queue.put(function)


class GeventConcurrencyEnv(ConcurrencyEnv):

    @staticmethod
    def gevent_install():
        import gevent.monkey
        gevent.monkey.patch_all()

    def start_worker(self):
        import gevent
        g = gevent.Greenlet(run=self.worker_run)
        g.start()


class ThreadConcurrencyEnv(ConcurrencyEnv):

    def start_worker(self):
        t = threading.Thread(target=self.worker_run, daemon=True)
        t.start()


def run(
    path,
    callback,
    params=None,
    tid=None,
    number=1,
    warmup=0,
    duration=None,
    meta_providers=None,
    show_progress=False,
    strict=False,
    max_errors=None,
    concurrency_env=None,
):

    assert number is None or duration is None
    concurrency_env = concurrency_env or ThreadConcurrencyEnv(1)
    concurrency_env.start()

    meta = votakvot.meta.capture_meta(meta_providers)
    tracker = votakvot.core.Tracker(path=f"{path}/{tid}", meta=meta, tid=tid)

    if show_progress:
        import tqdm
        if duration is None:
            progressbar = tqdm.tqdm(total=number, leave=False)
        else:
            progressbar = tqdm.tqdm(total=None)
    else:
        progressbar = None

    def dorun(**params):

        if isinstance(callback, type):
            real_callback = callback(**params)
        else:
            real_callback = functools.partial(callback, **params)

        collector = StatsCollector(
            tracker,
            warmup=warmup,
            max_errors=max_errors,
            lock=concurrency_env.global_lock,
        )

        def call():
            _do_onecall(collector, real_callback)
            if show_progress:
                progressbar.update()

        def spawn():
            concurrency_env.spawn(call)

        def checkerr():
            if strict and collector.errors_all:
                raise collector.errors_all[-1]

        with progressbar if progressbar is not None else contextlib.nullcontext():
            if number is None:
                until = time.time() + duration
                while time.time() < until:
                    spawn()
                    checkerr()
                concurrency_env.shutdown(False)
            else:
                for _ in range(number + warmup):
                    spawn()
                    checkerr()
                concurrency_env.shutdown(True)

        checkerr()
        print("calc stats...")
        return collector.calculate_statistics()

    with votakvot.using_tracker(tracker, globally=True):
        tracker.run(dorun, **(params or {}))
        return votakvot.core.Trial(tracker.path)


def main(args=None):


    parser = argparse.ArgumentParser(description="votakvot cli runner")
    parser.add_argument("-c", "--concurrency", help="Concurrency", type=int, default=1)
    parser.add_argument("-q", "--quiet", help="Don't display progress bar", action="store_true")
    parser.add_argument("-w", "--warmup", help="Number of skipped requests", default=0, type=int)
    parser.add_argument("-p", "--path", help="Path to results storage", type=str, default=".")
    parser.add_argument("-t", "--tid", help="Tid identifier", default=None)
    parser.add_argument("-g", "--gevent", help="Patch sockets with Gevent", action='store_true', default=False)

    parser.add_argument("-s", "--strict", help="Fail on a first error", action='store_true')
    parser.add_argument("--max-errors", help="Max number of captured errors", type=int, default=100)

    group = parser.add_mutually_exclusive_group()
    group.add_argument("-n", "--number", help="Number of requests", type=int)
    group.add_argument("-d", "--duration", help="Duration in seconds", type=int)

    parser.add_argument("callback", type=str, help="Python callable name")
    parser.add_argument("param", metavar="KEY=VALUE", nargs="*", help="Function named argument")

    opts = parser.parse_args(args)

    if opts.gevent:
        GeventConcurrencyEnv.gevent_install()
        concurrency_env = GeventConcurrencyEnv(opts.concurrency)
    else:
        concurrency_env = ThreadConcurrencyEnv(opts.concurrency)

    if opts.number is None and opts.duration is None:
        opts.number = 1

    if opts.concurrency > 100:
        print("warning: too big `concurrency`, consider enabling Gevent with `--gevent`")

    callback_name = opts.callback
    callback = resolve_obj(opts.callback)
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
        callback=callback,
        params=params,
        path=opts.path,
        tid=opts.tid,
        number=opts.number,
        duration=opts.duration,
        warmup=opts.warmup,
        show_progress=not opts.quiet,
        strict=opts.strict,
        max_errors=opts.max_errors,
        concurrency_env=concurrency_env,
    )

    try:
        collector = trial.result
    except votakvot.core.TrialFailedException as e:
        print("absolutely fail!")
        print(e.traceback_txt.strip())
        exit(2)

    print("done")
    print("")

    if collector.real_rps > 1000:
        print(f"warning: too high rps\nresults might be very unaccurate")
        print()

    def ms(t):
        return "{:.2f} ms".format(1000 * t)

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
