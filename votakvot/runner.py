from __future__ import annotations

import multiprocess
import logging
import typing

import votakvot
from . import meta, core


logger = logging.getLogger(__name__)


def find_runner(name: str) -> typing.Type[ARunner]:

    all_names = []
    candidate = []

    for c in ARunner._all_subclasses():
        aname = getattr(c, 'runner_name', None)
        if name == aname:
            candidate.append(c)
        if aname:
            all_names.append(aname)

    logger.info("Found %d runners: %s", len(all_names), ", ".join(all_names))

    if len(candidate) == 0:
        raise ValueError(f"Not found runner with name {name!r}")
    if len(candidate) == 1:
        return candidate[0]
    else:
        raise ValueError(f"Found multipler runners for name {name!r}", candidate)



class ARunner(typing.Protocol):

    name: str
    path: str | None

    def run(self, tid, fn, /, **kwargs) -> core.Trial:
        ...

    def close(self) -> None:
        ...

    @classmethod
    def _all_subclasses(cls):
        for c in cls.__subclasses__():
            yield c
            yield from c._all_subclasses()


class BaseRunner(ARunner):

    name = None

    def __init__(self, path, metap=None, hook=None) -> None:
        self.path = path
        self.metap = metap
        self.hook = hook

    def run(self, tid, fn, /, **kwargs):
        tracker = self.create_tracker(tid, fn, kwargs)
        self.run_with_tracker(tracker, fn, kwargs)
        return core.Trial(tracker.path)

    def capture_meta(self):
        return meta.capture_meta(self.metap) if self.metap else {}

    def create_tracker(self, tid, func, params):
        meta = self.capture_meta()
        return core.Tracker(
            path=f"{self.path}/{tid}",
            meta=meta,
            tid=tid,
            hook=self.hook,
        )

    def close(self):
        pass

    def run_with_tracker(self, tracker: core.Tracker, fn, params: typing.Dict):
        raise NotImplementedError


class InplaceRunner(BaseRunner):

    runner_name = 'inplace'

    def run_with_tracker(self, tracker: core.Tracker, fn, params):
        with votakvot.using_tracker(tracker):
            tracker.run(fn, **params)


class ProcessRunner(BaseRunner):

    runner_name = 'process'

    def __init__(self, processes=None, mp_method='fork', **kwargs) -> None:
        super().__init__(**kwargs)
        self.mp_context = multiprocess.get_context(mp_method)
        self.mp_pool = self.mp_context.Pool(processes=processes)

    def run_with_tracker(self, ac: core.Tracker, fn, params):
        callref = self.mp_pool.apply_async(self._run_in_processs, (ac, fn, params))
        callref.wait()
        callref.get()

    @staticmethod
    def _run_in_processs(ac: core.Tracker, fn, params):
        with votakvot.using_tracker(ac, globally=True):
            ac.run(fn, **params)

    def close(self):
        self.mp_pool.close()
