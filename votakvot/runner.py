from __future__ import annotations

import multiprocess
import logging
import typing

import votakvot
from . import meta, core


logger = logging.getLogger(__name__)


class ARunner(typing.Protocol):

    path: str | None

    def run(self, tid, fn, /, **kwargs) -> core.Trial:
        ...

    def close(self) -> None:
        ...


class BaseRunner(ARunner):

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

    def run_with_tracker(self, tracker: core.Tracker, fn, params):
        with votakvot.using_tracker(tracker):
            tracker.run(fn, **params)


class ProcessRunner(BaseRunner):

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


# maps runner name to runner constructor/class
runner_classes = {
    'inplace': InplaceRunner,
    'process': ProcessRunner,
}
