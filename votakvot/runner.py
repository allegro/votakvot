import multiprocess
import logging

import votakvot
from . import meta, core


logger = logging.getLogger(__name__)


class RunnerContext(core.ATracker):

    tid = None
    uid = None

    def __init__(self, runner: 'Runner'):
        self.runner = runner
        self.path = runner.path

    def meter(self, **kwargs):
        raise RuntimeError("function `meter` can't be used from driver")

    def inform(self, **kwargs):
        raise RuntimeError("function `inform` can't be used from driver")

    def attach(self, name, **kwargs):
        raise RuntimeError("function `attach` can't be used from driver")

    def call(self, tid, func, params):
        return self.runner.call(tid, func, params)

    def snapshot(self):
        raise RuntimeError("function `snapshot` can't be used from driver")

    def close(self):
        logger.debug("Close runner context %s", self)
        self.runner.close()


class Runner:

    def __init__(self, path, metap=None, hook=None) -> None:
        self.path = path
        self.metap = metap
        self.hook = hook

    def call(self, tid, func, params):
        context = self._build_context(tid, func, params)
        context.prerun()
        self._make_call(context)
        return core.Trial(context.path).result

    def _capture_info(self, tid):
        return {
            'votakvot': votakvot.__version__,
            'meta': meta.capture_meta(self.metap) if self.metap else {},
        }

    def _build_context(self, tid, func, params):
        m = meta.capture_meta(self.metap) if self.metap else {}
        return core.Tracker(
            path=f"{self.path}/{tid}",
            func=func,
            params=params,
            meta=m,
            tid=tid,
            hook=self.hook,
        )

    def _make_call(self, ac: core.Tracker):
        ac.run()

    def close(self):
        pass


class InplaceRunner(Runner):
    pass


class ProcessRunner(Runner):

    def __init__(self, processes=None, mp_method='fork', **kwargs) -> None:
        super().__init__(**kwargs)
        self.mp_context = multiprocess.get_context(mp_method)
        self.mp_pool = self.mp_context.Pool(processes=processes)

    def _make_call(self, ac: core.Tracker):
        callref = self.mp_pool.apply_async(ac.run)
        callref.wait()
        callref.get()

    def close(self):
        self.mp_pool.close()


runners = {
    'inplace': InplaceRunner,
    'process': ProcessRunner,
}
